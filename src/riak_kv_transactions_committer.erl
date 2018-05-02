-module(riak_kv_transactions_committer).

-behaviour(gen_server).

-export([start_link/0,
         process_record/2,
         print_state/0]).

-export([code_change/3,
         handle_call/3,
         handle_cast/2,
	     handle_info/2,
	     init/1,
	     terminate/2]).

-include("riak_kv_log.hrl").

-define(MESSAGE_QUEUE_LENGTH_THRESHOLD, 100000).

-record(state, {next_lsn,
                running_transactions,
                latest_object_versions}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

process_record(Lsn, Record) ->
    gen_server:cast({global, ?MODULE}, {process_record, Lsn, Record}).

print_state() ->
    gen_server:cast({global, ?MODULE}, print_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    State = #state{next_lsn = 1,
                   running_transactions = dict:new(),
                   latest_object_versions = dict:new()},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({process_record, Lsn, Record}, #state{next_lsn = Lsn} = State) ->
    verify_message_queue_length(),
    NewState = do_process_record(Record, State),
    {noreply, NewState};

handle_cast(print_state, State) ->
    io:format("~p~n", [State]),
    {noreply, State};

handle_cast(Request, State) ->
    lager:error("Unexpected request received at hanlde_cast: ~p~n", [Request]),
    {noreply, State}.

handle_info(Info, State) ->
    lager:error("Unexpected info received at handle_info: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

verify_message_queue_length() ->
    {message_queue_len, MessageQueueLength} = process_info(self(), message_queue_len),
    if
        MessageQueueLength > ?MESSAGE_QUEUE_LENGTH_THRESHOLD ->
            lager:critical("Transactions committer message queue length is past the threshold, currently at ~p~n", [MessageQueueLength]);
        true ->
            ok
    end.

do_process_record(
  #log_record{type = transaction_commit,
              payload = Payload} = _Record,
  #state{next_lsn = Lsn,
         running_transactions = RunningTransactions,
         latest_object_versions = LatestObjectVersions} = State
) ->
    %lager:info("Processing log record #~p ~p~n", [Lsn, Record]),

    {Id, Snapshot, Gets, [FirstPut | _] = PayloadPuts, NVnodes, Client} = Payload,

    Vnode = get_vnode(FirstPut),
    NewRunningTransactions1 = case dict:find(Id, RunningTransactions) of
                                 {ok, {Count1, Puts1}} ->
                                     Puts2 = dict:store(Vnode, PayloadPuts, Puts1),
                                     dict:store(Id, {Count1 + 1, Puts2}, RunningTransactions);
                                 error ->
                                     Puts2 = dict:store(Vnode, PayloadPuts, dict:new()),
                                     dict:store(Id, {1, Puts2}, RunningTransactions)
                             end,

    {Count, Puts} = dict:fetch(Id, NewRunningTransactions1),
    if
        Count == NVnodes ->
            NewRunningTransactions = dict:erase(Id, NewRunningTransactions1),
            NewLatestObjectVersions = commit_transaction(Id, Snapshot, Gets, Puts, Client, Lsn, LatestObjectVersions);
        true ->
            NewRunningTransactions = NewRunningTransactions1,
            NewLatestObjectVersions = LatestObjectVersions
    end,

    State#state{next_lsn = Lsn + 1,
                running_transactions = NewRunningTransactions,
                latest_object_versions = NewLatestObjectVersions};

do_process_record(_Record, #state{next_lsn = Lsn} = State) ->
    State#state{next_lsn = Lsn + 1}.

commit_transaction(Id, Snapshot, Gets, PutsDict, Client, Lsn, LatestObjectVersions) ->
    {ConflictGets, _} = check_conflicts(Gets, Snapshot, Lsn, LatestObjectVersions),
    Puts = dict:fold(fun(_, Value, Acc) -> Value ++ Acc end, [], PutsDict),
    {ConflictPuts, NewLatestObjectVersions} = check_conflicts(Puts, Snapshot, Lsn, LatestObjectVersions),

    Vnodes = dict:fetch_keys(PutsDict),
    Conflict = ConflictGets or ConflictPuts,
    if
        Conflict -> 
            send_transaction_commit_status_to(Client, Id, aborted, Lsn),
            send_transaction_commit_status_to(Vnodes, Id, aborted, Lsn, PutsDict),
            LatestObjectVersions;

        true ->
            send_transaction_commit_status_to(Client, Id, committed, Lsn),
            send_transaction_commit_status_to(Vnodes, Id, committed, Lsn, PutsDict),
            NewLatestObjectVersions
    end.

check_conflicts(Objects, Snapshot, Lsn, LatestObjectVersions) ->
    check_conflicts(Objects, Snapshot, Lsn, LatestObjectVersions, false).

check_conflicts(_Objects, _Snapshot, _Lsn, LatestObjectVersions, true) ->
    {true, LatestObjectVersions};
check_conflicts([], _Snapshot, _Lsn, LatestObjectVersions, Conflict) ->
    {Conflict, LatestObjectVersions};
check_conflicts([Bkey | Rest], Snapshot, Lsn, LatestObjectVersions, Conflict) ->
    LatestObjectVersion = case dict:find(Bkey, LatestObjectVersions) of
                              {ok, V} -> V;
                              error -> -1
                          end,

    NewConflict = Conflict or (LatestObjectVersion > Snapshot),
    NewLatestObjectVersions = dict:store(Bkey, Lsn, LatestObjectVersions),

    check_conflicts(Rest, Snapshot, Lsn, NewLatestObjectVersions, NewConflict).

send_transaction_commit_status_to(Client, Id, Status, Lsn) ->
    Reply = {transaction_commit_status, Id, Status, Lsn},
    riak_core_vnode:reply(Client, Reply).

send_transaction_commit_status_to(Vnodes, Id, Status, Lsn, PutsDict) ->
    lists:foreach(fun(Vnode) ->
                          Puts = dict:fetch(Vnode, PutsDict),
                          riak_kv_vnode:transaction_commit_status([Vnode], Id, Status, Lsn, Puts)
                  end, Vnodes).

get_vnode(Bkey) ->
    DocIdx = riak_core_util:chash_key(Bkey),
    [Vnode] = riak_core_apl:get_apl(DocIdx, 1, riak_kv),
    Vnode.
