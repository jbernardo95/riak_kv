-module(riak_kv_transactions_committer).

-behaviour(gen_server).

-export([start_link/0, print_state/0]).

-export([code_change/3,
         handle_call/3,
         handle_cast/2,
	     handle_info/2,
	     init/1,
	     terminate/2]).

-include("riak_kv_log.hrl").

-define(LOG, riak_kv_log).

-record(state, {continuation,
                n_records_read,
                skip_n_records,
                running_transactions,
                latest_object_versions}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

print_state() ->
    gen_server:cast({global, ?MODULE}, print_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    erlang:send(self(), main),
    State = #state{continuation = start,
                   n_records_read = 0,
                   skip_n_records = 0,
                   running_transactions = dict:new(),
                   latest_object_versions = dict:new()},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast(print_state, State) ->
    io:format("~p~n", [State]),
    {noreply, State};

handle_cast(Request, State) ->
    lager:error("Unexpected request received at hanlde_cast: ~p~n", [Request]),
    {noreply, State}.

handle_info(main, State) ->
    NewState = main(State),
    erlang:send_after(1000, self(), main),
    {noreply, NewState};

handle_info(Info, State) ->
    lager:error("Unexpected info received at handle_info: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

main(#state{continuation = Continuation, n_records_read = NRecordsRead, skip_n_records = SkipNRecords} = State) ->
    % TODO this can be replced with disk_log:info() -> n_items
    Info = disk_log:info(?LOG),
    NRecordsLogged = proplists:get_value(no_items, Info, 0),
    N = NRecordsLogged - NRecordsRead,

    {NewContinuation, NewSkipNRecords, Records} = read_records(?LOG, Continuation, SkipNRecords, N), 

    NewState = State#state{continuation = NewContinuation,
                           skip_n_records = NewSkipNRecords},

    lists:foldl(fun process_record/2, NewState, Records).

read_records(_Log, Continuation, SkipNRecords, 0) ->
    {Continuation, SkipNRecords, []};
read_records(Log, Continuation, 0, N) ->
    do_read_records(Log, Continuation, N, []);
read_records(Log, Continuation, SkipNRecords, N) ->
    case disk_log:chunk(Log, Continuation, SkipNRecords) of
        {Continuation1, Terms} ->
            NewSkipNRecords = SkipNRecords - length(Terms),
            read_records(Log, Continuation1, NewSkipNRecords, N)
    end.

do_read_records(Log, Continuation, N, Terms) ->
    case disk_log:chunk(Log, Continuation, N) of
        {Continuation1, Terms1} ->
            NewN = N - length(Terms1),
            if
                NewN == 0 ->
                    {Continuation, N, Terms ++ Terms1};
                true ->
                    do_read_records(Log, Continuation1, NewN, Terms ++ Terms1)
            end
    end.

process_record(
  #log_record{type = transaction_commit,
              payload = Payload} = Record,
  #state{n_records_read = NRecordsRead,
         running_transactions = RunningTransactions,
         latest_object_versions = LatestObjectVersions} = State
) ->
    lager:info("Processing log record ~p~n", [Record]),

    Lsn = NRecordsRead + 1,
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

    State#state{n_records_read = Lsn,
                running_transactions = NewRunningTransactions,
                latest_object_versions = NewLatestObjectVersions};

process_record(_Record, #state{n_records_read = NRecordsRead} = State) ->
    State#state{n_records_read = NRecordsRead + 1}.

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
