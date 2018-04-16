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

main(#state{continuation = Continuation, n_records_read = NRecordsRead} = State) ->
    NRecordsLogged = riak_kv_log:get_n_records_logged(),
    N = (NRecordsLogged - 1) - NRecordsRead,
    {NewContinuation, Records} = read_records(?LOG, Continuation, N), 
    NewState = State#state{continuation = NewContinuation},
    lists:foldl(fun process_record/2, NewState, Records).

read_records(Log, Continuation, -1) ->
    read_records(Log, Continuation, 0);
read_records(Log, Continuation, N) ->
    read_records(Log, Continuation, N, []).

read_records(_Log, Continuation, 0, Terms) ->
    {Continuation, Terms};
read_records(Log, Continuation, N, Terms) ->
    case disk_log:chunk(Log, Continuation, N) of
        {Continuation1, Terms1} ->
            NewN = N - length(Terms1),
            read_records(Log, Continuation1, NewN, [Terms1 | Terms])
    end.

process_record(
  #log_record{type = transaction_commit,
              payload = Payload} = Record,
  #state{n_records_read = NRecordsRead,
         running_transactions = RunningTransactions,
         latest_object_versions = LatestObjectVersions} = State
) ->
    lager:info("Processing log record ~p~n", [Record]),

    N = NRecordsRead + 1,
    {Id, Snapshot, Gets, PayloadPuts, NVnodes, Client} = Payload,

    NewRunningTransactions = case dict:find(Id, RunningTransactions) of
                                 {ok, {Count1, Puts1}} -> dict:store(Id, {Count1 + 1, [PayloadPuts | Puts1]}, RunningTransactions);
                                 error -> dict:store(Id, {1, PayloadPuts}, RunningTransactions)
                             end,

    {Count, Puts} = dict:fetch(Id, RunningTransactions),
    NewLatestObjectVersions = if
                                  Count == NVnodes ->
                                      commit_transaction(Id, Snapshot, Gets, Puts, Client, N, LatestObjectVersions);
                                  true ->
                                      LatestObjectVersions
                              end,

    State#state{n_records_read = N,
                running_transactions = NewRunningTransactions,
                latest_object_versions = NewLatestObjectVersions};

process_record(_Record, Acc) -> Acc.

commit_transaction(Id, Snapshot, Gets, Puts, Client, Version, LatestObjectVersions) ->
    {ConflictGets, _} = check_conflicts(Gets, Snapshot, Version, LatestObjectVersions),
    {ConflictPuts, NewLatestObjectVersions} = check_conflicts(Puts, Snapshot, Version, LatestObjectVersions),

    Conflict = ConflictGets or ConflictPuts,
    if
        Conflict -> 
            send_transaction_commit_status_to(Client, Id, aborted, Version),

            % TODO
            % Send message to vnodes to delete temporary values

            LatestObjectVersions;

        true ->
            send_transaction_commit_status_to(Client, Id, committed, Version),

            % TODO
            % Send message to vnodes to commit temporary values

            NewLatestObjectVersions
    end.

check_conflicts(Objects, Snapshot, Version, LatestObjectVersions) ->
    check_conflicts(Objects, Snapshot, Version, LatestObjectVersions, false).

check_conflicts(_Objects, _Snapshot, _Version, LatestObjectVersions, true) ->
    {true, LatestObjectVersions};
check_conflicts([], _Snapshot, _Version, LatestObjectVersions, Conflict) ->
    {Conflict, LatestObjectVersions};
check_conflicts([Bkey | Rest], Snapshot, Version, LatestObjectVersions, Conflict) ->
    LatestObjectVersion = case dict:find(Bkey, LatestObjectVersions) of
                              {ok, V} -> V;
                              error -> -1
                          end,

    NewConflict = Conflict or (LatestObjectVersion > Snapshot),
    NewLatestObjectVersions = dict:store(Bkey, Version, LatestObjectVersions),

    check_conflicts(Rest, Snapshot, Version, NewLatestObjectVersions, NewConflict).

send_transaction_commit_status_to(Client, TransactionId, Status, Version) ->
    Messsage = {transaction_commit_status, TransactionId, Status, Version},
    riak_core_vnode:reply(Client, Messsage).
