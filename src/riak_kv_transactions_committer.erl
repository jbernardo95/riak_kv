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

    NewState = lists:foldl(fun process_record/2, {NRecordsRead + 1, State}, Records),

    NewState#state{continuation = NewContinuation,
                   n_records_read = (NRecordsLogged - 1)}.

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
  {N, #state{running_transactions = RunningTransactions,
             latest_object_versions = LatestObjectVersions} = State}
) ->
    lager:info("Processing log record ~p~n", [Record]),

    {Id, _Snapshot, _Gets, _Puts, NVnodes} = Payload,

    NewRunningTransactions = case dict:find(Id, RunningTransactions) of
                                 {ok, Value} ->
                                     if
                                         (Value + 1) == NVnodes -> dict:erase(Id, RunningTransactions);
                                         true -> dict:store(Id, Value + 1, RunningTransactions)
                                     end;
                                 error -> dict:store(Id, 1, RunningTransactions)
                             end,

    NewLatestObjectVersions = case dict:find(Id, RunningTransactions) of
                                  error ->
                                      commit_transaction(Payload, N, LatestObjectVersions);
                                  _ ->
                                      LatestObjectVersions
                              end,

    {N + 1, State#state{running_transactions = NewRunningTransactions,
                        latest_object_versions = NewLatestObjectVersions}};

process_record(_Record, Acc) -> Acc.

commit_transaction({_Id, Snapshot, Gets, Puts, _}, Version, LatestObjectVersions) ->
    {ConflictGets, NewLatestObjectVersions1} = check_conflicts(Gets, Snapshot, LatestObjectVersions, Version),
    {ConflictPuts, NewLatestObjectVersions2} = check_conflicts(Puts, Snapshot, NewLatestObjectVersions1, Version),

    Conflict = ConflictGets or ConflictPuts,
    if
        Conflict -> 
            % TODO
            % Send message to commit fsm telling abort
            % Send message to vnodes to delete temporary values
            LatestObjectVersions;

        true ->
            % TODO
            % Send message to commit fsm telling commit 
            % Send message to vnodes to commit temporary values
            NewLatestObjectVersions2
    end.

check_conflicts(Objects, Snapshot, LatestObjectVersions, Version) ->
    check_conflicts(Objects, Snapshot, LatestObjectVersions, Version, false).

check_conflicts(_Objects, _Snapshot, LatestObjectVersions, _Version, true) ->
    {true, LatestObjectVersions};
check_conflicts([], _Snapshot, LatestObjectVersions, _Version, Conflict) ->
    {Conflict, LatestObjectVersions};
check_conflicts([Object | Rest], Snapshot, LatestObjectVersions, Version, Conflict) ->
    LatestObjectVersion = case dict:find(Object, LatestObjectVersions) of
                              {ok, V} -> V;
                              error -> -1
                          end,

    NewConflict = Conflict or (LatestObjectVersion > Snapshot),
    NewLatestObjectVersions = dict:store(Object, Version, LatestObjectVersions),

    check_conflicts(Rest, Snapshot, NewLatestObjectVersions, Version, NewConflict).
