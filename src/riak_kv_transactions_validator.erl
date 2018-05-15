-module(riak_kv_transactions_validator).

-behaviour(gen_server).

-export([start_link/1,
         validate/7,
         print_state/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {n, lsn, step, latest_object_versions}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(N) ->
    gen_server:start_link({global, {?MODULE, N}}, ?MODULE, N, []).

validate(N, Id, Snapshot, Gets, Puts, NValidations, Client) ->
    gen_server:cast({global, {?MODULE, N}}, {validate, Id, Snapshot, Gets, Puts, NValidations, Client}).

print_state(N) ->
    gen_server:cast({global, {?MODULE, N}}, print_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(N) ->
    {ok, Step} = application:get_env(riak_kv, n_transactions_managers),
    State = #state{n = N,
                   lsn = N + 1,
                   step = Step,
                   latest_object_versions = dict:new()},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({validate, Id, Snapshot, Gets, Puts, NValidations, Client}, State) ->
    do_validate(Id, Snapshot, Gets, Puts, NValidations, Client, State);

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

do_validate(
  Id, Snapshot, Gets, Puts, NValidations, Client,
  #state{n = N, lsn = Lsn, step = Step,
         latest_object_versions = LatestObjectVersions} = State
) ->
    {ConflictsGets, _} = check_conflicts(Gets, Snapshot, Lsn, LatestObjectVersions),
    BkeyPuts = lists:map(fun riak_object:bkey/1, Puts),
    {ConflictsPuts, NewLatestObjectVersions} = check_conflicts(BkeyPuts, Snapshot, Lsn, LatestObjectVersions),
    Conflicts = ConflictsGets or ConflictsPuts,

    %lager:info("Transaction ~p validated, conflicts: ~p~n", [Id, Conflicts]),

    Record = riak_kv_transactions_log:new_log_record(Lsn, {Id, Snapshot, Gets, Puts, NValidations, Client, Conflicts}),
    riak_kv_transactions_log:append(N, Record),
    
    case Conflicts of
        true ->
            NewState = State#state{lsn = Lsn + Step};
        false ->
            NewState = State#state{lsn = Lsn + Step,
                                   latest_object_versions = NewLatestObjectVersions}
    end,
    {noreply, NewState}.

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
