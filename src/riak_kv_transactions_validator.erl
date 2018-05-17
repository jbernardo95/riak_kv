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

-define(LATEST_OBJECT_VERSIONS, latest_object_versions).

-record(state, {n, lsn, step}).

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
    ets:new(?LATEST_OBJECT_VERSIONS, [private, named_table]), 

    {ok, Step} = application:get_env(riak_kv, n_transactions_managers),
    State = #state{n = N,
                   lsn = N + 1,
                   step = Step},
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
  #state{n = N, lsn = Lsn, step = Step} = State
) ->
    ConflictsGets = check_conflicts(Gets, Snapshot),
    BkeyPuts = lists:map(fun riak_object:bkey/1, Puts),
    ConflictsPuts = check_conflicts(BkeyPuts, Snapshot),
    Conflicts = ConflictsGets or ConflictsPuts,

    lager:info("Transaction ~p validated, conflicts: ~p~n", [Id, Conflicts]),

    Record = riak_kv_transactions_log:new_log_record(Lsn, {Id, Snapshot, Gets, Puts, NValidations, Client, Conflicts}),
    riak_kv_transactions_log:append(N, Record),
    
    if
        not Conflicts ->
            lists:foreach(fun(Bkey) -> ets:insert(?LATEST_OBJECT_VERSIONS, {Bkey, Lsn}) end, BkeyPuts);
        true ->
            ok
    end,

    NewState = State#state{lsn = Lsn + Step},
    {noreply, NewState}.

check_conflicts(Objects, Snapshot) ->
    check_conflicts(Objects, Snapshot, false).

check_conflicts(_Objects, _Snapshot, true) -> true;
check_conflicts([], _Snapshot, Conflict) -> Conflict;
check_conflicts([Bkey | Rest], Snapshot, Conflict) ->
    LatestObjectVersion = case ets:lookup(?LATEST_OBJECT_VERSIONS, Bkey) of
                              [{Bkey, Version}] -> Version;
                              [] -> -1
                          end,

    NewConflict = Conflict or (LatestObjectVersion > Snapshot),

    check_conflicts(Rest, Snapshot, NewConflict).
