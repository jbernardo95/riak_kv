-module(riak_kv_transactions_validator).

-behaviour(gen_server).

-export([start_link/1,
         validate/7]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(LATEST_OBJECT_VERSIONS, latest_object_versions).

-record(state, {id, lsn, step}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    gen_server:start_link({global, {?MODULE, Id}}, ?MODULE, Id, []).

validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    gen_server:cast({global, {?MODULE, Id}}, {validate, TransactionId, Snapshot, Gets, Puts, NValidations, Client}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Id) ->
    ets:new(?LATEST_OBJECT_VERSIONS, [private, named_table]), 

    {ok, NTransactionsManagers} = application:get_env(riak_kv, n_transactions_managers),
    Step = case (NTransactionsManagers - 1) of 0 -> 1; S -> S end,
    State = #state{id = Id,
                   lsn = Id + 1,
                   step = Step},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({validate, TransactionId, Snapshot, Gets, Puts, NValidations, Client}, State) ->
    do_validate(TransactionId, Snapshot, Gets, Puts, NValidations, Client, State);

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
  TransactionId, Snapshot, Gets, Puts, NValidations, Client,
  #state{id = Id, lsn = Lsn, step = Step} = State
) ->
    ConflictsGets = check_conflicts(Gets, Snapshot),
    NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
    ConflictsPuts = check_conflicts(NbkeyPuts, Snapshot),
    Conflicts = ConflictsGets or ConflictsPuts,

    %lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts]),

    Record = riak_kv_transactions_log:new_log_record(Lsn, {TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts}),
    riak_kv_transactions_log:append(Id, Record),
    
    if
        not Conflicts ->
            lists:foreach(fun(Nbkey) -> ets:insert(?LATEST_OBJECT_VERSIONS, {Nbkey, Lsn}) end, NbkeyPuts);
        true ->
            ok
    end,

    NewState = State#state{lsn = Lsn + Step},
    {noreply, NewState}.

check_conflicts(Objects, Snapshot) ->
    check_conflicts(Objects, Snapshot, false).

check_conflicts(_Objects, _Snapshot, true) -> true;
check_conflicts([], _Snapshot, Conflict) -> Conflict;
check_conflicts([Nbkey | Rest], Snapshot, Conflict) ->
    LatestObjectVersion = case ets:lookup(?LATEST_OBJECT_VERSIONS, Nbkey) of
                              [{Nbkey, Version}] -> Version;
                              [] -> -1
                          end,

    NewConflict = Conflict or (LatestObjectVersion > Snapshot),

    check_conflicts(Rest, Snapshot, NewConflict).
