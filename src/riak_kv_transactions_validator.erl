-module(riak_kv_transactions_validator).

-behaviour(gen_server).

-export([start_link/1,
         validate/7,
         validate/8,
         update_latest_object_versions/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(LATEST_OBJECT_VERSIONS, latest_object_versions).
-define(RUNNING_TRANSACTIONS, running_transactions).

-record(state, {id, lsn, step}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    gen_server:start_link({global, {?MODULE, Id}}, ?MODULE, Id, []).

validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    gen_server:cast({global, {?MODULE, Id}}, {validate, TransactionId, Snapshot, Gets, Puts, NValidations, Client, false}).

validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts) ->
    gen_server:cast({global, {?MODULE, Id}}, {validate, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts}).

update_latest_object_versions(Id, Objects, Versions) ->
    gen_server:cast({global, {?MODULE, Id}}, {update_latest_object_versions, Objects, Versions}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Id) ->
    ets:new(?LATEST_OBJECT_VERSIONS, [private, named_table]), 
    ets:new(?RUNNING_TRANSACTIONS, [private, named_table]), 

    {ok, NTransactionsManagers} = application:get_env(riak_kv, n_transactions_managers),
    Step = case (NTransactionsManagers - 1) of 0 -> 1; S -> S end,
    State = #state{id = Id,
                   lsn = Id + 1,
                   step = Step},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({validate, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts}, State) ->
    do_validate(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, State);

handle_cast({update_latest_object_versions, Objects, Version}, State) ->
    do_update_latest_object_versions(Objects, Version, State);

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
  TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts,
  #state{id = Id} = State
) ->
    lager:info("Received transaction ~p for validation~n", [TransactionId]),

    {ok, NTransactionsManagers} = application:get_env(riak_kv, n_transactions_managers),
    Root = NTransactionsManagers - 1,
    if
        Id < Root ->
            leaf_validate(TransactionId, Snapshot, Gets, Puts, NValidations, Client, State);
        true ->
            root_validate(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, State)
    end.

do_update_latest_object_versions(Objects, Version, State) ->
    do_update_latest_object_versions2(Objects, Version),
    {noreply, State}.

% Validates transaction as soon as it arrives
leaf_validate(
  TransactionId, Snapshot, Gets, Puts, NValidations, Client,
  #state{id = Id, lsn = Lsn, step = Step} = State
 ) ->
    lager:info("Leaf validation in progress...~n", []),
    NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
    Conflicts = check_conflicts(Gets, NbkeyPuts, Snapshot),
    lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts]),

    if
        not Conflicts ->
            do_update_latest_object_versions2(NbkeyPuts, Lsn),

            {ok, NTransactionsManagers} = application:get_env(riak_kv, n_transactions_managers),
            Root = NTransactionsManagers - 1,
            riak_kv_transactions_validator:update_latest_object_versions(Root, NbkeyPuts, Lsn);
        true ->
            ok
    end,

    Record = riak_kv_transactions_log:new_log_record(Lsn, {TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts}),
    riak_kv_transactions_log:append(Id, Record),

    NewState = State#state{lsn = Lsn + Step},
    {noreply, NewState}.

% Validates the transaction once all the info has arrived from its children
root_validate(
  TransactionId, Snapshot1, Gets1, Puts1, NValidations, Client, Conflicts1,
  #state{id = Id} = State
) ->
    % Update transaction metadata
    case ets:lookup(?RUNNING_TRANSACTIONS, TransactionId) of
        [{TransactionId, Snapshot2, Gets2, Puts2, Conflicts2, ReceivedValidations1}] ->
            ets:insert(?RUNNING_TRANSACTIONS, {
                TransactionId,
                max(Snapshot1, Snapshot2),
                Gets1 ++ Gets2,
                Puts1 ++ Puts2,
                Conflicts1 or Conflicts2,
                ReceivedValidations1 + 1
            });
        [] ->
            ets:insert(?RUNNING_TRANSACTIONS, {TransactionId, Snapshot1, Gets1, Puts1, Conflicts1, 1})
    end,

    [{TransactionId, Snapshot, Gets, Puts, Conflicts, ReceivedValidations}] = ets:lookup(?RUNNING_TRANSACTIONS, TransactionId),
    case ReceivedValidations of
        NValidations ->
            do_root_commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts);
        _ ->
            ok
    end,

    {noreply, State}.

do_root_commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, true = Conflicts) ->
    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, false),
    ets:delete(?RUNNING_TRANSACTIONS, TransactionId);
do_root_commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, false = _Conflicts) ->
    lager:info("Root validation in progress...~n", []),
    NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
    Conflicts = check_conflicts(Gets, NbkeyPuts, Snapshot),
    lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts]),

    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, true),

    if
        not Conflicts -> do_update_latest_object_versions2(NbkeyPuts, Snapshot);
        true -> ok
    end,

    ets:delete(?RUNNING_TRANSACTIONS, TransactionId).

do_update_latest_object_versions2(Nbkeys, Version) ->
    lists:foreach(
        fun(Nbkey) ->
            ets:insert(?LATEST_OBJECT_VERSIONS, {Nbkey, Version})
        end,
        Nbkeys
    ).

check_conflicts(Gets, Puts, Snapshot) ->
    ConflictsGets = do_check_conflicts(Gets, Snapshot),
    ConflictsPuts = do_check_conflicts(Puts, Snapshot),
    ConflictsGets or ConflictsPuts.

do_check_conflicts(Objects, Snapshot) ->
    do_check_conflicts(Objects, Snapshot, false).

do_check_conflicts(_Objects, _Snapshot, true) -> true;
do_check_conflicts([], _Snapshot, Conflict) -> Conflict;
do_check_conflicts([Nbkey | Rest], Snapshot, Conflict) ->
    LatestObjectVersion = case ets:lookup(?LATEST_OBJECT_VERSIONS, Nbkey) of
                              [{Nbkey, Version}] -> Version;
                              [] -> -1
                          end,

    NewConflict = Conflict or (LatestObjectVersion > Snapshot),

    do_check_conflicts(Rest, Snapshot, NewConflict).
