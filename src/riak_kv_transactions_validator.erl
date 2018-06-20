-module(riak_kv_transactions_validator).

-behaviour(gen_server).

-export([start_link/1,
         validate/7,
         validate/9,
         update_latest_object_versions/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(LATEST_OBJECT_VERSIONS, latest_object_versions).
-define(RUNNING_TRANSACTIONS, running_transactions).

-record(state, {id, next_lsn, step}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    gen_server:start_link({global, {?MODULE, Id}}, ?MODULE, Id, []).

validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    gen_server:cast({global, {?MODULE, Id}}, {validate, TransactionId, Snapshot, Gets, Puts, NValidations, Client, false, -1}).

validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn) ->
    gen_server:cast({global, {?MODULE, Id}}, {validate, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn}).

update_latest_object_versions(Id, Objects, Versions) ->
    gen_server:cast({global, {?MODULE, Id}}, {update_latest_object_versions, Objects, Versions}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Id) ->
    ets:new(?LATEST_OBJECT_VERSIONS, [private, named_table]), 
    ets:new(?RUNNING_TRANSACTIONS, [private, named_table]), 

    {ok, NNodes} = application:get_env(riak_kv, transactions_manager_tree_n_nodes),
    Step = case (NNodes - 1) of 0 -> 1; S -> S end,
    State = #state{id = Id,
                   next_lsn = Id + 1,
                   step = Step},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({validate, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn}, State) ->
    do_validate(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, State);

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
  TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn,
  #state{id = Id} = State
) ->
    lager:info("Received transaction ~p for validation~n", [TransactionId]),

    {ok, NNodes} = application:get_env(riak_kv, transactions_manager_tree_n_nodes),
    Root = NNodes - 1,
    if
        Id < Root ->
            leaf_validate(TransactionId, Snapshot, Gets, Puts, NValidations, Client, State);
        true ->
            root_validate(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, State)
    end.

do_update_latest_object_versions(Objects, Version, State) ->
    do_update_latest_object_versions2(Objects, Version),
    {noreply, State}.

% Validates transaction as soon as it arrives
leaf_validate(
  TransactionId, Snapshot, Gets, Puts, NValidations, Client,
  #state{id = Id, next_lsn = NextLsn, step = Step} = State
) ->
    Lsn = generate_lsn(Snapshot, NextLsn, Step),

    lager:info("Leaf validation in progress...~n", []),
    NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
    Conflicts = check_conflicts(Gets, NbkeyPuts, Snapshot),
    lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts]),

    if
        not Conflicts ->
            do_update_latest_object_versions2(NbkeyPuts, Lsn),

            {ok, NNodes} = application:get_env(riak_kv, transactions_manager_tree_n_nodes),
            Root = NNodes - 1,
            riak_kv_transactions_validator:update_latest_object_versions(Root, NbkeyPuts, Lsn);
        true ->
            ok
    end,

    Record = riak_kv_transactions_log:new_log_record(Lsn, {TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts}),
    riak_kv_transactions_log:append(Id, Record),

    NewState = State#state{next_lsn = Lsn + Step},
    {noreply, NewState}.

generate_lsn(Snapshot, Lsn, _Step) when Lsn > Snapshot ->
    Lsn;
generate_lsn(Snapshot, Lsn, Step) when Lsn =< Snapshot ->
    generate_lsn(Snapshot, Lsn + Step, Step).

% Validates the transaction once all the info has arrived from its children
root_validate(
  TransactionId, Snapshot, Gets1, Puts1, NValidations, Client, Conflicts1, Lsn1,
  #state{id = Id} = State
) ->
    case ets:lookup(?RUNNING_TRANSACTIONS, TransactionId) of
        [{TransactionId, Gets2, Puts2, Conflicts2, Lsn2, ReceivedValidations1}] ->
            ets:insert(?RUNNING_TRANSACTIONS, {
                TransactionId,
                Gets1 ++ Gets2,
                Puts1 ++ Puts2,
                Conflicts1 or Conflicts2,
                max(Lsn1, Lsn2),
                ReceivedValidations1 + 1
            });
        [] ->
            ets:insert(?RUNNING_TRANSACTIONS, {TransactionId, Gets1, Puts1, Conflicts1, Lsn1, 1})
    end,

    [{TransactionId, Gets, Puts, Conflicts, Lsn, ReceivedValidations}] = ets:lookup(?RUNNING_TRANSACTIONS, TransactionId),
    case ReceivedValidations of
        NValidations ->
            do_root_commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn);
        _ ->
            ok
    end,

    {noreply, State}.

do_root_commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, true = Conflicts, Lsn) ->
    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, false),
    ets:delete(?RUNNING_TRANSACTIONS, TransactionId);
do_root_commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, false = _Conflicts, Lsn) ->
    lager:info("Root validation in progress...~n", []),
    NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
    Conflicts = check_conflicts(Gets, NbkeyPuts, Snapshot),
    lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts]),

    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, true),

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
