-module(riak_kv_transactions_validator).

-behaviour(gen_server).

-export([start_link/1,
         validate/7,
         validate/9,
         update_object_versions/4]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(OBJECT_VERSIONS, object_versions).
-define(RUNNING_TRANSACTIONS, running_transactions).
-define(STATS, stats).

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

update_object_versions(Id, Objects, Version, TransactionId) ->
    gen_server:cast({global, {?MODULE, Id}}, {update_object_versions, Objects, Version, TransactionId}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Id) ->
    ets:new(?STATS, [private, named_table]), 
    ets:insert(?STATS, {n_validations, 0}),
    ets:insert(?STATS, {n_aborts, 0}),
    erlang:send_after(60000, self(), print_stats),

    ets:new(?OBJECT_VERSIONS, [private, named_table]), 
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

handle_cast({update_object_versions, Objects, Version, TransactionId}, State) ->
    do_update_object_versions(Objects, Version, TransactionId, State);

handle_cast(Request, State) ->
    lager:error("Unexpected request received at hanlde_cast: ~p~n", [Request]),
    {noreply, State}.

handle_info(print_stats, State) ->
    NValidations = ets:lookup_element(?STATS, n_validations, 2),
    NAborts = ets:lookup_element(?STATS, n_aborts, 2),
    lager:info("STATS: n_validations: ~p, n_aborts: ~p~n", [NValidations, NAborts]),
    erlang:send_after(60000, self(), print_stats),
    {noreply, State};

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

do_update_object_versions(Objects, Version, TransactionId, State) ->
    do_update_object_versions2(Objects, Version, TransactionId),
    {noreply, State}.

% Validates transaction as soon as it arrives
leaf_validate(
  TransactionId, Snapshot, Gets, Puts, NValidations, Client,
  #state{id = Id, next_lsn = NextLsn, step = Step} = State
) ->
    Lsn = generate_lsn(Snapshot, NextLsn, Step),

    NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
    BlindWrite = ((length(Gets) == 0) and (length(Puts) == 1) and (NValidations == 1)),
    if
        BlindWrite ->
            Conflicts = false;
        true ->
            lager:info("Leaf validation in progress...~n", []),
            Conflicts = check_conflicts(TransactionId, Gets, NbkeyPuts, Snapshot, Lsn),
            lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts])
    end,

    if
        not Conflicts ->
            do_update_object_versions2(NbkeyPuts, Lsn, TransactionId),
            {ok, NNodes} = application:get_env(riak_kv, transactions_manager_tree_n_nodes),
            Root = NNodes - 1,
            riak_kv_transactions_validator:update_object_versions(Root, NbkeyPuts, Lsn, TransactionId);
        true ->
            A = ets:lookup_element(?STATS, n_aborts, 2),
            ets:insert(?STATS, {n_aborts, A + 1}),
            ok
    end,

    Record = riak_kv_transactions_log:new_log_record(Lsn, {TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts}),
    riak_kv_transactions_log:append(Id, Record),

    V = ets:lookup_element(?STATS, n_validations, 2),
    ets:insert(?STATS, {n_validations, V + 1}),

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
    Conflicts = check_conflicts(TransactionId, Gets, NbkeyPuts, Snapshot, Lsn),
    lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts]),

    V = ets:lookup_element(?STATS, n_validations, 2),
    ets:insert(?STATS, {n_validations, V + 1}),

    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, true),

    if
        not Conflicts -> do_update_object_versions2(NbkeyPuts, Lsn, TransactionId);
        true ->
            A = ets:lookup_element(?STATS, n_aborts, 2),
            ets:insert(?STATS, {n_aborts, A + 1}),
            ok
    end,

    ets:delete(?RUNNING_TRANSACTIONS, TransactionId).

do_update_object_versions2(Nbkeys, Version, TransactionId) ->
    lists:foreach(
        fun(Nbkey) ->
            case ets:lookup(?OBJECT_VERSIONS, Nbkey) of
                [{Nbkey, VTIds}] ->
                    MaximumObjectVersions = app_helper:get_env(riak_kv, maximum_object_versions),
                    NewVersions = lists:sublist([{Version, TransactionId} | VTIds], 1, MaximumObjectVersions),
                    ets:insert(?OBJECT_VERSIONS, {Nbkey, NewVersions});
                [] ->
                    ets:insert(?OBJECT_VERSIONS, {Nbkey, [{Version, TransactionId}]})
            end
        end,
        Nbkeys
    ).

check_conflicts(TransactionId, Gets, Puts, Snapshot, Lsn) ->
    ConflictsGets = do_check_conflicts(TransactionId, Gets, Snapshot, Lsn),
    ConflictsPuts = do_check_conflicts(TransactionId, Puts, Snapshot, Lsn),
    ConflictsGets or ConflictsPuts.

do_check_conflicts(TransactionId, Objects, Snapshot, Lsn) ->
    do_check_conflicts(TransactionId, Objects, Snapshot, Lsn, false).

do_check_conflicts(_TransactionId, _Objects, _Snapshot, _Lsn, true) -> true;
do_check_conflicts(_TransactionId, [], _Snapshot, _Lsn, Conflict) -> Conflict;
do_check_conflicts(TransactionId, [Nbkey | Rest], Snapshot, Lsn, Conflict1) ->
    Conflict2 = case ets:lookup(?OBJECT_VERSIONS, Nbkey) of
                    [{Nbkey, VTIds}] -> check_object_conflicts(VTIds, TransactionId, Snapshot, Lsn);
                    [] -> false
                end,

    NewConflict = Conflict1 or Conflict2,
    do_check_conflicts(TransactionId, Rest, Snapshot, Lsn, NewConflict).

check_object_conflicts([], _TransactionId, _Snapshot, _Lsn) -> false;
check_object_conflicts([{_, TransactionId} | Rest], TransactionId, Snapshot, Lsn) ->
    check_object_conflicts(Rest, TransactionId, Snapshot, Lsn);
check_object_conflicts([{Version, _} | _], _TransactionId, Snapshot, _Lsn) when Version =< Snapshot -> false;
check_object_conflicts([{Version, _} | _], _TransactionId, Snapshot, Lsn) when Version > Snapshot, Version =< Lsn -> true;
check_object_conflicts([_ | Rest], TransactionId,  Snapshot, Lsn) ->
    check_object_conflicts(Rest, TransactionId, Snapshot, Lsn).
