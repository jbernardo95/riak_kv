-module(riak_kv_transactions_validator).

-behaviour(gen_server).

-export([start_link/1,
         leaf_validate/7,
         batch_validate/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(OBJECT_VERSIONS, object_versions).
-define(RUNNING_TRANSACTIONS, running_transactions).

-record(state, {id, next_lsn, step}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    gen_server:start_link({global, {?MODULE, Id}}, ?MODULE, Id, []).

leaf_validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    gen_server:cast({global, {?MODULE, Id}}, {leaf_validate, TransactionId, Snapshot, Gets, Puts, NValidations, Client}).

batch_validate(Id, TransactionsBatch) ->
    gen_server:cast({global, {?MODULE, Id}}, {batch_validate, TransactionsBatch}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Id) ->
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

handle_cast({leaf_validate, TransactionId, Snapshot, Gets, Puts, NValidations, Client}, State) ->
    do_leaf_validate(TransactionId, Snapshot, Gets, Puts, NValidations, Client, State);

handle_cast({batch_validate, TransactionsBatch}, State) ->
    do_batch_validate(TransactionsBatch, State);

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

% Validates transaction as soon as it arrives
do_leaf_validate(
  TransactionId, Snapshot, Gets, Puts, NValidations, Client,
  #state{id = Id, next_lsn = NextLsn, step = Step} = State
) ->
    %lager:info("Received transaction ~p for validation~n", [TransactionId]),

    Lsn = generate_lsn(Snapshot, NextLsn, Step),

    NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
    BlindWrite = ((length(Gets) == 0) and (length(Puts) == 1) and (NValidations == 1)),
    %lager:info("Leaf validation in progress...~n", []),
    if
        BlindWrite ->
            Conflicts = false;
            %lager:info("Blind write, conflicts false~n", []);
        true ->
            Conflicts = check_conflicts(TransactionId, Gets, NbkeyPuts, Snapshot, Lsn)
            %lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts])
    end,

    if
        (not Conflicts) and (NbkeyPuts /= []) -> do_update_object_versions(NbkeyPuts, Lsn, TransactionId);
        true -> ok
    end,

    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn),

    NewState = State#state{next_lsn = Lsn + Step},
    {noreply, NewState}.

generate_lsn(Snapshot, Lsn, _Step) when Lsn > Snapshot ->
    Lsn;
generate_lsn(Snapshot, Lsn, Step) when Lsn =< Snapshot ->
    generate_lsn(Snapshot, Lsn + Step, Step).

do_batch_validate(TransactionsBatch, State) ->
    %lager:info("Received batch of transactions to validate~n", []),

    lists:foreach(fun({TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn}) ->
                          NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
                          do_update_object_versions(NbkeyPuts, Lsn, TransactionId),

                          case NValidations of
                              1 ->
                                  % Transaction validated and commited in one of the leafs, so do nothing
                                  %lager:info("Transaction already committed~n", []),
                                  ok;
                              _ ->
                                  % Transaction must be validated again with more information
                                  root_validate(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, State)
                          end
                  end, TransactionsBatch),

    {noreply, State}.

% Validates the transaction once all the info has arrived from its children
root_validate(
  TransactionId, Snapshot, Gets1, Puts1, NValidations, Client, Conflicts1, Lsn1,
  #state{id = Id}
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
            do_root_validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn);
        _ ->
            ok
    end.

do_root_validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, true = Conflicts, Lsn) ->
    %lager:info("Conflicts found in lower validator~n", []),
    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, false),
    ets:delete(?RUNNING_TRANSACTIONS, TransactionId);
do_root_validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, false = _Conflicts, Lsn) ->
    %lager:info("Root validation in progress...~n", []),
    NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
    Conflicts = check_conflicts(TransactionId, Gets, NbkeyPuts, Snapshot, Lsn),
    %lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts]),

    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, true),

    if
        (not Conflicts) and (NbkeyPuts /= []) -> do_update_object_versions(NbkeyPuts, Lsn, TransactionId);
        true -> ok
    end,

    ets:delete(?RUNNING_TRANSACTIONS, TransactionId).

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
                    [{Nbkey, VTIds}] ->
                        check_object_conflicts(VTIds, TransactionId, Snapshot, Lsn);
                    [] ->
                        % Object has no versions so, there are no conflicts 
                        false
                end,

    NewConflict = Conflict1 or Conflict2,
    do_check_conflicts(TransactionId, Rest, Snapshot, Lsn, NewConflict).

% This function assumes versions are ordered in descending ordered
check_object_conflicts([], _TransactionId, _Snapshot, _Lsn) -> true;
check_object_conflicts([{_, TransactionId} | Rest], TransactionId, Snapshot, Lsn) ->
    check_object_conflicts(Rest, TransactionId, Snapshot, Lsn);
check_object_conflicts([{Version, _} | _], _TransactionId, Snapshot, _Lsn) when Version =< Snapshot -> false;
check_object_conflicts([{Version, _} | _], _TransactionId, Snapshot, Lsn) when Version > Snapshot, Version =< Lsn -> true;
check_object_conflicts([_ | Rest], TransactionId,  Snapshot, Lsn) ->
    check_object_conflicts(Rest, TransactionId, Snapshot, Lsn).

% This function must store object versions in descending order because check_object_conflicts/4 assumes that
do_update_object_versions(Nbkeys, Version, TransactionId) ->
    {ok, MaximumObjectVersions} = application:get_env(riak_kv, transactions_validator_maximum_object_versions),
    lists:foreach(
        fun(Nbkey) ->
            VTId = {Version, TransactionId},
            case ets:lookup(?OBJECT_VERSIONS, Nbkey) of
                [{Nbkey, VTIds}] ->
                    VTIdsLength = length(VTIds),
                    NewVTIds = case VTIdsLength of
                                   MaximumObjectVersions -> [VTId | droplast(VTIds)];
                                   _ -> [VTId | VTIds]
                               end,
                    ets:insert(?OBJECT_VERSIONS, {Nbkey, NewVTIds});
                [] ->
                    ets:insert(?OBJECT_VERSIONS, {Nbkey, [VTId]})
            end
        end,
        Nbkeys
    ).

droplast([_T])  -> [];
droplast([H | T]) -> [H | droplast(T)].
