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

-define(LATEST_OBJECT_VERSIONS, latest_object_versions).
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
    lager:info("Received transaction ~p for validation~n", [TransactionId]),

    Lsn = generate_lsn(Snapshot, NextLsn, Step),

    NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
    BlindWrite = ((length(Gets) == 0) and (length(Puts) == 1) and (NValidations == 1)),
    lager:info("Leaf validation in progress...~n", []),
    if
        BlindWrite ->
            Conflicts = false,
            lager:info("Blind write, conflicts false~n", []);
        true ->
            Conflicts = check_conflicts(Gets ++ NbkeyPuts, Snapshot),
            lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts])
    end,

    if
        not Conflicts -> update_latest_object_versions(NbkeyPuts, Lsn);
        true -> ok
    end,

    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Puts, NValidations, Client, Conflicts, Lsn),

    NewState = State#state{next_lsn = Lsn + Step},
    {noreply, NewState}.

generate_lsn(Snapshot, Lsn, _Step) when Lsn > Snapshot ->
    Lsn;
generate_lsn(Snapshot, Lsn, Step) when Lsn =< Snapshot ->
    generate_lsn(Snapshot, Lsn + Step, Step).

check_conflicts(Objects, Snapshot) ->
    check_conflicts(Objects, Snapshot, false).

check_conflicts(_Objects, _Snapshot, true) -> true;
check_conflicts([], _Snapshot, Conflict) -> Conflict;
check_conflicts([Nbkey | Rest], Snapshot, _Conflict) ->
    Conflict = case ets:lookup(?LATEST_OBJECT_VERSIONS, Nbkey) of
                   [{Nbkey, Version}] -> Version > Snapshot;
                   [] -> false
               end,

    check_conflicts(Rest, Snapshot, Conflict).

update_latest_object_versions(Nbkeys, Version) ->
    lists:foreach(fun(Nbkey) ->
                          ets:insert(?LATEST_OBJECT_VERSIONS, {Nbkey, Version})
                  end, Nbkeys).

do_batch_validate(TransactionsBatch, State) ->
    lager:info("Received batch of transactions to validate~n", []),

    lists:foreach(fun({TransactionId, Snapshot, Puts, NValidations, Client, Conflicts, Lsn}) ->
                          root_validate(TransactionId, Snapshot, Puts, NValidations, Client, Conflicts, Lsn, State)
                  end, TransactionsBatch),

    {noreply, State}.

% Transaction validated and commited in one of the leafs, so do nothing
root_validate(
  _TransactionId, _Snapshot, _Puts1, 1 = _NValidations,
  _Client, _Conflicts, _Lsn, _State
) -> ok;

% Validates the transaction once all the info has arrived from its children
root_validate(
  TransactionId, Snapshot, Puts1, NValidations,
  Client, Conflicts1, Lsn1,
  #state{id = Id}
) ->
    case ets:lookup(?RUNNING_TRANSACTIONS, TransactionId) of
        [{TransactionId, Puts2, Conflicts2, Lsn2, ReceivedValidations1}] ->
            ets:insert(?RUNNING_TRANSACTIONS, {
                TransactionId,
                Puts1 ++ Puts2,
                Conflicts1 or Conflicts2,
                max(Lsn1, Lsn2),
                ReceivedValidations1 + 1
            });
        [] ->
            ets:insert(?RUNNING_TRANSACTIONS, {TransactionId, Puts1, Conflicts1, Lsn1, 1})
    end,

    [{TransactionId, Puts, Conflicts, Lsn, ReceivedValidations}] = ets:lookup(?RUNNING_TRANSACTIONS, TransactionId),
    case ReceivedValidations of
        NValidations ->
            lager:info("All partial validation from transaction ~p received~n", [TransactionId]),

            riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Puts, NValidations, Client, Conflicts, Lsn, true),

            ets:delete(?RUNNING_TRANSACTIONS, TransactionId);
        _ ->
            ok
    end.
