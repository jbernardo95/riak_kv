-module(riak_kv_transactions_validator).

-behaviour(gen_server).

-export([start_link/1,
         leaf_validate/7,
         batch_validate/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(LATEST_OBJECT_VERSIONS, latest_object_versions).
-define(CHILD_LSNS, child_lsns).
-define(RECEIVE_BUFFER, receive_buffer).
-define(WAITING_STABILITY, waiting_stability).

-record(state, {id, next_lsn, step}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    gen_server:start_link({global, {?MODULE, Id}}, ?MODULE, Id, []).

leaf_validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    gen_server:cast({global, {?MODULE, Id}}, {leaf_validate, TransactionId, Snapshot, Gets, Puts, NValidations, Client}).

batch_validate(Id, ChildId, TransactionsBatch) ->
    gen_server:cast({global, {?MODULE, Id}}, {batch_validate, ChildId, TransactionsBatch}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Id) ->
    ets:new(?LATEST_OBJECT_VERSIONS, [private, named_table]), 
    ets:new(?CHILD_LSNS, [private, named_table]), 
    ets:new(?RECEIVE_BUFFER, [private, named_table]), 
    ets:new(?WAITING_STABILITY, [private, named_table, ordered_set]), 

    {ok, NNodes} = application:get_env(riak_kv, transactions_manager_tree_n_nodes),
    Root = NNodes - 1,
    if
        Id < Root ->
            {ok, MoveLsnForwardInterval} = application:get_env(riak_kv, transactions_manager_move_lsn_forward_interval),
            erlang:send_after(MoveLsnForwardInterval, self(), move_lsn_forward);
        true -> ok
    end,

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

handle_cast({batch_validate, ChildId, TransactionsBatch}, State) ->
    do_batch_validate(ChildId, TransactionsBatch, State);

handle_cast(Request, State) ->
    lager:error("Unexpected request received at hanlde_cast: ~p~n", [Request]),
    {noreply, State}.

handle_info(move_lsn_forward, #state{id = Id, next_lsn = NextLsn, step = Step} = State) ->
    NewNextLsn = NextLsn + Step,
    lager:info("Moving lsn of validator ~p forward to ~p~n", [Id, NewNextLsn]),

    riak_kv_transactions_committer:propagate_lsn(Id, NewNextLsn),

    {ok, MoveLsnForwardInterval} = application:get_env(riak_kv, transactions_manager_move_lsn_forward_interval),
    erlang:send_after(MoveLsnForwardInterval, self(), move_lsn_forward),

    NewState = State#state{next_lsn = NewNextLsn},
    {noreply, NewState};

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

    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn),

    NewState = State#state{next_lsn = Lsn + Step},
    {noreply, NewState}.

generate_lsn(Snapshot, Lsn, _Step) when Lsn > Snapshot ->
    Lsn;
generate_lsn(Snapshot, Lsn, Step) when Lsn =< Snapshot ->
    generate_lsn(Snapshot, Lsn + Step, Step).

do_batch_validate(ChildId, TransactionsBatch, #state{id = Id} = State) ->
    lager:info("Received batch of transactions to validate~n", []),

    % Process batch
    lists:foreach(fun(Message) ->
                          case Message of
                              {transaction, {TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn}} ->
                                  ets:insert(?CHILD_LSNS, {ChildId, Lsn}),
                                  receive_transaction(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn);
                              {lsn, Lsn} ->
                                  ets:insert(?CHILD_LSNS, {ChildId, Lsn})
                          end
                  end, TransactionsBatch),

    % Validate transactions that can be validated
    StableLsn = lists:foldl(fun([{_ChildId, Lsn}], Acc) ->
                                    case Acc of
                                        undefined -> Lsn;
                                        _ -> min(Acc, Lsn)
                                    end
                            end, undefined, ets:match(?CHILD_LSNS, '$1')),
    root_validate(Id, StableLsn),

    {noreply, State}.

% Transaction validated and commited in one of the leafs
receive_transaction(
  _TransactionId, _Snapshot, _Gets, Puts,
  1 = _NValidations, _Client, _Conflicts, Lsn
) ->
    NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
    update_latest_object_versions(NbkeyPuts, Lsn),
    ok;

% Transaction partially validated in one of the leafs
receive_transaction(
  TransactionId, Snapshot, Gets1, Puts1, 
  NValidations, Client, Conflicts1, Lsn1
) ->
    case ets:lookup(?RECEIVE_BUFFER, TransactionId) of
        [{TransactionId, Gets2, Puts2, Conflicts2, Lsn2, ReceivedValidations1}] ->
            ets:insert(?RECEIVE_BUFFER, {
                TransactionId,
                Gets1 ++ Gets2,
                Puts1 ++ Puts2,
                Conflicts1 or Conflicts2,
                max(Lsn1, Lsn2),
                ReceivedValidations1 + 1
            });
        [] ->
            ets:insert(?RECEIVE_BUFFER, {TransactionId, Gets1, Puts1, Conflicts1, Lsn1, 1})
    end,

    [{TransactionId, Gets, Puts, Conflicts, Lsn, ReceivedValidations}] = ets:lookup(?RECEIVE_BUFFER, TransactionId),
    case ReceivedValidations of
        NValidations ->
            Transaction = {TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts},

            case ets:lookup(?WAITING_STABILITY, Lsn) of
                [{Lsn, [WaitingStability]}] ->
                    ets:insert(?WAITING_STABILITY, {Lsn, [Transaction | WaitingStability]});
                [] ->
                    ets:insert(?WAITING_STABILITY, {Lsn, [Transaction]})
            end,

            ets:delete(?RECEIVE_BUFFER, TransactionId);
        _ ->
            ok
    end.

root_validate(Id, StableLsn) ->
    Lsn = ets:first(?WAITING_STABILITY),
    if
        Lsn =< StableLsn ->
            Transactions = ets:lookup_element(?WAITING_STABILITY, Lsn, 2),
            lists:foreach(fun({TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts}) ->
                                  do_root_validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn)
                          end, Transactions),
            ets:delete(?WAITING_STABILITY, Lsn),
            root_validate(Id, StableLsn);
        true ->
            ok
    end.

do_root_validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, true = Conflicts, Lsn) ->
    lager:info("Conflicts found in lower validator~n", []),
    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, false);

do_root_validate(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, false = _Conflicts, Lsn) ->
    lager:info("Root validation in progress...~n", []),
    NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
    Conflicts = check_conflicts(Gets ++ NbkeyPuts, Snapshot),
    lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts]),

    riak_kv_transactions_committer:commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, true),

    if
        not Conflicts -> update_latest_object_versions(NbkeyPuts, Lsn);
        true -> ok
    end.

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
