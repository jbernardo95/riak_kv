-module(riak_kv_transactions_committer).

-behaviour(gen_server).

-export([start_link/1,
         connect_to_vnodes_cluster/2,
         commit/9,
         commit/10]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(RIAK_RING, riak_ring).
-define(BATCH, batch).
-define(STATS, stats_transactions_committer).

-record(state, {id, timer}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    gen_server:start_link({global, {?MODULE, Id}}, ?MODULE, Id, []).

connect_to_vnodes_cluster(Id, VnodeClusterGatewayNode) ->
    gen_server:cast({global, {?MODULE, Id}}, {connect_to_vnodes_cluster, VnodeClusterGatewayNode}).

commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn) ->
    gen_server:cast({global, {?MODULE, Id}}, {commit, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, true}).

commit(Id, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, InformClient) ->
    gen_server:cast({global, {?MODULE, Id}}, {commit, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, InformClient}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Id) ->
    ets:new(?RIAK_RING, [private, named_table, {keypos, 2}]), 
    ets:new(?BATCH, [private, named_table]), 

    erlang:send(self(), send_batch),

    ets:new(?STATS, [private, named_table]), 
    ets:insert(?STATS, {to_client, 0}), 
    erlang:send_after(60000, self(), print_stats),

    State = #state{id = Id},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({connect_to_vnodes_cluster, VnodeClusterGatewayNode}, State) ->
    do_connect_to_vnodes_cluster(VnodeClusterGatewayNode, State);

handle_cast({commit, TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, InformClient}, State) ->
    do_commit(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, InformClient, State);

handle_cast(Request, State) ->
    lager:error("Unexpected request received at hanlde_cast: ~p~n", [Request]),
    {noreply, State}.

handle_info(print_stats, State) ->
    ToClient = ets:lookup_element(?STATS, to_client, 2),

    lager:info("### ~p COUNT STATS ###~n" ++
               "to_client = ~p~n", [?MODULE, ToClient]),

    erlang:send_after(60000, self(), print_stats),

    {noreply, State};

handle_info(send_batch, State) ->
    do_send_batch(State);

handle_info(Info, State) ->
    lager:error("Unexpected info received at handle_info: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_connect_to_vnodes_cluster(VnodeClusterGatewayNode, State) ->
    {ok, Ring} = rpc:call(VnodeClusterGatewayNode, riak_core_ring_manager, get_raw_ring, []),
    {_RingSize, Vnodes1} = element(4, Ring),
    Vnodes = lists:usort(fun({_, A}, {_, B}) -> A =< B end, Vnodes1),
    ets:insert(?RIAK_RING, Vnodes),
    {noreply, State}.

do_send_batch(#state{id = Id} = State) ->
    {ok, NNodes} = application:get_env(riak_kv, transactions_manager_tree_n_nodes),
    Root = NNodes - 1,
    if
        Id < Root -> leaf_send_batch();
        true -> root_send_batch()
    end,
    
    {ok, BatchTimeout} = application:get_env(riak_kv, transactions_manager_batch_timeout),
    Timer = erlang:send_after(BatchTimeout, self(), send_batch),

    NewState = State#state{timer = Timer},
    {noreply, NewState}.

leaf_send_batch() ->
    FoldFun = fun(I, Acc) ->
                  Transaction = ets:lookup_element(?BATCH, I, 2),
                  [Transaction | Acc]
              end,
    Size = batch_size(),
    TransactionsBatch = lists:foldl(FoldFun, [], lists:reverse(lists:seq(1, Size))),

    case Size of
        0 -> ok;
        _ ->
            {ok, NNodes} = application:get_env(riak_kv, transactions_manager_tree_n_nodes),
            Root = NNodes - 1,
            %lager:info("Sending batch with size ~p up the tree~n", [Size]),
            riak_kv_transactions_validator:batch_validate(Root, TransactionsBatch)
    end,

    ets:insert(?BATCH, {size, 0}).

batch_size() ->
    case ets:lookup(?BATCH, size) of
        [{size, Size}] -> Size;
        [] -> 0
    end.

root_send_batch() ->
    Vnodes = ets:match(?RIAK_RING, '$1'),
    lists:foreach(fun([{_, Node} = Vnode]) ->
                          FoldFun = fun(I, Acc) ->
                                            TransactionValidation = ets:lookup_element(?BATCH, {Node, I}, 2),
                                            [TransactionValidation | Acc]
                                    end,
                          Size = batch_size(Node),
                          TransactionsValidationBatch = lists:foldl(FoldFun, [], lists:reverse(lists:seq(1, Size))),

                          case Size of
                              0 -> ok;
                              _ ->
                                  %lager:info("Sending batch with size ~p to vnode ~p~n", [Size, Vnode]),
                                  riak_kv_vnode:transaction_validation_batch(Vnode, TransactionsValidationBatch)
                          end,

                          ets:insert(?BATCH, {{Node, size}, 0})
                  end, Vnodes),

    ets:insert(?BATCH, {size, 0}).

batch_size(Node) ->
    case ets:lookup(?BATCH, {Node, size}) of
        [{{Node, size}, Size}] -> Size;
        [] -> 0
    end.

do_commit(
  TransactionId,
  Snapshot,
  Gets,
  Puts,
  NValidations,
  Client,
  Conflicts,
  Lsn,
  InformClient,
  #state{id = Id,
         timer = Timer} = State
) ->
    %lager:info("Received transaction ~p to commit~n", [TransactionId]),

    {ok, NNodes} = application:get_env(riak_kv, transactions_manager_tree_n_nodes),
    Root = NNodes - 1,
    if
        Id < Root ->
            leaf_commit(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn);
        true ->
            root_commit(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn, InformClient)
    end,

    CurrentBatchSize = ets:lookup_element(?BATCH, size, 2),
    {ok, BatchSize} = application:get_env(riak_kv, transactions_manager_batch_size),
    case CurrentBatchSize of
        BatchSize ->
            erlang:cancel_timer(Timer),
            do_send_batch(State);
        _ ->
            {noreply, State}
    end.

% Leaf committer
% Transaction only needs one validation 
% So it can be committed right away 
leaf_commit(TransactionId, Snapshot, Gets, Puts, 1 = NValidations, Client, Conflicts, Lsn) ->
    send_validation_result_to_client(TransactionId, Lsn, Client, Conflicts),

    leaf_add_to_batch(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn),

    send_validation_result_to_vnodes(TransactionId, Lsn, Puts, Conflicts);

    %lager:info("Transaction ~p committed~n", [TransactionId]);

% Leaf committer
% Transaction needs more than one validation
% So it is sent to a transactions manager a level up in the tree
% For now the transaction is sent to the root committer automatically
% In the future the routing code should be changed so that the transaction is sent to the correct committer
leaf_commit(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn) ->
    %lager:info("Not enough information to commit transaction ~p, sending transaction to be validated up the tree~n", [TransactionId]),

    if
        Conflicts -> send_validation_result_to_client(TransactionId, Lsn, Client, Conflicts);
        true -> ok
    end,

    leaf_add_to_batch(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn).

% Root committer
root_commit(TransactionId, _Snapshot, _Gets, Puts, _NValidations, Client, Conflicts, Lsn, InformClient) ->
    if
        InformClient -> send_validation_result_to_client(TransactionId, Lsn, Client, Conflicts);
        true -> ok
    end,

    root_add_to_batch(TransactionId, Lsn, Puts, Conflicts).

    %lager:info("Transaction ~p committed~n", [TransactionId]).

send_validation_result_to_client(TransactionId, Lsn, Client, Conflicts) ->
    ToClient = ets:lookup_element(?STATS, to_client, 2),
    ets:insert(?STATS, {to_client, ToClient + 1}),

    Reply = {transaction_commit_result, TransactionId, Conflicts, Lsn},
    riak_core_vnode:reply(Client, Reply).

send_validation_result_to_vnodes(TransactionId, Lsn, Puts, Conflicts) ->
    NodesPuts = group_puts_per_node(Puts),
    Nodes = dict:fetch_keys(NodesPuts),
    lists:foreach(fun(Node) ->
                          [Vnode] = ets:lookup(?RIAK_RING, Node),
                          BkeyPuts = dict:fetch(Node, NodesPuts),

                          riak_kv_vnode:transaction_validation(Vnode, TransactionId, BkeyPuts, Conflicts, Lsn)
                  end, Nodes).

leaf_add_to_batch(TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn) ->
    InsertAt = ets:update_counter(?BATCH, size, 1),
    Transaction = {TransactionId, Snapshot, Gets, Puts, NValidations, Client, Conflicts, Lsn},
    ets:insert(?BATCH, {InsertAt, Transaction}).

root_add_to_batch(TransactionId, Lsn, Puts, Conflicts) ->
    ets:update_counter(?BATCH, size, 1),

    NodesPuts = group_puts_per_node(Puts),
    Nodes = dict:fetch_keys(NodesPuts),
    lists:foreach(fun(Node) ->
                          InsertAt = ets:update_counter(?BATCH, {Node, size}, 1),
                          BkeyPuts = dict:fetch(Node, NodesPuts),
                          TransactionValidation = {TransactionId, BkeyPuts, Conflicts, Lsn},

                          ets:insert(?BATCH, {{Node, InsertAt}, TransactionValidation})
                  end, Nodes).

group_puts_per_node(Puts) ->
    FoldFun = fun(Object, NodesPuts1) ->
                      Node1 = riak_object:get_node(Object),
                      Bkey = riak_object:bkey(Object),
                      case dict:find(Node1, NodesPuts1) of
                          {ok, Puts1} -> dict:store(Node1, [Bkey | Puts1], NodesPuts1);
                          error -> dict:store(Node1, [Bkey], NodesPuts1)
                      end
              end,
    lists:foldl(FoldFun, dict:new(), Puts).
