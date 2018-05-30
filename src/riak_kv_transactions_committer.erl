-module(riak_kv_transactions_committer).

-behaviour(gen_server).

-export([start_link/1,
         connect_to_vnodes_cluster/2,
         commit/7]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(RUNNING_TRANSACTIONS, running_transactions).
-define(RIAK_RING, riak_ring).

-record(state, {id}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    gen_server:start_link({global, {?MODULE, Id}}, ?MODULE, Id, []).

connect_to_vnodes_cluster(Id, VnodeClusterGatewayNode) ->
    gen_server:cast({global, {?MODULE, Id}}, {connect_to_vnodes_cluster, VnodeClusterGatewayNode}).

commit(Id, TransactionId, Puts, NValidations, Client, Conflicts, Lsn) ->
    gen_server:cast({global, {?MODULE, Id}}, {commit, TransactionId, Puts, NValidations, Client, Conflicts, Lsn}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Id) ->
    ets:new(?RUNNING_TRANSACTIONS, [private, named_table]), 
    ets:new(?RIAK_RING, [private, named_table, {keypos, 2}]), 

    State = #state{id = Id},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({commit, TransactionId, Puts, NValidations, Client, Conflicts, Lsn}, State) ->
    do_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn, State);

handle_cast({connect_to_vnodes_cluster, VnodeClusterGatewayNode}, State) ->
    do_connect_to_vnodes_cluster(VnodeClusterGatewayNode, State);

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

do_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn, #state{id = Id} = State) ->
    {ok, NLeafTransactionsManagers} = application:get_env(riak_kv, n_leaf_transactions_managers),
    if
        Id < NLeafTransactionsManagers ->
            leaf_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn);
        true ->
            root_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn)
    end,

    {noreply, State}.

do_connect_to_vnodes_cluster(VnodeClusterGatewayNode, State) ->
    {ok, Ring} = rpc:call(VnodeClusterGatewayNode, riak_core_ring_manager, get_raw_ring, []),
    {_RingSize, Vnodes} = element(4, Ring), 
    ets:insert(?RIAK_RING, Vnodes),
    {noreply, State}.

% Root committer
root_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn1) ->
    Node = riak_object:get_node(hd(Puts)),

    % Update transaction metadata
    BkeyPuts = lists:map(fun riak_object:bkey/1, Puts),
    case ets:lookup(?RUNNING_TRANSACTIONS, TransactionId) of
        [{TransactionId, ReceivedValidations1, NodesPuts1, Lsn2}] ->
            NewNodePuts = dict:update(Node, fun(Old) -> Old ++ BkeyPuts end, BkeyPuts, NodesPuts1),
            ets:insert(?RUNNING_TRANSACTIONS, {TransactionId, ReceivedValidations1 + 1, NewNodePuts, max(Lsn1, Lsn2)});
        [] ->
            NewNodePuts = dict:store(Node, BkeyPuts, dict:new()),
            ets:insert(?RUNNING_TRANSACTIONS, {TransactionId, 1, NewNodePuts, Lsn1})
    end,

    [{TransactionId, ReceivedValidations, NodePuts, Lsn}] = ets:lookup(?RUNNING_TRANSACTIONS, TransactionId),
    case ReceivedValidations of
        NValidations ->
            send_validation_result_to_client(TransactionId, Conflicts, Lsn, Client),
            send_validation_result_to_vnodes(TransactionId, Conflicts, Lsn, NodePuts),
            lager:info("Transaction ~p committed~n", [TransactionId]),
            ets:delete(?RUNNING_TRANSACTIONS, TransactionId);
        _ ->
            ok
    end.

% Leaf committer
% Transaction only needs one validation 
% So it can be committed right away 
leaf_commit(TransactionId, Puts, 1 = _NValidations, Client, Conflicts, Lsn) ->
    send_validation_result_to_client(TransactionId, Conflicts, Lsn, Client),

    Node = riak_object:get_node(hd(Puts)),
    [Vnode] = ets:lookup(?RIAK_RING, Node),
    BkeyPuts = lists:map(fun riak_object:bkey/1, Puts),
    riak_kv_vnode:transaction_validation(Vnode, TransactionId, BkeyPuts, Conflicts, Lsn),

    lager:info("Transaction ~p committed~n", [TransactionId]),

    ets:delete(?RUNNING_TRANSACTIONS, TransactionId);

% Leaf committer
% Transaction needs more than one validation
% So it is sent to a committer a level up in the tree
% For now the transaction is sent to the root committer automatically
% In the future the routing code should be changed so that the transaction is sent to the correct committer
leaf_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn) ->
    {ok, Root} = application:get_env(riak_kv, n_leaf_transactions_managers),
    riak_kv_transactions_committer:commit(Root, TransactionId, Puts, NValidations, Client, Conflicts, Lsn).

send_validation_result_to_client(TransactionId, Conflicts, Lsn, Client) ->
    Reply = {transaction_commit_result, TransactionId, Conflicts, Lsn},
    riak_core_vnode:reply(Client, Reply).

send_validation_result_to_vnodes(TransactionId, Conflicts, Lsn, NodePuts) ->
    Nodes = dict:fetch_keys(NodePuts),
    lists:foreach(fun(Node) ->
                          [Vnode] = ets:lookup(?RIAK_RING, Node),
                          Puts = dict:fetch(Node, NodePuts),
                          riak_kv_vnode:transaction_validation(Vnode, TransactionId, Puts, Conflicts, Lsn)
                  end, Nodes).
