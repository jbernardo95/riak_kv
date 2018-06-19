-module(riak_kv_transactions_committer).

-behaviour(gen_server).

-export([start_link/1,
         connect_to_vnodes_cluster/2,
         commit/8,
         commit/9]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(RIAK_RING, riak_ring).

-record(state, {id}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    gen_server:start_link({global, {?MODULE, Id}}, ?MODULE, Id, []).

connect_to_vnodes_cluster(Id, VnodeClusterGatewayNode) ->
    gen_server:cast({global, {?MODULE, Id}}, {connect_to_vnodes_cluster, VnodeClusterGatewayNode}).

commit(Id, TransactionId, Lsn, Gets, Puts, NValidations, Client, Conflicts) ->
    gen_server:cast({global, {?MODULE, Id}}, {commit, TransactionId, Lsn, Gets, Puts, NValidations, Client, Conflicts, true}).

commit(Id, TransactionId, Lsn, Gets, Puts, NValidations, Client, Conflicts, InformClient) ->
    gen_server:cast({global, {?MODULE, Id}}, {commit, TransactionId, Lsn, Gets, Puts, NValidations, Client, Conflicts, InformClient}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Id) ->
    ets:new(?RIAK_RING, [private, named_table, {keypos, 2}]), 

    State = #state{id = Id},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({commit, TransactionId, Lsn, Gets, Puts, NValidations, Client, Conflicts, InformClient}, State) ->
    do_commit(TransactionId, Lsn, Gets, Puts, NValidations, Client, Conflicts, InformClient, State);

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

do_commit(TransactionId, Lsn, Gets, Puts, NValidations, Client, Conflicts, InformClient, #state{id = Id} = State) ->
    lager:info("Received transaction ~p to commit~n", [TransactionId]),

    {ok, NNodes} = application:get_env(riak_kv, transactions_manager_tree_n_nodes),
    Root = NNodes - 1,
    if
        Id < Root ->
            leaf_commit(TransactionId, Lsn, Gets, Puts, NValidations, Client, Conflicts);
        true ->
            root_commit(TransactionId, Lsn, Gets, Puts, NValidations, Client, Conflicts, InformClient)
    end,

    {noreply, State}.

do_connect_to_vnodes_cluster(VnodeClusterGatewayNode, State) ->
    {ok, Ring} = rpc:call(VnodeClusterGatewayNode, riak_core_ring_manager, get_raw_ring, []),
    {_RingSize, Vnodes1} = element(4, Ring),
    Vnodes = lists:usort(fun({_, A}, {_, B}) -> A =< B end, Vnodes1),
    ets:insert(?RIAK_RING, Vnodes),
    {noreply, State}.

% Leaf committer
% Transaction only needs one validation 
% So it can be committed right away 
leaf_commit(TransactionId, Lsn, _Gets, Puts, 1 = _NValidations, Client, Conflicts) ->
    send_validation_result_to_client(TransactionId, Lsn, Client, Conflicts),

    Node = riak_object:get_node(hd(Puts)),
    [Vnode] = ets:lookup(?RIAK_RING, Node),
    BkeyPuts = lists:map(fun riak_object:bkey/1, Puts),
    riak_kv_vnode:transaction_validation(Vnode, TransactionId, BkeyPuts, Conflicts, Lsn),

    lager:info("Transaction ~p committed~n", [TransactionId]);

% Leaf committer
% Transaction needs more than one validation
% So it is sent to a transactions manager a level up in the tree
% For now the transaction is sent to the root committer automatically
% In the future the routing code should be changed so that the transaction is sent to the correct committer
leaf_commit(TransactionId, Lsn, Gets, Puts, NValidations, Client, Conflicts) ->
    lager:info("Not enough information to commit transaction ~p, sending transaction to be validated up the tree~n", [TransactionId]),

    if
        Conflicts -> send_validation_result_to_client(TransactionId, Lsn, Client, Conflicts);
        true -> ok
    end,

    {ok, NNodes} = application:get_env(riak_kv, transactions_manager_tree_n_nodes),
    Root = NNodes - 1,
    riak_kv_transactions_validator:validate(Root, TransactionId, Lsn, Gets, Puts, NValidations, Client).

% Root committer
root_commit(TransactionId, Lsn, _Gets, Puts, _NValidations, Client, Conflicts, InformClient) ->
    if
        InformClient -> send_validation_result_to_client(TransactionId, Lsn, Client, Conflicts);
        true -> ok
    end,
    send_validation_result_to_vnodes(TransactionId, Lsn, Puts, Conflicts),
    lager:info("Transaction ~p committed~n", [TransactionId]).

send_validation_result_to_client(TransactionId, Lsn, Client, Conflicts) ->
    Reply = {transaction_commit_result, TransactionId, Conflicts, Lsn},
    riak_core_vnode:reply(Client, Reply).

send_validation_result_to_vnodes(TransactionId, Lsn, Puts, Conflicts) ->
    FoldFun = fun(Object, NodesPuts1) ->
                      Node1 = riak_object:get_node(Object),
                      Bkey = riak_object:bkey(Object),
                      case dict:find(Node1, NodesPuts1) of
                          {ok, Puts1} -> dict:store(Node1, [Bkey | Puts1], NodesPuts1);
                          error -> dict:store(Node1, [Bkey], NodesPuts1)
                      end
              end,
    NodesPuts = lists:foldl(FoldFun, dict:new(), Puts),
    Nodes = dict:fetch_keys(NodesPuts),
    lists:foreach(fun(Node) ->
                          [Vnode] = ets:lookup(?RIAK_RING, Node),
                          BkeyPuts = dict:fetch(Node, NodesPuts),
                          riak_kv_vnode:transaction_validation(Vnode, TransactionId, BkeyPuts, Conflicts, Lsn)
                  end, Nodes).
