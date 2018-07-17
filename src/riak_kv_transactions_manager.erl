-module(riak_kv_transactions_manager).

-behaviour(supervisor).

-export([start_link/1, connect_to_vnodes_cluster/1, validate_and_commit/6]).

-export([init/1]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    supervisor:start_link({global, {?MODULE, Id}}, ?MODULE, Id).

connect_to_vnodes_cluster(Id) ->
    {ok, VnodeClusterGatewayNode} = application:get_env(riak_kv, vnode_cluster_gateway_node),
    case net_adm:ping(VnodeClusterGatewayNode) of
        pong -> do_connect_to_vnodes_cluster(Id, VnodeClusterGatewayNode);
        pang -> {error, vnode_cluster_gateway_node_unreachable}
    end.

validate_and_commit(TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    do_validate_and_commit(TransactionId, Snapshot, Gets, Puts, NValidations, Client).

%%%===================================================================
%%% supervisor callbacks
%%%===================================================================

init(Id) ->
    Validator = {riak_kv_transactions_validator,
                 {riak_kv_transactions_validator, start_link, [Id]}, permanent, 5000, worker, [riak_kv_transactions_validator]},
    Log = {riak_kv_transactions_log,
           {riak_kv_transactions_log, start_link, [Id]},
           permanent, 5000, worker, [riak_kv_transactions_log]},
    Committer = {riak_kv_transactions_committer,
                 {riak_kv_transactions_committer, start_link, [Id]},
                 permanent, 5000, worker, [riak_kv_transactions_committer]},

    ChildSpecs = [Validator, Log, Committer],
    SupFlags = {one_for_one, 10, 10},

    {ok, {SupFlags, ChildSpecs}}.

do_connect_to_vnodes_cluster(Id, VnodeClusterGatewayNode) ->
    riak_kv_transactions_committer:connect_to_vnodes_cluster(Id, VnodeClusterGatewayNode).

do_validate_and_commit(TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    TransactionsManagerId = get_transactions_manager_id(),
    riak_kv_transactions_validator:validate(TransactionsManagerId, TransactionId, Snapshot, Gets, Puts, NValidations, Client).

get_transactions_manager_id() ->
    {ok, NNodes} = application:get_env(riak_kv, transactions_manager_tree_n_nodes),
    NLeafTransactionsManagers = case (NNodes - 1) of 0 -> 1; S -> S end,
    Nodes = lists:sort(riak_core_node_watcher:nodes(riak_kv)),
    {Index, _} = lists:foldl(fun(Node, {Pos, I}) ->
                              if
                                  node() == Node -> {I, I};
                                  true -> {Pos, I + 1} 
                              end
                          end, {-1, 0}, Nodes),
    Index rem NLeafTransactionsManagers.
