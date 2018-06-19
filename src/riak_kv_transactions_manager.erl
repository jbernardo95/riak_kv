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
    {ok, Backend} = application:get_env(riak_kv, transactions_manager_backend),
    {ok, VnodeClusterGatewayNode} = application:get_env(riak_kv, vnode_cluster_gateway_node),
    case net_adm:ping(VnodeClusterGatewayNode) of
        pong -> do_connect_to_vnodes_cluster(Backend, Id, VnodeClusterGatewayNode);
        pang -> {error, vnode_cluster_gateway_node_unreachable}
    end.

validate_and_commit(TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    {ok, Backend} = application:get_env(riak_kv, transactions_manager_backend),
    do_validate_and_commit(Backend, TransactionId, Snapshot, Gets, Puts, NValidations, Client).

%%%===================================================================
%%% supervisor callbacks
%%%===================================================================

init(Id) ->
    {ok, Backend} = application:get_env(riak_kv, transactions_manager_backend),
    ChildSpecs = case Backend of
                     tree ->
                         Validator = {riak_kv_transactions_validator,
                                      {riak_kv_transactions_validator, start_link, [Id]}, permanent, 5000, worker, [riak_kv_transactions_validator]},
                         Log = {riak_kv_transactions_log,
                                {riak_kv_transactions_log, start_link, [Id]},
                                permanent, 5000, worker, [riak_kv_transactions_log]},
                         Committer = {riak_kv_transactions_committer,
                                      {riak_kv_transactions_committer, start_link, [Id]},
                                      permanent, 5000, worker, [riak_kv_transactions_committer]},
                        [Validator, Log, Committer];
                     sequencer ->
                         Sequencer = {riak_kv_transactions_sequencer,
                                      {riak_kv_transactions_sequencer, start_link, []},
                                      permanent, 5000, worker, [riak_kv_transactions_sequencer]},
                         [Sequencer]
                 end,

    SupFlags = {one_for_one, 10, 10},

    {ok, {SupFlags, ChildSpecs}}.

do_connect_to_vnodes_cluster(tree, Id, VnodeClusterGatewayNode) ->
    riak_kv_transactions_committer:connect_to_vnodes_cluster(Id, VnodeClusterGatewayNode);
do_connect_to_vnodes_cluster(sequencer, _Id, VnodeClusterGatewayNode) ->
    riak_kv_transactions_sequencer:connect_to_vnodes_cluster(VnodeClusterGatewayNode).

do_validate_and_commit(tree, TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    TransactionsManagerId = get_transactions_manager_id(),
    riak_kv_transactions_validator:validate(TransactionsManagerId, TransactionId, Snapshot, Gets, Puts, NValidations, Client);
do_validate_and_commit(sequencer, TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    riak_kv_transactions_sequencer:validate_and_commit(TransactionId, Snapshot, Gets, Puts, NValidations, Client).

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
