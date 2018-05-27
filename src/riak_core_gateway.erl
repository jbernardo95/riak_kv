% This module is way to access the the riak_core cluster state
% without actually joining the cluster.
%
% When the gen_server starts, it gets the cluster state it needs by
% contacting a node inside the cluster and saves it locally to use
% later.
%
% Because the gen_server only gets the state once when it starts up,
% it can end up using outdated state.
%
% This module was built as an hack and with the assumption that the
% riak_core cluster never changed (which is not a valid assumption
% for real world scenarios). Use this module carefully.

-module(riak_core_gateway).

-behaviour(gen_server).

-export([start_link/1,
         connect_to_vnodes_cluster/2,
         get_bkey_vnode/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(BUCKET_PROPS, bucket_props).

-record(state, {vnode_cluster_gateway_node, chbin}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    gen_server:start_link({global, {?MODULE, Id}}, ?MODULE, Id, []).

connect_to_vnodes_cluster(Id, VnodeClusterGatewayNode) ->
    gen_server:call({global, {?MODULE, Id}}, {connect_to_vnodes_cluster, VnodeClusterGatewayNode}).

get_bkey_vnode(Id, Bkey) ->
    gen_server:call({global, {?MODULE, Id}}, {get_bkey_vnode, Bkey}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    ets:new(?BUCKET_PROPS, [private, named_table]), 

    State = #state{vnode_cluster_gateway_node = undefined, chbin = undefined},
    {ok, State}.

handle_call({connect_to_vnodes_cluster, VnodeClusterGatewayNode}, _From, State) ->
    do_connect_to_vnodes_cluster(VnodeClusterGatewayNode, State);

handle_call({get_bkey_vnode, Bkey}, _From, State) ->
    do_get_bkey_vnode(Bkey, State);

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

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

do_connect_to_vnodes_cluster(VnodeClusterGatewayNode, State) ->
    {ok, CHBin} = rpc:call(VnodeClusterGatewayNode, riak_core_ring_manager, get_chash_bin, []),
    NewState = State#state{vnode_cluster_gateway_node = VnodeClusterGatewayNode, chbin = CHBin},
    {reply, ok, NewState}.

do_get_bkey_vnode({Bucket, _Key} = Bkey, #state{vnode_cluster_gateway_node = VnodeClusterGatewayNode, chbin = CHBin} = State) ->
    BucketProps = get_bucket_props(Bucket, VnodeClusterGatewayNode),
    DocIdx = riak_core_util:chash_key(Bkey, BucketProps),
    Itr = chashbin:iterator(DocIdx, CHBin),
    {[Vnode], _} = chashbin:itr_pop(1, Itr),
    {reply, Vnode, State}.

get_bucket_props(Bucket, VnodeClusterGatewayNode) ->
    case ets:lookup(?BUCKET_PROPS, Bucket) of
        [{Bucket, Props}] ->
            Props;
        [] ->
            Props = rpc:call(VnodeClusterGatewayNode, riak_core_bucket, get_bucket, [Bucket]),
            ets:insert(?BUCKET_PROPS, {Bucket, Props}),
            Props
    end.
