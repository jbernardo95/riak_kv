-module(riak_kv_transactions_coordinator).

-behaviour(gen_server).

-export([start_link/0,
         connect_to_vnodes_cluster/1,
         batch_commit/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {prepare_results, vnode_message_batchers}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

connect_to_vnodes_cluster(VnodeClusterGatewayNode) ->
    gen_server:cast({global, ?MODULE}, {connect_to_vnodes_cluster, VnodeClusterGatewayNode}).

batch_commit(Vnode, Batch) ->
    gen_server:cast({global, ?MODULE}, {batch_commit, Vnode, Batch}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    PrepareResults = ets:new(prepare_results, [private]), 
    VnodeMessageBatchers = ets:new(vnode_message_batchers, [private]), 
    State = #state{prepare_results = PrepareResults,
                   vnode_message_batchers = VnodeMessageBatchers},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({connect_to_vnodes_cluster, VnodeClusterGatewayNode}, State) ->
    do_connect_to_vnodes_cluster(VnodeClusterGatewayNode, State),
    {noreply, State};

handle_cast({batch_commit, Vnode, Batch}, State) ->
    NewState = do_batch_commit(Vnode, Batch, State),
    {noreply, NewState};

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

do_connect_to_vnodes_cluster(VnodeClusterGatewayNode, #state{vnode_message_batchers = VnodeMessageBatchers}) ->
    pong = net_adm:ping(VnodeClusterGatewayNode),

    {ok, Ring} = rpc:call(VnodeClusterGatewayNode, riak_core_ring_manager, get_raw_ring, []),
    {_RingSize, Vnodes1} = element(4, Ring),
    Vnodes = lists:usort(fun({_, A}, {_, B}) -> A =< B end, Vnodes1),

    {ok, BatchSize} = application:get_env(riak_kv, transactions_batch_size),
    {ok, BatchTimeout} = application:get_env(riak_kv, transactions_batch_timeout),
    lists:foreach(fun(Vnode) ->
                          DispatchFun = fun(Batch) ->
                                                riak_kv_vnode:batch_commit_transactions(Vnode, Batch)
                                        end,
                          {ok, VnodeMessageBatcher} = message_batcher:start_link(DispatchFun, BatchSize, BatchTimeout), 
                          ets:insert(VnodeMessageBatchers, {Vnode, VnodeMessageBatcher})
                  end, Vnodes).

do_batch_commit(Vnode, Batch, #state{prepare_results = PrepareResults, vnode_message_batchers = VnodeMessageBatchers} = State) ->
    lager:info("Received a batch (~p) of transactions to commit from vnode ~p~n", [Batch, Vnode]),

    lists:foreach(fun({TransactionId, NNodes, Client, PrepareResult}) ->
                          do_commit(Vnode, TransactionId, NNodes, Client, PrepareResult, PrepareResults, VnodeMessageBatchers)
                  end, Batch),
    State.

do_commit(
  Vnode,
  TransactionId,
  NNodes,
  Client,
  PrepareResult,
  PrepareResults,
  VnodeMessageBatchers
) ->
    {TransactionId, ReceivedPrepareResults, TransactionPrepareResult, Vnodes} =
        case ets:lookup(PrepareResults, TransactionId) of
            [{TransactionId, _, _, _} = Result] -> Result;
            [] -> {TransactionId, 0, undefined, []}
        end,

    NewReceivedPrepareResults = ReceivedPrepareResults + 1,
    NewTransactionPrepareResult = receive_prepare_result(PrepareResult, TransactionPrepareResult),
    NewVnodes = [Vnode | Vnodes],

    if
        NewReceivedPrepareResults == NNodes ->
            lager:info("Committing transaction ~p~n", [TransactionId]),

            riak_core_vnode:reply(Client, {prepare_commit_result, NewTransactionPrepareResult}),

            lists:foreach(fun(Vnode1) ->
                                  VnodeMessageBatcher = ets:lookup_element(VnodeMessageBatchers, Vnode1, 2),
                                  message_batcher:add_to_batch(VnodeMessageBatcher, {TransactionId, NewTransactionPrepareResult})
                          end, NewVnodes),

            ets:delete(PrepareResults, TransactionId);
        true ->
            ets:insert(PrepareResults, {TransactionId, NewReceivedPrepareResults, NewTransactionPrepareResult, NewVnodes})
    end.

receive_prepare_result(PrepareResult, undefined) -> PrepareResult;
receive_prepare_result({aborted, Timestamp1}, {_, Timestamp2}) -> {aborted, max(Timestamp1, Timestamp2)};
receive_prepare_result({_, Timestamp1}, {aborted, Timestamp2}) -> {aborted, max(Timestamp1, Timestamp2)};
receive_prepare_result({prepared, Timestamp1}, {prepared, Timestamp2}) -> {prepared, max(Timestamp1, Timestamp2)}.
