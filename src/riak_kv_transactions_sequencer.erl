-module(riak_kv_transactions_sequencer).

-behaviour(gen_server).

-export([start_link/0,
         connect_to_vnodes_cluster/1,
         validate_and_commit/6]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(RUNNING_TRANSACTIONS, running_transactions).
-define(LATEST_OBJECT_VERSIONS, latest_object_versions).
-define(RIAK_RING, riak_ring).

-record(state, {lsn}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

connect_to_vnodes_cluster(VnodeClusterGatewayNode) ->
    gen_server:cast({global, ?MODULE}, {connect_to_vnodes_cluster, VnodeClusterGatewayNode}).

validate_and_commit(TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    gen_server:cast({global, ?MODULE}, {validate_and_commit, TransactionId, Snapshot, Gets, Puts, NValidations, Client}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    ets:new(?RUNNING_TRANSACTIONS, [private, named_table]), 
    ets:new(?LATEST_OBJECT_VERSIONS, [private, named_table]), 
    ets:new(?RIAK_RING, [private, named_table, {keypos, 2}]), 

    State = #state{lsn = 1},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({connect_to_vnodes_cluster, VnodeClusterGatewayNode}, State) ->
    do_connect_to_vnodes_cluster(VnodeClusterGatewayNode, State);

handle_cast({validate_and_commit, TransactionId, Snapshot, Gets, Puts, NValidations, Client}, State) ->
    do_validate_and_commit(TransactionId, Snapshot, Gets, Puts, NValidations, Client, State);

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
    {ok, Ring} = rpc:call(VnodeClusterGatewayNode, riak_core_ring_manager, get_raw_ring, []),
    {_RingSize, Vnodes1} = element(4, Ring),
    Vnodes = lists:usort(fun({_, A}, {_, B}) -> A =< B end, Vnodes1),
    ets:insert(?RIAK_RING, Vnodes),
    {noreply, State}.

do_validate_and_commit(
  TransactionId, Snapshot1, Gets1, Puts1, NValidations, Client,
  #state{lsn = Lsn1} = State
) ->
    lager:info("Received transaction ~p for validation~n", [TransactionId]),

    % Update transaction metadata
    case ets:lookup(?RUNNING_TRANSACTIONS, TransactionId) of
        [{TransactionId, _Snapshot2, Gets2, Puts2, _Lsn2, ReceivedValidations1}] ->
            ets:insert(?RUNNING_TRANSACTIONS, {
                TransactionId,
                Snapshot1,
                Gets1 ++ Gets2,
                Puts1 ++ Puts2,
                Lsn1,
                ReceivedValidations1 + 1
            });
        [] ->
            ets:insert(?RUNNING_TRANSACTIONS, {TransactionId, Snapshot1, Gets1, Puts1, Lsn1, 1})
    end,

    [{TransactionId, Snapshot, Gets, Puts, Lsn, ReceivedValidations}] = ets:lookup(?RUNNING_TRANSACTIONS, TransactionId),
    case ReceivedValidations of
        NValidations ->
            lager:info("Validation in progress...~n", []),
            NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
            Conflicts = check_conflicts(Gets, NbkeyPuts, Snapshot),
            lager:info("Transaction ~p validated, conflicts: ~p~n", [TransactionId, Conflicts]),

            send_validation_result_to_client(TransactionId, Lsn, Client, Conflicts),
            send_validation_result_to_vnodes(TransactionId, Lsn, Puts, Conflicts),
            lager:info("Transaction ~p committed~n", [TransactionId]),

            if
                not Conflicts -> do_update_latest_object_versions(NbkeyPuts, Lsn);
                true -> ok
            end,

            ets:delete(?RUNNING_TRANSACTIONS, TransactionId);
        _ ->
            ok
    end,

    NewState = State#state{lsn = Lsn1 + 1},
    {noreply, NewState}.

check_conflicts(Gets, Puts, Snapshot) ->
    ConflictsGets = do_check_conflicts(Gets, Snapshot),
    ConflictsPuts = do_check_conflicts(Puts, Snapshot),
    ConflictsGets or ConflictsPuts.

do_check_conflicts(Objects, Snapshot) ->
    do_check_conflicts(Objects, Snapshot, false).

do_check_conflicts(_Objects, _Snapshot, true) -> true;
do_check_conflicts([], _Snapshot, Conflict) -> Conflict;
do_check_conflicts([Nbkey | Rest], Snapshot, Conflict) ->
    LatestObjectVersion = case ets:lookup(?LATEST_OBJECT_VERSIONS, Nbkey) of
                              [{Nbkey, Version}] -> Version;
                              [] -> -1
                          end,

    NewConflict = Conflict or (LatestObjectVersion > Snapshot),

    do_check_conflicts(Rest, Snapshot, NewConflict).

do_update_latest_object_versions(Nbkeys, Version) ->
    lists:foreach(
        fun(Nbkey) ->
            ets:insert(?LATEST_OBJECT_VERSIONS, {Nbkey, Version})
        end,
        Nbkeys
    ).

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
