-module(riak_kv_commit_transaction_fsm).

-behaviour(gen_fsm).

-export([start_link/5]).
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).
-export([group_objects_per_node/2,
         prepare/2,
         wait_for_prepare_results/2,
         commit/2,
         wait_for_commit_results/2,
         prepare_commit/2,
         wait_for_prepare_commit_result/2,
         respond_to_client/2]).

-record(state, {id,
                snapshot,
                gets,
                puts,
                client,
                nodes,
                node_gets, 
                node_puts, 
                timerref,
                received_prepare_results,
                prepare_result,
                received_commit_results,
                timeout}).

-define(DEFAULT_TIMEOUT, 60000).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(Id, Snapshot, Gets, Puts, Client) ->
    Args = [Id, Snapshot, Gets, Puts, Client],
    case sidejob_supervisor:start_child(riak_kv_commit_transaction_fsm_sj,
                                        gen_fsm, start_link,
                                        [?MODULE, Args, []]) of
        {error, overload} = Reply ->
            erlang:send(Client, Reply),
            Reply;
        {ok, Pid} ->
            {ok, Pid}
    end.

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([Id, Snapshot, Gets, Puts, Client]) ->
    StateData = #state{id = Id, 
                       snapshot = Snapshot,
                       gets = Gets,
                       puts = Puts,
                       client = Client,
                       nodes = undefined,
                       node_gets = undefined,
                       node_puts = undefined,
                       timerref = undefined,
                       received_prepare_results = 0,
                       prepare_result = undefined,
                       received_commit_results = 0,
                       timeout = false},
    {ok, group_objects_per_node, StateData, 0}.

group_objects_per_node(timeout, #state{gets = Gets, puts = Puts} = StateData) ->
    FoldFun = fun(Object, NodePuts1) ->
                      Node = riak_object:get_node(Object),
                      case dict:find(Node, NodePuts1) of
                          {ok, Puts1} -> dict:store(Node, [Object | Puts1], NodePuts1);
                          error -> dict:store(Node, [Object], NodePuts1)
                      end
              end,
    NodeGets = lists:foldl(FoldFun, dict:new(), Gets),
    NodePuts = lists:foldl(FoldFun, dict:new(), Puts),

    Nodes = lists:usort(dict:fetch_keys(NodeGets) ++ dict:fetch_keys(NodePuts)),

    NewStateData = StateData#state{nodes = Nodes,
                                   node_gets = NodeGets,
                                   node_puts = NodePuts},
    if
        length(Nodes) == 1 ->
            {next_state, prepare_commit, NewStateData, 0};
        true ->
            {next_state, prepare, NewStateData, 0}
    end.

prepare(
  timeout,
  #state{snapshot = Snapshot,
         nodes = Nodes,
         node_gets = NodeGets,
         node_puts = NodePuts} = StateData
) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    {_RingSize, IdxNodes} = element(4, Ring), 
    lists:foreach(fun(Node) ->
                          Vnode = lists:keyfind(Node, 2, IdxNodes),
                          Gets = case dict:find(Node, NodeGets) of
                                     {ok, Gets1} ->
                                         lists:map(fun riak_object:nbkey/1, Gets1);
                                     error -> []
                                 end,
                          Puts = case dict:find(Node, NodePuts) of
                                     {ok, Puts1} -> Puts1;
                                     error -> []
                                 end,
                          riak_kv_vnode:prepare_transaction(Vnode, Snapshot, Gets, Puts)
                  end, Nodes),

    TimerRef = schedule_timeout(?DEFAULT_TIMEOUT),
    NewStateData = StateData#state{timerref = TimerRef},
    {next_state, wait_for_prepare_results, NewStateData}.

wait_for_prepare_results(
  {prepare_result, PrepareResult},
  #state{nodes = Nodes} = StateData
) ->
    NewStateData = receive_prepare_result(PrepareResult, StateData),
    case NewStateData of
        #state{received_prepare_results = ReceivedPrepareResults} when ReceivedPrepareResults == length(Nodes) ->
            {next_state, commit, NewStateData, 0};
        _ ->
            {next_state, wait_for_prepare_results, NewStateData}
    end;

wait_for_prepare_results(timeout, StateData) ->
    NewStateData = StateData#state{timeout = true},
    {next_state, respond_to_client, NewStateData, 0}.

commit(
  timeout,
  #state{id = Id,
         nodes = Nodes,
         node_gets = NodeGets,
         node_puts = NodePuts,
         prepare_result = PrepareResult} = StateData
) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    {_RingSize, IdxNodes} = element(4, Ring), 
    lists:foreach(fun(Node) ->
                          Vnode = lists:keyfind(Node, 2, IdxNodes),
                          Gets = case dict:find(Node, NodeGets) of
                                     {ok, Gets1} ->
                                         lists:map(fun riak_object:nbkey/1, Gets1);
                                     error -> []
                                 end,
                          Puts = case dict:find(Node, NodePuts) of
                                     {ok, Puts1} -> Puts1;
                                     error -> []
                                 end,
                          NbkeyPuts = lists:map(fun riak_object:nbkey/1, Puts),
                          riak_kv_vnode:commit_transaction(Vnode, Id, Gets, NbkeyPuts, PrepareResult)
                  end, Nodes),

    {next_state, wait_for_commit_results, StateData}.

wait_for_commit_results(
  {commit_result, CommitResult},
  #state{nodes = Nodes} = StateData
) ->
    NewStateData = receive_commit_result(CommitResult, StateData),
    case NewStateData of
        #state{received_commit_results = ReceivedCommitResults} when ReceivedCommitResults == length(Nodes) ->
            {next_state, respond_to_client, NewStateData, 0};
        _ ->
            {next_state, wait_for_commit_results, NewStateData}
    end;

wait_for_commit_results(timeout, StateData) ->
    NewStateData = StateData#state{timeout = true},
    {next_state, respond_to_client, NewStateData, 0}.

prepare_commit(
  timeout,
  #state{snapshot = Snapshot,
         nodes = [Node],
         node_gets = NodeGets,
         node_puts = NodePuts} = StateData
) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    {_RingSize, IdxNodes} = element(4, Ring), 
    Vnode = lists:keyfind(Node, 2, IdxNodes),
    Gets = case dict:find(Node, NodeGets) of
               {ok, Gets1} ->
                   lists:map(fun riak_object:nbkey/1, Gets1);
               error -> []
           end,
    Puts = case dict:find(Node, NodePuts) of
               {ok, Puts1} -> Puts1;
               error -> []
           end,
    riak_kv_vnode:prepare_commit_transaction(Vnode, Snapshot, Gets, Puts),

    TimerRef = schedule_timeout(?DEFAULT_TIMEOUT),
    NewStateData = StateData#state{timerref = TimerRef},
    {next_state, wait_for_prepare_commit_result, NewStateData}.

wait_for_prepare_commit_result({prepare_commit_result, PrepareCommitResult}, StateData) ->
    NewStateData = StateData#state{prepare_result = PrepareCommitResult},
    {next_state, respond_to_client, NewStateData, 0};

wait_for_prepare_commit_result(timeout, StateData) ->
    NewStateData = StateData#state{timeout = true},
    {next_state, respond_to_client, NewStateData, 0}.

respond_to_client(timeout, #state{client = Client,
                                  prepare_result = {Result, Timestamp},
                                  timeout = Timeout} = StateData) ->
    Reply = if
                Timeout -> {error, timeout};
                true -> {ok, Result, Timestamp}
            end,
    erlang:send(Client, Reply),
    {stop, normal, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_info(timeout, StateName, StateData) ->
    ?MODULE:StateName(timeout, StateData);
handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

terminate(Reason, _StateName, _State) ->
    Reason.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), timeout).

receive_prepare_result(PrepareResult, #state{received_prepare_results = ReceivedPrepareResults} = StateData) ->
    do_receive_prepare_result(PrepareResult, StateData#state{received_prepare_results = ReceivedPrepareResults + 1}).

do_receive_prepare_result(PrepareResult, #state{prepare_result = undefined} = StateData) ->
    StateData#state{prepare_result = PrepareResult};
do_receive_prepare_result({aborted, Timestamp1}, #state{prepare_result = {_, Timestamp2}} = StateData) ->
    StateData#state{prepare_result = {aborted, max(Timestamp1, Timestamp2)}};
do_receive_prepare_result({_, Timestamp1}, #state{prepare_result = {aborted, Timestamp2}} = StateData) ->
    StateData#state{prepare_result = {aborted, max(Timestamp1, Timestamp2)}};
do_receive_prepare_result({prepared, Timestamp1}, #state{prepare_result = {prepared, Timestamp2}} = StateData) ->
    StateData#state{prepare_result = {prepared, max(Timestamp1, Timestamp2)}}.

receive_commit_result(ok = _CommitResult, #state{received_commit_results = ReceivedCommitResults} = StateData) ->
    StateData#state{received_commit_results = ReceivedCommitResults + 1}.
