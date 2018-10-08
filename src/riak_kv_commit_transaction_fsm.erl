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
         wait_for_response/2,
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
                prepare_commit_result,
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
                       prepare_commit_result = undefined,
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
    {next_state, prepare, NewStateData, 0}.

prepare(
  timeout,
  #state{id = Id,
         snapshot = Snapshot,
         nodes = Nodes,
         node_gets = NodeGets,
         node_puts = NodePuts} = StateData
) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    {_RingSize, IdxNodes} = element(4, Ring), 
    NodesLength = length(Nodes),
    ShuffledNodes = [N2 || {_, N2} <- lists:sort([{random:uniform(), N1} || N1 <- Nodes])],
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
                          riak_kv_vnode:prepare_transaction(Vnode, Id, Snapshot, Gets, Puts, NodesLength),
                          timer:sleep(random:uniform(5))
                  end, ShuffledNodes),

    TimerRef = schedule_timeout(?DEFAULT_TIMEOUT),
    NewStateData = StateData#state{timerref = TimerRef},
    {next_state, wait_for_response, NewStateData}.

wait_for_response({prepare_commit_result, PrepareCommitResult}, StateData) ->
    NewStateData = StateData#state{prepare_commit_result = PrepareCommitResult},
    {next_state, respond_to_client, NewStateData, 0};

wait_for_response(timeout, StateData) ->
    NewStateData = StateData#state{timeout = true},
    {next_state, respond_to_client, NewStateData, 0}.

respond_to_client(timeout, #state{client = Client,
                                  prepare_commit_result = {Result, Timestamp},
                                  timeout = Timeout} = StateData) ->
    Reply = if
                Timeout -> {error, timeout};
                true -> {ok, Result, Timestamp}
            end,
    erlang:send(Client, Reply),
    {stop, normal, StateData};

respond_to_client(_, StateData) ->
    {next_state, respond_to_client, StateData, 0}.

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
