-module(riak_kv_transactional_get_fsm).

-behaviour(gen_fsm).

-export([start_link/6]).
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).
-export([execute/2,
         wait_for_vnode/2,
         respond_to_client/2]).

-record(state, {node, bucket, key, snapshot, read_only, client, timerref, retval, timeout}).

-define(DEFAULT_TIMEOUT, 60000).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(Node, Bucket, Key, Snapshot, ReadOnly, Client) ->
    Args = [Node, Bucket, Key, Snapshot, ReadOnly, Client],
    case sidejob_supervisor:start_child(riak_kv_transactional_get_fsm_sj,
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

init([Node, Bucket, Key, Snapshot, ReadOnly, Client]) ->
    StateData = #state{node = Node,
                       bucket = Bucket,
                       key = Key,
                       snapshot = Snapshot, 
                       read_only = ReadOnly,
                       client = Client,
                       timerref = undefined,
                       retval = undefined,
                       timeout = false},
    {ok, execute, StateData, 0}.

execute(
  timeout,
  #state{node = Node,
         bucket = Bucket,
         key = Key,
         snapshot = Snapshot,
         read_only = ReadOnly} = StateData
) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    {_RingSize, IdxNodes} = element(4, Ring), 
    Vnode = lists:keyfind(Node, 2, IdxNodes),

    riak_kv_vnode:transactional_get(Vnode, {Bucket, Key}, Snapshot, ReadOnly),

    TimerRef = schedule_timeout(?DEFAULT_TIMEOUT),
    NewStateData = StateData#state{timerref = TimerRef},
    {next_state, wait_for_vnode, NewStateData}.

wait_for_vnode(timeout, StateData) ->
    NewStateData = StateData#state{timeout = true},
    {next_state, respond_to_client, NewStateData, 0};

wait_for_vnode(Retval, StateData) ->
    NewStateData = StateData#state{retval = Retval},
    {next_state, respond_to_client, NewStateData, 0}.

respond_to_client(timeout, #state{client = Client, retval = Retval, timeout = Timeout} = StateData) ->
    Reply = if
                Timeout -> {error, timeout};
                true -> Retval
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
