-module(riak_kv_commit_transaction_fsm).

-behaviour(gen_fsm).

-export([start/5, start_link/5]).
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).
-export([prepare/2,
         execute/2,
         wait_for_vnode/2,
         wait_for_transactions_committer/2,
         respond_to_client/2]).

-record(state, {from,
                id,
                snapshot,
                gets,
                puts,
                preflist_puts, 
                timerref,
                vnode_responses,
                status,
                lsn,
                timeout}).

-define(DEFAULT_TIMEOUT, 60000).

%% ===================================================================
%% Public API
%% ===================================================================

start(From, Id, Snapshot, Gets, Puts) ->
    Args = [From, Id, Snapshot, Gets, Puts],
    case sidejob_supervisor:start_child(riak_kv_commit_transaction_fsm_sj,
                                        gen_fsm, start_link,
                                        [?MODULE, Args, []]) of
        {error, overload} ->
            riak_kv_util:overload_reply(From),
            {error, overload};
        {ok, Pid} ->
            {ok, Pid}
    end.

start_link(From, Id, Snapshot, Gets, Puts) -> start(From, Id, Snapshot, Gets, Puts).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([From, Id, Snapshot, Gets, Puts]) ->
    StateData = #state{from = From,
                       id = Id, 
                       snapshot = Snapshot,
                       gets = Gets,
                       puts = Puts,
                       preflist_puts = undefined,
                       timerref = undefined,
                       vnode_responses = 0,
                       status = undefined,
                       lsn = undefined,
                       timeout = false},
    {ok, prepare, StateData, 0}.

prepare(timeout, #state{puts = Puts} = StateData) ->
    FoldFun = fun(Object, PreflistPuts1) ->
                      Bkey = riak_object:bkey(Object),
                      DocIdx = riak_core_util:chash_key(Bkey),
                      [Vnode] = riak_core_apl:get_apl(DocIdx, 1, riak_kv),

                      case dict:find(Vnode, PreflistPuts1) of
                          {ok, Puts1} -> dict:store(Vnode, [Object | Puts1], PreflistPuts1);
                          error -> dict:store(Vnode, [Object], PreflistPuts1)
                      end
              end,
    PreflistPuts = lists:foldl(FoldFun, dict:new(), Puts),

    NewStateData = StateData#state{preflist_puts = PreflistPuts},
    {next_state, execute, NewStateData, 0}.

execute(
  timeout,
  #state{id = Id,
         snapshot = Snapshot,
         gets = Gets,
         preflist_puts = PreflistPuts} = StateData
) ->
    Vnodes = dict:fetch_keys(PreflistPuts),
    NVnodes = length(Vnodes),
    lists:foreach(fun(Vnode) ->
                          Puts = dict:fetch(Vnode, PreflistPuts),
                          riak_kv_vnode:commit_transaction([Vnode], Id, Snapshot, Gets, Puts, NVnodes)
                  end, Vnodes),

    TimerRef = schedule_timeout(?DEFAULT_TIMEOUT),
    NewStateData = StateData#state{timerref = TimerRef},
    {next_state, wait_for_vnode, NewStateData}.

wait_for_vnode(
  {ok, _Idx, _Id},
  #state{preflist_puts = PreflistPuts,
         vnode_responses = VnodeResponses} = StateData
) ->
    NewVnodeResponses = VnodeResponses + 1,
    NewStateData = StateData#state{vnode_responses = NewVnodeResponses},
    NVnodes = dict:size(PreflistPuts),
    if
        NewVnodeResponses == NVnodes ->
            {next_state, wait_for_transactions_committer, NewStateData};
        true ->
            {next_state, wait_for_vnode, NewStateData}
    end;

wait_for_vnode(timeout, StateData) ->
    NewStateData = StateData#state{timeout = true},
    {next_state, respond_to_client, NewStateData, 0}.

wait_for_transactions_committer(
  {transaction_commit_status, Id, Status, Lsn},
  #state{id = Id} = StateData
) ->
    NewStateData = StateData#state{status = Status,
                                   lsn = Lsn},
    {next_state, respond_to_client, NewStateData, 0};

wait_for_transactions_committer(timeout, StateData) ->
    NewStateData = StateData#state{timeout = true},
    {next_state, respond_to_client, NewStateData, 0}.

respond_to_client(timeout,
                  #state{from = {raw, ReqId, Pid},
                         status = Status,
                         lsn = Lsn,
                         timeout = Timeout} = StateData) ->
    ClientReply = if
                      Timeout -> Timeout;
                      true -> {Status, Lsn}
                  end,
    FsmReply = {ReqId, ClientReply},
    erlang:send(Pid, FsmReply),
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
