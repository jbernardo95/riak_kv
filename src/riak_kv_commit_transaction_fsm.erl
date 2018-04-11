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

-record(state, {from, id, snapshot, gets, puts, preflist, timerref, commit_responses}).

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
                       commit_responses = 0},
    {ok, prepare, StateData, 0}.

prepare(timeout, #state{puts = Puts} = StateData) ->
    FoldFun = fun(Object, {Preflist1, Puts1}) ->
                      BKey = riak_object:bkey(Object),
                      DocIdx = riak_core_util:chash_key(BKey),
                      [{Index, _Pid} = Node] = riak_core_apl:get_apl(DocIdx, 1, riak_kv),
                      {dict:store(Node, ok, Preflist1), [{Object, Index} | Puts1]}
              end,
    {PreflistMap, Puts2} = lists:foldl(FoldFun, {dict:new(), []}, Puts),
    Preflist =  dict:fetch_keys(PreflistMap),

    NewStateData = StateData#state{puts = Puts2, preflist = Preflist},
    {next_state, execute, NewStateData, 0}.

execute(
  timeout,
  #state{id = Id,
         snapshot = Snapshot,
         gets = Gets,
         puts = Puts,
         preflist = Preflist} = StateData
) ->
    riak_kv_vnode:commit_transaction(Preflist, Id, Snapshot, Gets, Puts),

    TimerRef = schedule_timeout(?DEFAULT_TIMEOUT),
    NewStateData = StateData#state{timerref = TimerRef},
    {next_state, wait_for_vnode, NewStateData}.

wait_for_vnode(
  {ok, _Idx, _Id},
  #state{preflist = Preflist,
         commit_responses = CommitResponses} = StateData
) ->
    NewCommitResponses = CommitResponses + 1,
    NewStateData = StateData#state{commit_responses = NewCommitResponses},
    if
        NewCommitResponses == length(Preflist) ->
            %{next_state, wait_for_transactions_committer, NewStateData};
            {next_state, respond_to_client, NewStateData, 0};

        true ->
            {next_state, wait_for_vnode, NewStateData}
    end;

wait_for_vnode(timeout, StateData) ->
    % TODO
    {next_state, respond_to_client, StateData, 0}.

wait_for_transactions_committer(_Message, StateData) ->
    % TODO
    {next_state, respond_to_client, StateData, 0}.

respond_to_client(timeout, #state{from = {raw, ReqId, Pid}} = StateData) ->
    Reply = {ReqId, ok},
    Pid ! Reply,
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
