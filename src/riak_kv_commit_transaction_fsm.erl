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
         respond_to_client/2]).

-record(state, {from,
                id,
                snapshot,
                gets,
                puts,
                record,
                reply}).

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
                       record = undefined,
                       reply = undefined},
    {ok, prepare, StateData, 0}.

prepare(timeout, #state{id = Id, snapshot = Snapshot, gets = Gets, puts = Puts} = StateData) ->
    Record = riak_kv_log:new_log_record(transaction_commit, {Id, Snapshot, Gets, Puts}),
    NewStateData = StateData#state{record = Record},
    {next_state, execute, NewStateData, 0}.

execute(timeout, #state{record = Record} = StateData) ->
    Reply = riak_kv_log:append_record(Record),
    NewStateData = StateData#state{reply = Reply},
    {next_state, respond_to_client, NewStateData, 0}.

respond_to_client(timeout, #state{from = {raw, ReqId, Pid},
                                  reply = Reply} = StateData) ->
    FsmReply = {ReqId, Reply},
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
