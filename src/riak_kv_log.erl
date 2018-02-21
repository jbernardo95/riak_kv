-module(riak_kv_log).

-behaviour(gen_server).

-export([start_link/0,
         append_record/2,
         heartbeat/2,
         new_log_record/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("riak_kv_log.hrl").

-define(SERVER, ?MODULE).
-define(Pending_Records_Table_Name, labels).

-record(state, {heartbeats, tid, log}).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

append_record(Record, Partition)->
    gen_server:cast({global, ?MODULE}, {append_record, Record, Partition}).

heartbeat(Partition, Clock)->
    gen_server:cast({global, ?MODULE}, {heartbeat, Partition, Clock}).

new_log_record(ReqId, Timestamp) ->
    #log_record{req_id = ReqId, timestamp = Timestamp}.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    % Initialize heartbeats dictionary
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    PrefLists = riak_core_ring:all_preflists(Ring, 1),
    Heartbeats = lists:foldl(
        fun(PrefList, Dict) ->
            {Partition, _Node} = hd(PrefList),
            %riak_kv_vnode:heartbeat(PrefList),
            dict:store(Partition, 0, Dict)
        end,
        dict:new(),
        PrefLists
    ),

    % Create ets table to store unstable records 
    EtsTableOptions = [ordered_set, named_table, private],
    Tid = ets:new(?Pending_Records_Table_Name, EtsTableOptions),

    % Open log
    {ok, LogOptions} = application:get_env(riak_kv, log),
    LogOptionsDict = dict:from_list(LogOptions),
    DiskLogOptions = [
        {name, riak_kv_log},
        {file, dict:fetch(data_path, LogOptionsDict) ++ "/data"},
        {repair, true},
        {type, wrap},
        {size, {dict:fetch(max_n_bytes, LogOptionsDict), dict:fetch(max_n_files, LogOptionsDict)}},
        {format, internal},
        {mode, read_write}
    ],
    lager:error("Disk log options ~p ~n", [DiskLogOptions]),
    {ok, Log} = disk_log:open(DiskLogOptions),
    
    State = #state{heartbeats = Heartbeats,
                   tid = Tid,
                   log = Log},
    {ok, State}.


handle_call(_Request, _From, State) ->
    lager:error("Unexpected message received at hanlde_call~n"),
    {reply, ok, State}.


handle_cast(
  {append_record, #log_record{timestamp = Timestamp} = Record, Partition},
  #state{heartbeats = Heartbeats} = State
 )->
    lager:info("Received record ~p to append from partition ~p ~n", [Record, Partition]),

    % Insert record in ets table
    ets:insert(?Pending_Records_Table_Name, {{Timestamp, Partition}, Record}),

    % Store heartbeat from partition
    Heartbeats1 = dict:store(Partition, Timestamp, Heartbeats),

    % Insert stable records into the log
    append_stable_records_to_the_log(State),

    State1 = State#state{heartbeats = Heartbeats1},
    {noreply, State1};


handle_cast(
  {heartbeat, Partition, Clock},
  #state{heartbeats = Heartbeats} = State
 )->
    % Store hearbeat from partition
    Heartbeats1 = dict:store(Partition, Clock, Heartbeats),

    % Insert stable records into the log
    append_stable_records_to_the_log(State),

    State1 = State#state{heartbeats = Heartbeats1},
    {noreply, State1};


handle_cast(_Request, State) ->
    lager:error("Unexpected message received at hanlde_cast~n"),
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, #state{log = Log}) ->
    disk_log:close(Log),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

get_stable_timestamp(Heartbeats) ->
    HeartbeatsList = dict:to_list(Heartbeats),
    {_Partition, Clock} = hd(HeartbeatsList),
    lists:foldl(
        fun({_Partition1, Clock1}, Min) ->
            if
                Clock1 < Min -> Clock1;
                true -> Min
            end
        end,
        Clock,
        HeartbeatsList 
     ).

append_stable_records_to_the_log(#state{heartbeats = Heartbeats} = State) ->
    StableTimestamp = get_stable_timestamp(Heartbeats),
    append_stable_records_to_the_log(StableTimestamp, State).

append_stable_records_to_the_log(StableTimestamp, #state{log = Log} = State) ->
    case ets:first(?Pending_Records_Table_Name) of
        {Timestamp, _Partition} = Key when Timestamp =< StableTimestamp ->
            disk_log:log(Log, Key),
            ets:delete(?Pending_Records_Table_Name, Key),
            append_stable_records_to_the_log(StableTimestamp, State);

        _ ->
            %lager:info("Stable records appended to the log~n", []),
            disk_log:sync(Log),
            ok
    end.
