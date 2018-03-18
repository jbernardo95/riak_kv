-module(riak_kv_log).

-behaviour(gen_server).

-export([start_link/0,
         append_records/2,
         heartbeat/2,
         new_log_record/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("riak_kv_log.hrl").

-define(PENDING_RECORDS_TABLE, pending_records).

-record(state, {heartbeats, tid, log}).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).


append_records(Records, Partition)->
    gen_server:call({global, ?MODULE}, {append_records, Records, Partition}).


heartbeat(Partition, Clock)->
    gen_server:cast({global, ?MODULE}, {heartbeat, Partition, Clock}).


new_log_record(ReqId, Timestamp) ->
    #log_record{req_id = ReqId, timestamp = Timestamp}.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    % Initialize heartbeats dictionary
    Heartbeats = dict:new(),

    % Create ets table to store unstable records 
    EtsTableOptions = [ordered_set, named_table, private],
    Tid = ets:new(?PENDING_RECORDS_TABLE, EtsTableOptions),

    % Open log
    {ok, LogOptions} = application:get_env(riak_kv, log),
    LogOptionsDict = dict:from_list(LogOptions),
    LogFile = dict:fetch(data_path, LogOptionsDict) ++ "/data",
    ok = filelib:ensure_dir(LogFile),
    DiskLogOptions = [
        {name, riak_kv_log},
        {file, LogFile},
        {repair, true},
        {type, wrap},
        {size, {dict:fetch(max_n_bytes, LogOptionsDict), dict:fetch(max_n_files, LogOptionsDict)}},
        {format, internal},
        {mode, read_write}
    ],
    lager:info("Disk log options ~p ~n", [DiskLogOptions]),
    {ok, Log} = disk_log:open(DiskLogOptions),

    erlang:send(self(), append_stable_records_to_the_log),
    
    State = #state{heartbeats = Heartbeats,
                   tid = Tid,
                   log = Log},
    {ok, State}.


handle_call(
  {append_records, Records, Partition},
  _From,
  #state{heartbeats = Heartbeats} = State
 )->
    %lager:info("Received ~p records to append from partition ~p ~n", [length(Records), Partition]),
    
    % Insert records in ets table 
    F = fun(#log_record{timestamp = Timestamp} = Record) -> 
                ets:insert(?PENDING_RECORDS_TABLE, {{Timestamp, Partition}, Record})
        end,
    lists:foreach(F, Records),
    
    % Store heartbeat from partition
    #log_record{timestamp = LastTimestamp} = lists:last(Records),
    Heartbeats1 = dict:store(Partition, LastTimestamp, Heartbeats),

    State1 = State#state{heartbeats = Heartbeats1},
    {reply, ok, State1};

handle_call(_Request, _From, State) ->
    lager:error("Unexpected message received at hanlde_call~n"),
    {reply, ok, State}.


handle_cast(
  {heartbeat, _Partition, _Clock},
  State
 )->
    {noreply, State};


handle_cast(_Request, State) ->
    lager:error("Unexpected message received at hanlde_cast~n"),
    {noreply, State}.


handle_info(append_stable_records_to_the_log, State) ->
    append_stable_records_to_the_log(State),
    erlang:send_after(1000, self(), append_stable_records_to_the_log),
    {noreply, State};

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
    do_get_stable_timestamp(HeartbeatsList).

do_get_stable_timestamp([]) ->
    0;
do_get_stable_timestamp(HeartbeatsList) ->
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
    case ets:first(?PENDING_RECORDS_TABLE) of
        {Timestamp, _Partition} = Key when Timestamp =< StableTimestamp ->
            Record = ets:lookup(?PENDING_RECORDS_TABLE, Key),
            disk_log:log(Log, Record),
            ets:delete(?PENDING_RECORDS_TABLE, Key),
            append_stable_records_to_the_log(StableTimestamp, State);

        _ ->
            lager:info("Stable records appended to the log (~p)~n", [StableTimestamp]),
            disk_log:sync(Log),
            ok
    end.
