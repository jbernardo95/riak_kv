-module(riak_kv_log).

-behaviour(gen_server).

-export([start_link/0,
         append_record/2,
         heartbeat/2,
         new_log_record/3]).

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


append_record(Record, Partition) ->
    gen_server:call({global, ?MODULE}, {append_record, Record, Partition}).


heartbeat(Partition, Clock) ->
    gen_server:cast({global, ?MODULE}, {heartbeat, Partition, Clock}).


new_log_record(Timestamp, Type, Payload) ->
    #log_record{timestamp = Timestamp,
                type = Type,
                payload = Payload}.


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
            dict:store(Partition, 0, Dict)
        end,
        dict:new(),
        PrefLists
    ),

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
    lager:info("Disk log options ~p~n", [DiskLogOptions]),
    {ok, Log} = disk_log:open(DiskLogOptions),

    erlang:send(self(), append_stable_records_to_the_log),
    
    State = #state{heartbeats = Heartbeats,
                   tid = Tid,
                   log = Log},
    {ok, State}.


handle_call(
  {append_record, #log_record{timestamp = Timestamp} = Record, Partition},
  _From,
  #state{heartbeats = Heartbeats} = State
 )->
    %lager:info("Received record ~p to append from partition ~p~n", [Record, Partition]),

    % Insert record in ets table
    ets:insert(?PENDING_RECORDS_TABLE, {{Timestamp, Partition}, Record}),

    % Store heartbeat from partition
    Heartbeats1 = dict:store(Partition, Timestamp, Heartbeats),

    State1 = State#state{heartbeats = Heartbeats1},
    {reply, ok, State1};

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at handle_call: ~p~n", [Request]),
    {reply, error, State}.


handle_cast(
  {heartbeat, Partition, Clock},
  #state{heartbeats = Heartbeats} = State
 )->
    % Store hearbeat from partition
    Heartbeats1 = dict:store(Partition, Clock, Heartbeats),

    State1 = State#state{heartbeats = Heartbeats1},
    {noreply, State1};


handle_cast(Request, State) ->
    lager:error("Unexpected request received at handle_cast: ~p~n", [Request]),
    {noreply, State}.


handle_info(append_stable_records_to_the_log, State) ->
    append_stable_records_to_the_log(State),
    erlang:send_after(1000, self(), append_stable_records_to_the_log),
    {noreply, State};

handle_info(Info, State) ->
    lager:error("Unexpected info received at handle_info: ~p~n", [Info]),
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

verify_if_log_is_full(Log) ->
    Info = disk_log:info(Log),
    {SinceLogWasOpened, _} = proplists:get_value(no_overflows, Info, {0, 0}),
    if
        SinceLogWasOpened > 0 ->
            lager:critical("Log is full~n", []),
            exit(log_is_full);
        true ->
            ok
    end.

append_stable_records_to_the_log(#state{log = Log, heartbeats = Heartbeats} = State) ->
    verify_if_log_is_full(Log),
    StableTimestamp = get_stable_timestamp(Heartbeats),
    append_stable_records_to_the_log(StableTimestamp, State).

append_stable_records_to_the_log(StableTimestamp, #state{log = Log} = State) ->
    case ets:first(?PENDING_RECORDS_TABLE) of
        {Timestamp, _Partition} = Key when Timestamp =< StableTimestamp ->
            [{_, Record}] = ets:lookup(?PENDING_RECORDS_TABLE, Key),
            disk_log:log(Log, Record),
            ets:delete(?PENDING_RECORDS_TABLE, Key),
            append_stable_records_to_the_log(StableTimestamp, State);

        _ ->
            lager:info("Stable records appended to the log (~p)~n", [StableTimestamp]),
            disk_log:sync(Log),
            ok
    end.
