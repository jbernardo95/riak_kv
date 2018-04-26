-module(riak_kv_log).

-behaviour(gen_server).

-export([start_link/0,
         append_record/1,
         new_log_record/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("riak_kv_log.hrl").

-record(state, {lsn,
                log,
                log_cache_table_id}).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).


append_record(Record) ->
    gen_server:call({global, ?MODULE}, {append_record, Record}).


new_log_record(Type, Payload) ->
    #log_record{type = Type,
                payload = Payload}.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    % Open log
    {ok, LogOptions} = application:get_env(riak_kv, log),
    LogOptionsDict = dict:from_list(LogOptions),
    LogFile = dict:fetch(data_path, LogOptionsDict) ++ "/data",
    ok = filelib:ensure_dir(LogFile),
    DiskLogOptions = [
        {name, ?LOG},
        {file, LogFile},
        {repair, true},
        {type, wrap},
        {size, {dict:fetch(max_n_bytes, LogOptionsDict), dict:fetch(max_n_files, LogOptionsDict)}},
        {format, internal},
        {mode, read_write}
    ],
    lager:info("Disk log options ~p~n", [DiskLogOptions]),
    {ok, Log} = disk_log:open(DiskLogOptions),

    % Create ets table to serve as a cache of the log
    LogCacheEtsTableOptions = [set, named_table, public, {write_concurrency, true}],
    LogCacheTid = ets:new(?LOG_CACHE, LogCacheEtsTableOptions),

    ets:insert(?LOG_CACHE, {current_lsn, 0}),

    State = #state{lsn = 0,
                   log = Log,
                   log_cache_table_id = LogCacheTid},
    {ok, State}.


handle_call({append_record, Record}, _From, #state{lsn = Lsn} = State)->
    verify_if_log_is_full(),

    disk_log:log(?LOG, Record),
    disk_log:sync(?LOG),

    ets:insert(?LOG_CACHE, {Lsn + 1, Record}),
    ets:insert(?LOG_CACHE, {current_lsn, Lsn + 1}),

    %lager:info("Record ~p was appended to the log~n", [Record]),

    NewState = State#state{lsn = Lsn + 1},
    {reply, ok, NewState};

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at handle_call: ~p~n", [Request]),
    {reply, error, State}.


handle_cast(Request, State) ->
    lager:error("Unexpected request received at handle_cast: ~p~n", [Request]),
    {noreply, State}.


handle_info(Info, State) ->
    lager:error("Unexpected info received at handle_info: ~p~n", [Info]),
    {noreply, State}.


terminate(_Reason, _State) ->
    disk_log:close(?LOG),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

verify_if_log_is_full() ->
    Info = disk_log:info(?LOG),
    {SinceLogWasOpened, _} = proplists:get_value(no_overflows, Info, {0, 0}),
    if
        SinceLogWasOpened > 0 ->
            lager:critical("Log is full~n", []),
            exit(log_is_full);
        true ->
            ok
    end.
