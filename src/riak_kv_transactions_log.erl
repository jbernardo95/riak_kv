-module(riak_kv_transactions_log).

-behaviour(gen_server).

-export([start_link/1,
         append/2,
         new_log_record/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(log_record, {lsn, content}). 

-record(state, {n, log}).


%%%===================================================================
%%% API
%%%===================================================================

start_link(N) ->
    gen_server:start_link({global, {?MODULE, N}}, ?MODULE, N, []).

append(N, #log_record{} = Record) ->
    gen_server:cast({global, {?MODULE, N}}, {append, Record}).

new_log_record(Lsn, Content) ->
    #log_record{lsn = Lsn, content = Content}.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(N) ->
    {ok, LogOptions} = application:get_env(riak_kv, transactions_log),
    LogOptionsDict = dict:from_list(LogOptions),
    LogName = "riak_kv_transactions_log_" ++ integer_to_list(N),
    LogFile = dict:fetch(data_path, LogOptionsDict) ++ "/" ++ LogName ++ "_data",
    ok = filelib:ensure_dir(LogFile),
    DiskLogOptions = [
        {name, LogName},
        {file, LogFile},
        {repair, true},
        {type, wrap},
        {size, {dict:fetch(max_n_bytes, LogOptionsDict), dict:fetch(max_n_files, LogOptionsDict)}},
        {format, internal},
        {mode, read_write}
    ],
    {ok, Log} = disk_log:open(DiskLogOptions),

    State = #state{n = N, log = Log},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at handle_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({append, Record}, State) ->
    do_append(Record, State);

handle_cast(Request, State) ->
    lager:error("Unexpected request received at handle_cast: ~p~n", [Request]),
    {noreply, State}.

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

do_append(#log_record{lsn = Lsn, content = Transaction} = Record, #state{n = N, log = Log} = State) ->
    verify_if_log_is_full(Log),

    disk_log:log(Log, Record),
    %disk_log:sync(Log),

    %lager:info("Record ~p was appended to the log~n", [Record]),

    {Id, _Snapshot, _Gets, Puts, NValidations, Client, Conflicts} = Transaction,
    BkeyPuts = lists:map(fun riak_object:bkey/1, Puts),
    riak_kv_transactions_committer:commit(N, Id, BkeyPuts, NValidations, Client, Conflicts, Lsn),

    {noreply, State}.

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
