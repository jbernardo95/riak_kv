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

-define(DEFAULT_TIMEOUT, 60000).

-record(state, {lsn, log, latest_object_versions}).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).


append_record(Record) ->
    gen_server:call({global, ?MODULE}, {append_record, Record}, ?DEFAULT_TIMEOUT).


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

    State = #state{lsn = 0, log = Log, latest_object_versions = dict:new()},
    {ok, State}.


handle_call(
  {append_record, #log_record{type = transaction_commit,
                              payload = Payload} = Record},
  _From,
  State
)->
    verify_if_log_is_full(),

    {Conflicts, NewState1} = check_conflicts(Payload, State),

    if
        not Conflicts ->
            NewState = append_record(Record, NewState1),
            Reply = {ok, NewState#state.lsn};
        true ->
            NewState = NewState1,
            Reply = {not_appended, NewState1#state.lsn}
    end,

    {reply, Reply, NewState};

handle_call({append_record, _Record}, _From, State)->
    {reply, error, State};

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

check_conflicts({_Id, Snapshot, Gets, PutsObjects},
                #state{lsn = Lsn, latest_object_versions = LatestObjectVersions} = State) ->
    {ConflictsGets, _} = check_conflicts(Gets, Snapshot, Lsn + 1, LatestObjectVersions),
    Puts = lists:map(fun riak_object:bkey/1, PutsObjects),
    {ConflictsPuts, NewLatestObjectVersions} = check_conflicts(Puts, Snapshot, Lsn + 1, LatestObjectVersions),

    Conflicts = ConflictsGets or ConflictsPuts,
    if
        Conflicts -> {Conflicts, State};
        true ->
            NewState = State#state{latest_object_versions = NewLatestObjectVersions},
            {Conflicts, NewState}
    end.

check_conflicts(Objects, Snapshot, Lsn, LatestObjectVersions) ->
    check_conflicts(Objects, Snapshot, Lsn, LatestObjectVersions, false).

check_conflicts(_Objects, _Snapshot, _Lsn, LatestObjectVersions, true) ->
    {true, LatestObjectVersions};
check_conflicts([], _Snapshot, _Lsn, LatestObjectVersions, Conflict) ->
    {Conflict, LatestObjectVersions};
check_conflicts([Bkey | Rest], Snapshot, Lsn, LatestObjectVersions, Conflict) ->
    LatestObjectVersion = case dict:find(Bkey, LatestObjectVersions) of
                              {ok, V} -> V;
                              error -> -1
                          end,

    NewConflict = Conflict or (LatestObjectVersion > Snapshot),
    NewLatestObjectVersions = dict:store(Bkey, Lsn, LatestObjectVersions),

    check_conflicts(Rest, Snapshot, Lsn, NewLatestObjectVersions, NewConflict).

append_record(Record, #state{lsn = Lsn} = State) ->
    disk_log:log(?LOG, Record),
    disk_log:sync(?LOG),

    riak_kv_transactions_committer:process_record(Lsn + 1, Record),

    lager:info("Record ~p was appended to the log~n", [Record]),

    State#state{lsn = Lsn + 1}.
