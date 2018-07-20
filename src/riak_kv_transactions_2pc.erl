-module(riak_kv_transactions_2pc).

-behaviour(gen_server).

-export([start_link/0,
         prepare/3,
         prepare/4,
         commit/4,
         move_clock_forward/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {object_versions_table, locks_table, clock}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

prepare(ServerRef, Snapshot, Objects) ->
    gen_server:call(ServerRef, {prepare, Snapshot, Objects, false}).

prepare(ServerRef, Snapshot, Objects, BlindWrite) ->
    gen_server:call(ServerRef, {prepare, Snapshot, Objects, BlindWrite}).

commit(ServerRef, Gets, Puts, PrepareResult) ->
    gen_server:call(ServerRef, {commit, Gets, Puts, PrepareResult}).

move_clock_forward(ServerRef, Snapshot) ->
    gen_server:cast(ServerRef, {move_clock_forward, Snapshot}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    ObjectVersionsTable = ets:new(object_versions, [private]), 
    LocksTable = ets:new(locks_table, [private]), 

    State = #state{object_versions_table = ObjectVersionsTable,
                   locks_table = LocksTable,
                   clock = 1},
    {ok, State}.

handle_call({prepare, Snapshot, Objects, BlindWrite}, _From, State) ->
    do_prepare(Snapshot, Objects, BlindWrite, State);

handle_call({commit, Gets, Puts, PrepareResult}, _From, State) ->
    do_commit(Gets, Puts, PrepareResult, State);

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({move_clock_forward, Snapshot}, State) ->
    NewState = do_move_clock_forward(Snapshot, State),
    {noreply, NewState};

handle_cast(Request, State) ->
    lager:error("Unexpected request received at hanlde_cast: ~p~n", [Request]),
    {noreply, State}.

handle_info(Info, State) ->
    lager:error("Unexpected info received at handle_info: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_prepare(
  Snapshot,
  Objects,
  BlindWrite,
  #state{object_versions_table = ObjectVersionsTable,
         locks_table = LocksTable,
         clock = Clock} = State
) ->
    Timestamp = if
                    Clock > Snapshot -> Clock;
                    true -> Snapshot + 1
                end,
    NewState = do_move_clock_forward(Timestamp, State),

    %lager:info("Trying to acquire locks...~n", []),
    LockConflicts = check_lock_conflicts(Objects, LocksTable),
    if
        LockConflicts ->
            %lager:info("Locks not acquired, aborting~n", []),
            {reply, {aborted, Timestamp}, NewState};
        true ->
            %lager:info("Locks acquired, checking version conflicts...~n", []),
            VersionConflicts = if
                                   BlindWrite -> false;
                                   true -> check_version_conflicts(Snapshot, Objects, ObjectVersionsTable)
                               end,
            %lager:info("Conflicts: ~p~n", [VersionConflicts]),
            if
                VersionConflicts ->
                    {reply, {aborted, Timestamp}, NewState};
                true ->
                    ets:insert(LocksTable, lists:map(fun(Nbkey) -> {Nbkey, true} end, Objects)),
                    {reply, {prepared, Timestamp}, NewState}
            end
    end.

do_commit(
  Gets,
  Puts,
  PrepareResult,
  #state{object_versions_table = ObjectVersionsTable,
         locks_table = LocksTable} = State
) ->
    NewState = case PrepareResult of
                   {prepared, Timestamp} ->
                       lists:foreach(fun(Nbkey) -> ets:insert(ObjectVersionsTable, {Nbkey, Timestamp}) end, Puts),
                       NewState1 = do_move_clock_forward(Timestamp, State),
                       %lager:info("Transaction committed~n", []),
                       NewState1;
                   _ ->
                       %lager:info("Transaction aborted~n", []),
                       State
               end,

    lists:foreach(fun(Nbkey) -> ets:delete(LocksTable, Nbkey) end, Gets ++ Puts),

    {reply, ok, NewState}.

do_move_clock_forward(Timestamp, #state{clock = Clock} = State) ->
    if 
        Clock > Timestamp -> State;
        true -> State#state{clock = Timestamp + 1}
    end.

check_lock_conflicts(Objects, LocksTable) ->
    check_lock_conflicts(Objects, LocksTable, false).

check_lock_conflicts(_Objects, _LocksTable, true) -> true;
check_lock_conflicts([], _LocksTable, Conflict) -> Conflict;
check_lock_conflicts([Nbkey | Rest], LocksTable, _Conflict) ->
    Conflict = case ets:lookup(LocksTable, Nbkey) of
                   [{Nbkey, Locked}] -> Locked;
                   [] -> false
               end,
    check_lock_conflicts(Rest, LocksTable, Conflict).

check_version_conflicts(Snapshot, Objects, ObjectVersionsTable) ->
    check_version_conflicts(Snapshot, Objects, ObjectVersionsTable, false).

check_version_conflicts(_Snapshot, _Objects, _ObjectVersionsTable, true) -> true;
check_version_conflicts(_Snapshot, [], _ObjectVersionsTable, Conflict) -> Conflict;
check_version_conflicts(Snapshot, [Nbkey | Rest], ObjectVersionsTable, _Conflict) ->
    Conflict = case ets:lookup(ObjectVersionsTable, Nbkey) of
                    [{Nbkey, Version}] -> Version > Snapshot;
                    [] -> false
                end,
    check_version_conflicts(Snapshot, Rest, ObjectVersionsTable, Conflict).
