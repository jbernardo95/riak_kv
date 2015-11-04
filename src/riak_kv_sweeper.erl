%% -------------------------------------------------------------------
%%
%% riak_kv_sweeper: Riak sweep scheduler
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc
%% This module implements a gen_server process that manages and schedules sweeps.
%% Anyone can register a #sweep_participant{} with information about how
%% often it should run and what kind of fun it include
%%  (?DELETE_FUN, ?MODIFY_FUN or ?OBSERV_FUN)
%%
%% riak_kv_sweeper keep one sweep per index.
%% Once every tick  riak_kv_sweeper check if it's in the configured sweep_window
%% and find sweeps to run. It does this by comparing what the sweeps have swept
%% before with the requirments in #sweep_participant{}.
-module(riak_kv_sweeper).
-behaviour(gen_server).
-include("riak_kv_sweeper.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Default number of concurrent sweeps that are allowed to run.
-define(DEFAULS_SWEEP_CONCURRENCY,2).
-define(DEFAULT_SWEEP_TICK, 60 * 1000). %% 1/min
-define(ESTIMATE_EXPIRY, 24 * 3600).    %% 1 day in s

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
         add_sweep_participant/1,
         remove_sweep_participant/1,
         status/0,
         sweep/1,
         report_worker_pid/2,
         sweep_result/2,
         update_progress/2,
         stop_all_sweeps/0,
         disable_sweep_scheduling/0,
         enable_sweep_scheduling/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Add callback module that will be asked to participate in sweeps.
-spec add_sweep_participant(#sweep_participant{}) -> ok.
add_sweep_participant(Participant) ->
    gen_server:call(?MODULE, {add_sweep_participant, Participant}).

%% @doc Remove participant callback module.
-spec remove_sweep_participant(atom()) -> true | false.
remove_sweep_participant(Module) ->
    gen_server:call(?MODULE, {remove_sweep_participant, Module}).

%% @doc Initiat a sweep without using scheduling. Can be used as fold replacment.
-spec sweep(non_neg_integer()) -> ok.
sweep(Index) ->
    gen_server:call(?MODULE, {sweep_request, Index}).

%% @doc Get information about participants and all sweeps.
-spec status() -> {[#sweep_participant{}], [#sweep{}]}.
status() ->
    gen_server:call(?MODULE, status).

%% @doc Stop all running sweeps
-spec stop_all_sweeps() ->  ok.
stop_all_sweeps() ->
    gen_server:call(?MODULE, stop_all_sweeps).

%% Stop scheduled sweeps and disable the scheduler from starting new sweeps
%% Only allow manual sweeps throu sweep/1.
-spec disable_sweep_scheduling() ->  ok.
disable_sweep_scheduling() ->
    lager:info("Disable sweep scheduling"),
    application:set_env(riak_kv, sweeper_scheduler, false),
    stop_all_sweeps().
enable_sweep_scheduling() ->
    lager:info("Enable sweep scheduling"),
    application:set_env(riak_kv, sweeper_scheduler, true).

%% @private used by the sweeping worker to report pid to enable requests
%% from riak_kv_sweeper.
report_worker_pid(Index, Pid) ->
    gen_server:cast(?MODULE, {worker_pid, Index, Pid}).

%% @private used by the sweeping process to report results when done.
sweep_result(Index, Result) ->
    gen_server:cast(?MODULE, {sweep_result, Index, Result}).

% @private used by the sweeping process to report progress.
update_progress(Index, SweptKeys) ->
    gen_server:cast(?MODULE, {update_progress, Index, SweptKeys}).

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {sweep_participants = dict:new() :: dict:dict(),
                sweeps             = dict:new() :: dict:dict()
               }).

init([]) ->
    process_flag(trap_exit, true),
    schedule_sweep_tick(),
    case get_persistent_participants() of
        false ->
            {ok, #state{}};
        SP ->
            {ok, #state{sweep_participants = SP}}
    end.

handle_call({add_sweep_participant, Participant}, _From, #state{sweep_participants = SP} = State) ->
    SP1 = dict:store(Participant#sweep_participant.module, Participant, SP),
    persist_participants(SP1),
    {reply, ok, State#state{sweep_participants = SP1}};

handle_call({remove_sweep_participant, Module}, _From, #state{sweeps = Sweeps,
                                                              sweep_participants = SP} = State) ->
    Reply = dict:is_key(Module, SP),
    SP1 = dict:erase(Module, SP),
    persist_participants(SP1),
    disable_sweep_participant_in_running_sweep(Module, Sweeps),
    {reply, Reply, State#state{sweep_participants = SP1}};

handle_call({sweep_request, Index}, _From, State) ->
    State1 = sweep_request(Index, State),
    {reply, ok, State1};

handle_call(status, _From, State) ->
    ParticipantsString = format_participants(dict:to_list(State#state.sweep_participants)),
    SweepString = format_sweeps(dict:to_list(State#state.sweeps)),
    {reply, lists:flatten(ParticipantsString ++ SweepString), State};

handle_call(sweeps, _From, State) ->
    {reply, State#state.sweeps, State};

handle_call(stop_all_sweeps, _From, #state{sweeps = Sweeps} = State) ->
    [stop_sweep(Sweep) || Sweep <- get_running_sweeps(Sweeps)],
    {reply, ok, State}.

handle_cast({worker_pid, Index, Pid}, State) ->
    {noreply, add_worker_pid(Index, Pid, State)};

handle_cast({sweep_result, Index, Result}, State) ->
    {noreply, update_finished_sweep(Index, Result, State)};

handle_cast({update_progress, Index, SweptKeys}, State) ->
    {noreply, update_progress(Index, SweptKeys, State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, #state{sweeps = Sweeps} = State) ->
    SweepList = get_running_sweeps(Sweeps),
    case {lists:keyfind(Pid, #sweep.pid, SweepList), Reason} of
        {false, _} ->
            lager:error("Unknown proc ~p ~p", [Pid, Reason]),
            {noreply, State};
        {Sweep, normal} ->
            {noreply, State#state{sweeps = finish_sweep(Sweeps, Sweep)}};
        {Sweep, Reason} ->
            lager:error("Sweep crashed ~p ~p", [Sweep, Reason]),
            {noreply, State#state{sweeps = finish_sweep(Sweeps, Sweep)}}
    end;

handle_info(sweep_tick, State) ->
    schedule_sweep_tick(),
    State1 = maybe_initiate_sweeps(State),
    State2 = maybe_schedule_sweep(State1),
    {noreply, State2}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

maybe_schedule_sweep(#state{sweeps = Sweeps} = State) ->
    CLR = concurrency_limit_reached(Sweeps),
    InSweepWindow = in_sweep_window(),
    SweepSchedulerEnabled = app_helper:get_env(riak_kv, sweeper_scheduler, true),
    lager:info("CLR ~p  InSweepWindow ~p SweepSchedulerEnabled ~p ", [CLR, InSweepWindow, SweepSchedulerEnabled]),
    case InSweepWindow and not CLR and SweepSchedulerEnabled of
        true ->
            schedule_sweep(State);
        false ->
            State
    end.

schedule_sweep(#state{sweeps = Sweeps,
                      sweep_participants = Participant} = State) ->
    case get_never_runned_sweeps(Sweeps) of
        [] ->
            case find_expired_participant(Sweeps, Participant) of
                [] ->
                    State;
                Index ->
                    do_sweep(Index, State)
            end;
        Indices ->
            do_sweep(hd(Indices), State)
    end.

sweep_request(Index, #state{sweeps = Sweeps} = State) ->
    case concurrency_limit_reached(Sweeps) of
        true ->
            LastStartedSweep =
                get_last_started_sweep(get_running_sweeps(Sweeps)),
            stop_sweep(LastStartedSweep);
        false ->
            ok
    end,
    do_sweep(Index, State).

do_sweep(#sweep{index = Index}, State) ->
    do_sweep(Index, State);
do_sweep(Index, #state{sweep_participants = SP, sweeps = Sweeps} = State) ->

    %% Ask for estimate before we ask_participants since riak_kv_index_tree
    %% clears the trees if they are expired.
    AAEEnabled = riak_kv_entropy_manager:enabled(),
    EstimatedNrKeys = get_estimate_keys(Index, AAEEnabled, Sweeps),

    lager:info("EstimatedNrKeys ~p ~p", [Index, EstimatedNrKeys]),
    case ask_participants(Index, SP) of
        [] ->
            State;
        ActiveParticipants ->
            Pid =
                spawn_link(fun() ->
                                   Result = riak_kv_vnode:sweep({Index, node()}, ActiveParticipants, EstimatedNrKeys),
                                   lager:info("Sweep_result: ~p" ,[Result]),
                                   ?MODULE:sweep_result(Index, format_result(Result))
                           end),
            State#state{sweeps = start_sweep(Sweeps, Index, Pid, ActiveParticipants, EstimatedNrKeys)}
    end.

get_estimate_keys(Index, AAEEnabled, Sweeps) ->
	#sweep{estimated_keys = OldEstimate} = dict:fetch(Index, Sweeps),
	maybe_estimate_keys(Index, AAEEnabled, OldEstimate).

maybe_estimate_keys(Index, true, undefined) ->
	get_estimtate(Index);
maybe_estimate_keys(Index, true, {EstimatedNrKeys, TS}) ->
	EstimateOutdated  = elapsed_secs(os:timestamp(), TS) > ?ESTIMATE_EXPIRY,
	case EstimateOutdated of
		true ->
			get_estimtate(Index);
		false ->
			EstimatedNrKeys
	end;
maybe_estimate_keys(_Index, false, {EstimatedNrKeys, _TS}) ->
    EstimatedNrKeys;
maybe_estimate_keys(_Index, false, _) ->
    false.

get_estimtate(Index) ->
	case riak_kv_index_hashtree:estimate_keys(Index) of
		{ok, EstimatedNrKeys} ->
			EstimatedNrKeys;
		_ ->
			0
	end.

disable_sweep_participant_in_running_sweep(Module, Sweeps) ->
    [disable_participant(Sweep, Module) || #sweep{active_participants = ActiveP} = Sweep <- get_running_sweeps(Sweeps),
              lists:keymember(Module, #sweep_participant.module, ActiveP)].

disable_participant(Sweep, Module) ->
    send_to_sweep_worker({disable, Module}, Sweep).

stop_sweep(Sweep) ->
    send_to_sweep_worker(stop, Sweep).

send_to_sweep_worker(Msg, #sweep{index = Index, worker_pid = WorkPid}) when is_pid(WorkPid)->
    Ref = make_ref(),
    WorkPid ! {Msg, self(), Ref},
    receive
        {ack, Ref} ->
            ok;
        {Response, Ref} ->
            Response
    after 4000 ->
        lager:error("No response from sweep proc index ~p ~p", [Index, WorkPid])
    end;
send_to_sweep_worker(Msg,  #sweep{index = Index}) ->
    lager:info("no worker pid ~p to ~p " , [Msg, Index]),
    false.

in_sweep_window() ->
    {_, {Hour, _, _}} = calendar:local_time(),
    in_sweep_window(Hour, sweep_window()).

in_sweep_window(_NowHour, always) ->
    true;
in_sweep_window(_NowHour, never) ->
    false;
in_sweep_window(NowHour, {Start, End}) when Start =< End ->
    (NowHour >= Start) and (NowHour =< End);
in_sweep_window(NowHour, {Start, End}) when Start > End ->
    (NowHour >= Start) or (NowHour =< End).

sweep_window() ->
    case application:get_env(riak_kv, sweep_window) of
        {ok, always} ->
            always;
        {ok, never} ->
            never;
        {ok, {StartHour, EndHour}} when StartHour >= 0, StartHour =< 23,
                                        EndHour >= 0, EndHour =< 23 ->
            {StartHour, EndHour};
        Other ->
            error_logger:error_msg("Invalid riak_kv_sweep window specified: ~p. "
                                   "Defaulting to 'always'.\n", [Other]),
            always
    end.

concurrency_limit_reached(Sweeps) ->
    length(get_running_sweeps(Sweeps)) >= get_concurrency_limit().

get_concurrency_limit() ->
    app_helper:get_env(riak_kv, sweep_concurrency, ?DEFAULS_SWEEP_CONCURRENCY).

-spec schedule_sweep_tick() -> ok.
schedule_sweep_tick() ->
    Tick = app_helper:get_env(riak_kv, sweep_tick, ?DEFAULT_SWEEP_TICK),
    erlang:send_after(Tick, ?MODULE, sweep_tick),
    ok.

%% @private
ask_participants(Index, Participants) ->
    Funs =
        [{Participant, (Participant#sweep_participant.module):participate_in_sweep(Index, self())} ||
         {_Module, Participant} <- dict:to_list(Participants)],
    
    %% Filter non active participants
    [Participant#sweep_participant{sweep_fun = Fun, acc = InitialAcc} ||
     {Participant, {ok, Fun, InitialAcc}} <- Funs].

add_worker_pid(Index, Pid, #state{sweeps = Sweeps} = State) ->
    Sweeps1 =
        dict:update(Index,
                    fun(Sweep) ->
                            Sweep#sweep{worker_pid = Pid}
                    end, Sweeps),
    State#state{sweeps = Sweeps1}.

update_finished_sweep(Index, Result, #state{sweeps = Sweeps} = State) ->
    Sweep = dict:fetch(Index, Sweeps),
    Sweep1 = store_result(Result, Sweep),
    State#state{sweeps = dict:store(Index, Sweep1, Sweeps)}.

store_result({SweptKeys, Result}, #sweep{results = OldResult} = Sweep) ->
    TimeStamp = os:timestamp(),
    UpdatedResults =
        lists:foldl(fun({Mod, Outcome}, Dict) ->
                            dict:store(Mod, {TimeStamp, Outcome}, Dict)
                    end, OldResult, Result),
    Sweep#sweep{swept_keys = SweptKeys, estimated_keys = {SweptKeys, TimeStamp}, results = UpdatedResults, end_time = TimeStamp}.

update_progress(Index, SweptKeys, #state{sweeps = Sweeps} = State) ->
    Sweep = dict:fetch(Index, Sweeps),
    Sweep1 = Sweep#sweep{swept_keys = SweptKeys},
    State#state{sweeps = dict:store(Index, Sweep1, Sweeps)}.

find_expired_participant(Sweeps, Participants) ->
    ExpiredMissingSweeps =
        [{expired_or_missing(Sweep, Participants), Sweep} || Sweep <- get_idle_sweeps(Sweeps)],
    MaxExpiredMissingSweep = hd(lists:reverse(lists:keysort(1, ExpiredMissingSweeps))),
    case MaxExpiredMissingSweep of
        {0, _} -> [];
        {_N, Sweep} -> Sweep
    end.

expired_or_missing(#sweep{index = Index, results = Result}, Participants) ->
    Now = os:timestamp(),
    ResultList = dict:to_list(Result),
    Missing =
        [RunInterval ||
         {Module, #sweep_participant{module = Module, run_interval = RunInterval}}
             <- dict:to_list(Participants), not lists:keymember(Module, 1, ResultList)],
    Expired =
        [begin
             RunInterval = run_interval(Mod, Participants),
             expired(Now, TS, RunInterval)
         end ||
         {Mod, {TS, _Outcome}} <- ResultList],
    lager:info("Index: ~w Missing: ~w Expired: ~w", [Index, Missing, Expired]),
    lists:sum(Missing) + lists:sum(Expired).

expired(Now, TS, RunInterval) ->
    case elapsed_secs(Now, TS) - RunInterval of
        N when N > 0 ->
            N;
        _ ->
            0
    end.

run_interval(Mod, Participants) ->
    case dict:find(Mod, Participants) of
        #sweep_participant{run_interval = RunInterval} ->
            RunInterval;
        _ ->
            %% Participant have been disabled since last run.
            %% TODO: should we remove inactive results?
            0
    end.

elapsed_secs(Now, Start) ->
    timer:now_diff(Now, Start) div 1000000.

format_result(#sa{swept_keys = SweptKeys, active_p = Succ, failed_p = Failed}) ->
    {SweptKeys,
     format_result(succ, Succ) ++ format_result(fail, Failed)}.
format_result(SuccFail, Results) ->
    [{Module, SuccFail} || #sweep_participant{module = Module} <- Results].

finish_sweep(Sweeps, #sweep{index = Index}) ->
    dict:update(Index,
                fun(Sweep) ->
                        Sweep#sweep{state = idle, pid = undefind, worker_pid = undefind}
                end, Sweeps).

start_sweep(Sweeps, Index, Pid, ActiveParticipants, EstimatedNrKeys) ->
    TS = os:timestamp(),
    dict:update(Index,
                fun(Sweep) ->
                        Sweep#sweep{state = running,
                                    pid = Pid,
                                    estimated_keys = {EstimatedNrKeys, TS},
                                    active_participants = ActiveParticipants,
                                    start_time = TS}
                end, Sweeps).

maybe_initiate_sweeps(#state{sweeps = Sweeps} = State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    MissingIdx = [Idx || Idx <- Indices,
                         not dict:is_key(Idx, Sweeps)],
    Sweeps1 = add_sweeps(MissingIdx, Sweeps),
    State#state{sweeps = Sweeps1}.

add_sweeps(MissingIdx, Sweeps) ->
    lists:foldl(fun(Idx, SweepsDict) ->
                        dict:store(Idx, #sweep{index = Idx}, SweepsDict)
                end, Sweeps, MissingIdx).

get_last_started_sweep(RunningSweeps) ->
    SortedSweeps = lists:keysort(#sweep.start_time, RunningSweeps),
    hd(lists:reverse(SortedSweeps)).

get_running_sweeps(Sweeps) ->
    [Sweep || {_Index, #sweep{state = running} = Sweep} <- dict:to_list(Sweeps)].

get_idle_sweeps(Sweeps) ->
    [Sweep || {_Index, #sweep{state = idle} = Sweep} <- dict:to_list(Sweeps)].

get_never_runned_sweeps(Sweeps) ->
    [Sweep || {_Index, #sweep{state = idle, results = ResDict} = Sweep} <- dict:to_list(Sweeps), dict:is_empty(ResDict)].

format_sweeps(Sweeps) ->
    Header =
        io_lib:format("~s~n~-49s  ~-12s  ~-12s~n",
                      [string:centre(" Sweeps ", 79, $=), 
                       "Index",
                       "Last (ago)",
                       "Duration"]),
    Now = os:timestamp(),
    SortedSweeps = lists:keysort(1, Sweeps),
    FormatedSweeps =
        [begin
             EndTime = Sweep#sweep.end_time,
             LastSweep = format_timestamp(Now, EndTime),
             Duration = format_timestamp(EndTime, Sweep#sweep.start_time),
             ResultsString = format_results(Now, Sweep#sweep.results),
             Progress = format_progress(Sweep),
             io_lib:format("~-49b  ~-12s  ~-12s ~s ~n~s", [Index, LastSweep, Duration, ResultsString, Progress])
         end
         || {Index, Sweep} <- SortedSweeps],
    Header ++ FormatedSweeps.

format_progress(#sweep{state = idle}) ->
    "";

format_progress(#sweep{active_participants = AcitvePart, estimated_keys = {EstimatedKeys, _TS} , swept_keys = SweptKeys}) ->
	format_active_participants(AcitvePart) ++
    case EstimatedKeys of
        EstimatedKeys when EstimatedKeys > 0 ->
            progress(SweptKeys/EstimatedKeys, 80);
        _ ->
            format_progress_without_estimate(SweptKeys)
    end.

format_active_participants(AcitvePart) ->
	io_lib:format("| Running: ", []) ++
	[io_lib:format("| ~s", [atom_to_list(Mod)])
	 || #sweep_participant{module = Mod} <- AcitvePart].

format_progress_without_estimate(undefined) ->
    "";
format_progress_without_estimate(SweptKeys) ->
    io_lib:format("~nRunning: no estimate swept ~p keys~n", [SweptKeys]).

progress(PctDecimal, MaxSize) when PctDecimal > 1 ->
	progress(1, MaxSize);

progress(PctDecimal, MaxSize) ->
    ProgressTotalSize = progress_size(MaxSize),
    ProgressSize = trunc(PctDecimal * ProgressTotalSize),
    PadSize = ProgressTotalSize - ProgressSize,
    FormatStr = progress_fmt(ProgressSize, PadSize),
    riak_core_format:fmt(FormatStr, ["", "", integer_to_list(trunc(PctDecimal * 100))]).

progress_size(MaxSize) ->
    MaxSize - 3. %% 3 fixed characters in progress bar

progress_fmt(ArrowSize, PadSize) ->
    riak_core_format:fmt("~n|~~~p..=s~~~p.. s| ~~3.. s%~n", [ArrowSize, PadSize]).

format_results(Now, Results) ->
    ResultList = dict:to_list(Results),
    SortedResultList = lists:keysort(1, ResultList),
    [begin
        LastResult = format_timestamp(Now, TimeStamp),
        io_lib:format("| ~s ~-4s ~-8s", [atom_to_list(Mod), string:to_upper(atom_to_list(Outcome)), LastResult])
     end
    || {Mod, {TimeStamp, Outcome}} <- SortedResultList].

format_participants(SweepParticipants) ->
    Header =
        io_lib:format("~s~n~-25s  ~-20s ~-15s~n",
                      [string:centre(" Sweep Participants ", 79, $=),
                       "Module",
                       "Desciption",
                       "Run interval"]),
    SortedSweepParticipants = lists:keysort(1, SweepParticipants),
    lager:info("lixen ~p", [SortedSweepParticipants]),
    FormatedParticipants =
        [begin
             IntervalString = format_interval(Interval * 1000000),
             io_lib:format("~-25s  ~-20s ~-15s ~n", [atom_to_list(Mod), Desciption,  IntervalString])
         end
         || {Mod, #sweep_participant{description = Desciption, run_interval = Interval}}
                <- SortedSweepParticipants],
    Header ++ FormatedParticipants.

format_timestamp(_Now, undefined) ->
    "--";
format_timestamp(undefined, _) ->
    "--";
format_timestamp(Now, TS) ->
    case timer:now_diff(Now, TS) of
        Diff when Diff > 0 ->
            riak_core_format:human_time_fmt("~.1f", Diff);
        _ ->
            "--"
    end.

format_interval(Interval) ->
    riak_core_format:human_time_fmt("~.1f", Interval).

persist_participants(Participants) ->
    file:write_file(sweep_file("participants.dat") , io_lib:fwrite("~p.\n",[Participants])).

get_persistent_participants() ->
    case file:consult(sweep_file("participants.dat")) of
        {ok, [Participants]} ->
            Participants;
        _ ->
            false
    end.

sweep_file(File) ->
    PDD = app_helper:get_env(riak_core, platform_data_dir, "/tmp"),
    SweepDir = filename:join(PDD, ?MODULE),
    SweepFile = filename:join(SweepDir, File),
    ok = filelib:ensure_dir(SweepFile),
    SweepFile.
