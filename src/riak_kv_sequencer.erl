-module(riak_kv_sequencer).

-behaviour(gen_server).

-export([start_link/0,
         next/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {n}).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).


next()->
    gen_server:call({global, ?MODULE}, next).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    State = #state{n = 0},
    {ok, State}.


handle_call(next, _From, #state{n = N} = State) ->
    %lager:info("Received next request from ~p, responding with ~p~n", [From, N + 1]),
    % TODO eventually write N to disk
    Response = {ok, N + 1},
    NewState = State#state{n = N + 1},
    {reply, Response, NewState};

handle_call(Request, From, State) ->
    lager:error("Unexpected message (~p) received at hanlde_call from ~p~n", [Request, From]),
    {reply, ok, State}.


handle_cast(Request, State) ->
    lager:error("Unexpected message (~p) received at hanlde_cast~n", [Request]),
    {noreply, State}.


handle_info(Info, State) ->
    lager:error("Unexpected message (~p) received at hanlde_info~n", [Info]),
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
