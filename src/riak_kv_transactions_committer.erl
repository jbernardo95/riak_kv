-module(riak_kv_transactions_committer).

-behaviour(gen_server).

-export([start_link/0, print_state/0]).

-export([code_change/3,
         handle_call/3,
         handle_cast/2,
	     handle_info/2,
	     init/1,
	     terminate/2]).

-define(LOG, riak_kv_log).

-record(state, {log, continuation, n_records_read}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

print_state() ->
    gen_server:cast({global, ?MODULE}, print_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    erlang:send(self(), main),
    State = #state{continuation = start, n_records_read = 0},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast(print_state, State) ->
    io:format("~p~n", [State]),
    {noreply, State};

handle_cast(Request, State) ->
    lager:error("Unexpected request received at hanlde_cast: ~p~n", [Request]),
    {noreply, State}.

handle_info(main, State) ->
    NewState = main(State),
    erlang:send_after(1000, self(), main),
    {noreply, NewState};

handle_info(Info, State) ->
    lager:error("Unexpected info received at handle_info: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

main(#state{continuation = Continuation, n_records_read = NRecordsRead} = State) ->
    NRecordsLogged = riak_kv_log:get_n_records_logged(),
    N = (NRecordsLogged - 1) - NRecordsRead,
    {NewContinuation, Records} = read_records(?LOG, Continuation, N), 
    lager:info("Records read: ~p~n", [Records]),
    State#state{continuation = NewContinuation, n_records_read = (NRecordsLogged - 1)}.

read_records(Log, Continuation, -1) ->
    read_records(Log, Continuation, 0);
read_records(Log, Continuation, N) ->
    read_records(Log, Continuation, N, []).

read_records(_Log, Continuation, 0, Terms) ->
    {Continuation, Terms};
read_records(Log, Continuation, N, Terms) ->
    case disk_log:chunk(Log, Continuation, N) of
        {Continuation1, Terms1} ->
            NewN = N - length(Terms1),
            read_records(Log, Continuation1, NewN, Terms1 ++ Terms)
    end.
