-module(message_batcher).

-behaviour(gen_server).

-export([start_link/3,
         add_to_batch/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {dispatch_fun, batch_size, batch_timeout, batch, timer}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(DispatchFun, BatchSize, BatchTimeout) ->
    gen_server:start_link(?MODULE, [DispatchFun, BatchSize, BatchTimeout], []).

add_to_batch(Pid, Message) ->
    gen_server:cast(Pid, {add_to_batch, Message}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([DispatchFun, BatchSize, BatchTimeout]) ->
    Batch = ets:new(batch, [private]),
    ets:insert(Batch, {size, 0}),

    Timer = erlang:send_after(BatchTimeout, self(), batch_timeout),

    State = #state{dispatch_fun = DispatchFun,
                   batch_size = BatchSize,
                   batch_timeout = BatchTimeout,
                   batch = Batch,
                   timer = Timer},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({add_to_batch, Message}, State) ->
    NewState = do_add_to_batch(Message, State),
    {noreply, NewState};

handle_cast(Request, State) ->
    lager:error("Unexpected request received at hanlde_cast: ~p~n", [Request]),
    {noreply, State}.

handle_info(batch_timeout, State) ->
    NewState = do_dispatch_batch(State),
    {noreply, NewState};

handle_info(Info, State) ->
    lager:error("Unexpected info received at handle_info: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_add_to_batch(Message, #state{batch_size = BatchSize, batch = Batch} = State) ->
    %lager:info("Adding message (~p) to batch~n", [Message]),

    InsertAt = ets:update_counter(Batch, size, 1),
    ets:insert(Batch, {InsertAt, Message}),

    if
        InsertAt >= BatchSize -> do_dispatch_batch(State);
        true -> State 
    end.

do_dispatch_batch(
  #state{dispatch_fun = DispatchFun,
         batch_timeout = BatchTimeout,
         batch = Batch,
         timer = Timer} = State
) ->
    erlang:cancel_timer(Timer),

    FoldFun = fun(I, Acc) ->
                  Message = ets:lookup_element(Batch, I, 2),
                  [Message | Acc]
              end,
    CurrentBatchSize = ets:lookup_element(Batch, size, 2),
    BatchToSend = lists:foldl(FoldFun, [], lists:reverse(lists:seq(1, CurrentBatchSize))),

    case CurrentBatchSize of
        0 -> ok;
        _ ->
            %lager:info("Dispatching batch of ~p messages~n", [CurrentBatchSize]),
            DispatchFun(BatchToSend),
            ets:insert(Batch, {size, 0})
    end,

    NewTimer = erlang:send_after(BatchTimeout, self(), batch_timeout),

    State#state{timer = NewTimer}.
