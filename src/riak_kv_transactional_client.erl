-module(riak_kv_transactional_client).

-behaviour(gen_server).

-export([begin_transaction/1,
         commit_transaction/1,
         get/2,
         print_state/1,
         put/3,
         start_link/1]).

-export([code_change/3,
         handle_call/3,
         handle_cast/2,
	     handle_info/2,
	     init/1,
	     terminate/2]).

-define(DefaultBucket, <<"default_bucket">>).

-record(state, {client,
                clock,
                in_transaction,
                id,
                snapshot,
                gets,
                puts}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Node) ->
    gen_server:start_link(?MODULE, Node, []).

get(Key, Client) when is_list(Key) ->
    gen_server:call(Client, {get, list_to_binary(Key)}).

put(Key, Value, Client) when is_list(Key) ->
    gen_server:call(Client, {put, list_to_binary(Key), Value}).

begin_transaction(Client) ->
    gen_server:call(Client, begin_transaction).

commit_transaction(Client) ->
    gen_server:call(Client, commit_transaction).

print_state(Client) ->
    gen_server:cast(Client, print_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Node) ->
    case riak:client_connect(Node) of
        {ok, Client} ->
            State = #state{client = Client,
                           clock = 0,
                           in_transaction = false,
                           id = undefined,
                           snapshot = undefined,
                           gets = [],
                           puts = dict:new()},
            {ok, State};

        _ ->
            {stop, error}
    end.

% Transactional get
handle_call(
  {get, Key},
  _From,
  #state{in_transaction = true,
         snapshot = Snapshot,
         puts = Puts} = State
) ->
    GetResult1 = do_get(Key, State),
    ReadOnly = dict:size(Puts) == 0,
    GetResult = maybe_select_object_contents(GetResult1, Snapshot, ReadOnly),
    NewState = maybe_set_snapshot(GetResult, State),
    NewState1 = record_get(Key, NewState),
    Conflict = check_get_conflict(GetResult, Snapshot),
    if
        Conflict ->
            {reply, {error, transaction_aborted}, abort_transaction(NewState1)};

        true ->
            {reply, GetResult, NewState1}
    end;

% Single get 
handle_call({get, Key}, _From, State) ->
    GetResult1 = do_get(Key, State),
    GetResult = maybe_select_object_contents(GetResult1, -1, false),
    NewState = maybe_update_clock(GetResult, State),
    {reply, GetResult, NewState};

% Transactional put 
handle_call(
  {put, Key, Value},
  _From,
  #state{in_transaction = true, puts = Puts} = State
 ) ->
    Object = riak_object:new(?DefaultBucket, Key, Value),
    NewPuts = dict:store(Key, Object, Puts),
    NewState = State#state{puts = NewPuts},
    {reply, ok, NewState};

% Single put
% Acts as a single operation transaction
handle_call(
  {put, Key, Value},
  _From,
  #state{client = Client, clock = Clock} = State
 ) ->
    Id = erlang:phash2({self(), os:timestamp()}),
    Object = riak_object:new(?DefaultBucket, Key, Value),

    Result = Client:commit_transaction(Id, Clock, [], [Object]),

    % TODO do something with the result
    % update clock to match the transaction position in the log
    % or the latest log position in case the transaction aborts

    {reply, Result, State};

handle_call(begin_transaction, _From, #state{in_transaction = false} = State) ->
    Id = erlang:phash2({self(), os:timestamp()}),
    NewState = State#state{in_transaction = true, id = Id},
    {reply, ok, NewState};

handle_call(begin_transaction, _From, State) ->
    {reply, {error, in_transaction}, State};

handle_call(commit_transaction, _From, #state{in_transaction = false} = State) ->
    {reply, {error, not_in_transaction}, State};

handle_call(
  commit_transaction,
  _From,
  #state{client = Client,
         id = Id,
         snapshot = Snapshot,
         gets = Gets,
         puts = PutsDict} = State
 ) ->
    % TODO handle cases in which the transaction did no do anything -> empty gets and puts

    Puts = dict:fold(fun(_, Value, Acc) -> [Value | Acc] end, [], PutsDict),
    Result = Client:commit_transaction(Id, Snapshot, Gets, Puts),

    % TODO do something with the result
    % update clock to match the transaction position in the log
    % or the latest log position in case the transaction aborts

    NewState = clean_transaction_state(State),
    {reply, Result, NewState};

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast(print_state, State) ->
    io:format("~p~n", [State]),
    {noreply, State};

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

clean_transaction_state(State) ->
    State#state{in_transaction = false,
                id = undefined,
                snapshot = undefined,
                gets = [],
                puts = dict:new()}.

do_get(Key, #state{client = Client, snapshot = undefined, clock = Clock}) ->
    do_get_remote(Client, Key, Clock);

do_get(Key, #state{client = Client, snapshot = Snapshot, puts = Puts}) ->
    case do_get_local(Key, Puts) of
        {ok, Object} -> {ok, Object, Snapshot};
        _ -> do_get_remote(Client, Key, Snapshot)
    end.

do_get_local(Key, Puts) ->
    case dict:find(Key, Puts) of
        {ok, _} = Reply -> Reply;
        error -> {error, not_found}  
    end.

do_get_remote(Client, Key, Snapshot) ->
    case Client:get(?DefaultBucket, Key) of
        {ok, _Object, VnodeSnapshot} = Reply ->
            if
                Snapshot > VnodeSnapshot ->
                    % TODO eventually wait for node to be up to date
                    {error, node_snapshot_behind_local_snapshot};

                true -> Reply
            end;

        {error, _, _} = Error ->
            Error
    end.

maybe_select_object_contents({ok, Object, VnodeSnapshot}, Snapshot, ReadOnly) ->
    [Acc1 | _] = Contents = riak_object:get_contents(Object),
    SelectedContent = lists:foldl(fun(Content, Acc) ->
                                          ContentVersion = get_version(Content),
                                          AccVersion = get_version(Acc),
                                          if
                                              % Read-only transaction
                                              % Get value consistent with the snapshot
                                              ReadOnly ->
                                                  if
                                                      ContentVersion > AccVersion andalso ContentVersion =< Snapshot ->
                                                          Content;
                                                      true ->
                                                          Acc
                                                  end;

                                              % Read-write transaction
                                              % Get latest value 
                                              true ->
                                                  if
                                                      ContentVersion > AccVersion -> Content;
                                                      true -> Acc 
                                                  end
                                          end
                                  end, Acc1, Contents),
    case get_version(SelectedContent) of
        -1 ->
            {error, not_found, VnodeSnapshot};
        _ ->
            NewObject = riak_object:set_contents(Object, [SelectedContent]),
            {ok, NewObject, VnodeSnapshot}
    end;

maybe_select_object_contents(GetResult, _Snapshot, _Puts) -> GetResult.

get_version({Metadata, _}) ->
    case dict:find(<<"version">>, Metadata) of
        {ok, Value} -> Value;
        error -> -1 
    end.

maybe_set_snapshot({_, _, VnodeSnapshot}, #state{snapshot = undefined} = State)
  when is_integer(VnodeSnapshot), VnodeSnapshot /= -1 ->
    State#state{snapshot = VnodeSnapshot};
maybe_set_snapshot(_GetResult, State) -> State.

record_get(Key, #state{gets = Gets} = State) ->
    NewGets = [Key | Gets],
    State#state{gets = NewGets}.

maybe_update_clock({_, _, VnodeSnapshot}, State)
  when is_integer(VnodeSnapshot), VnodeSnapshot /= -1 ->
    State#state{clock = VnodeSnapshot};
maybe_update_clock(_GetResult, State) -> State.

check_get_conflict({ok, Object, _VnodeSnapshot}, Snapshot) ->
    riak_object:get_version(Object) > Snapshot;
check_get_conflict(_GetResult, _Snapshot) -> false.

abort_transaction(State) ->
    % TODO eventually send message to vnodes do clean up temporary values
    clean_transaction_state(State).
