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

-record(state, {client, clock, in_transaction, snapshot, gets, puts}).

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
                           snapshot = undefined,
                           gets = [],
                           puts = []},
            {ok, State};

        _ ->
            {stop, error}
    end.

% Transactional get
handle_call(
  {get, Key},
  _From,
  #state{client = Client,
         in_transaction = true,
         snapshot = Snapshot} = State
) ->
    GetResult = do_get(Client, Key, Snapshot),
    NewState = maybe_set_snapshot(GetResult, State),
    NewState1 = maybe_record_get(GetResult, NewState),
    Conflict = check_get_conflict(GetResult, Snapshot),
    if
        Conflict ->
            {reply, {error, transaction_aborted}, abort_transaction(NewState1)};

        true ->
            {reply, GetResult, NewState1}
    end;

% Normal get
handle_call(
  {get, Key},
  _From,
  #state{client = Client, clock = Clock} = State
 ) ->
    Reply = do_get(Client, Key),
    NewState = case Reply of
                   {ok, Object, _Snapshot} ->
                       Timestamp = riak_object:get_timestamp(Object),
                       State#state{clock = max(Clock, Timestamp)};
                   _ ->
                       State
               end,
    {reply, Reply, NewState};

% Transactional put 
handle_call(
  {put, Key, Value},
  _From,
  #state{in_transaction = true, puts = Puts} = State) ->
    Object = riak_object:new(?DefaultBucket, Key, Value),
    NewPuts = [Object | Puts],
    NewState = State#state{puts = NewPuts},
    {reply, ok, NewState};

% Normal put
handle_call(
  {put, Key, Value},
  _From,
  #state{client = Client, clock = Clock} = State) ->
    Object = riak_object:new(?DefaultBucket, Key, Value),
    {ok, Timestamp} = Client:put(Object, Clock),
    NewState = State#state{clock = Timestamp},
    {reply, ok, NewState};

handle_call(begin_transaction, _From, #state{in_transaction = false} = State) ->
    NewState = State#state{in_transaction = true},
    {reply, ok, NewState};

handle_call(begin_transaction, _From, State) ->
    {reply, {error, in_transaction}, State};

handle_call(commit_transaction, _From, #state{in_transaction = false} = State) ->
    {reply, {error, not_in_transaction}, State};

handle_call(commit_transaction, _From, State) ->
    % TODO actually commit the transaction
    NewState = clean_transaction_state(State)
    {reply, ok, NewState};

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
                snapshot = undefined,
                gets = [],
                puts = []}.

do_get(Client, Key) -> do_get(Client, Key, undefined).

do_get(Client, Key, undefined) ->
    Client:get(?DefaultBucket, Key);

do_get(Client, Key, TransactionSnapshot) ->
    case Client:get(?DefaultBucket, Key) of
        {ok, Object, NodeSnapshot} ->
            if
                TransactionSnapshot > NodeSnapshot ->
                    % TODO eventually wait for node to be up to date
                    {error, node_snapshot_behind_transaction_snapshot};

                true ->
                    {ok, prune_outdated_values(Object), NodeSnapshot}
            end;

        {error, _} = Error ->
            Error
    end.

prune_outdated_values(Object) ->
    [Value | _] = lists:sort(fun({Metadata1, _}, {Metadata2, _}) ->
                                     get_version(Metadata1) < get_version(Metadata2)
                             end, riak_object:get_contents(Object)),
    riak_object:set_contents(Object, [Value]).

get_version(Metadata) ->
    case dict:find(version, Metadata) of
        {ok, Value} -> Value;
        error -> 0
    end.

maybe_set_snapshot({ok, Object, _Snapshot}, #state{snapshot = undefined} = State) ->
    Metadata = riak_object:get_metadata(Object),
    State#state{snapshot = get_version(Metadata)};

maybe_set_snapshot(_GetResult, State) -> State.

maybe_record_get({ok, Object, _Snapshot}, #state{gets = Gets} = State) ->
    NewGets = [Object | Gets],
    State#state{gets = NewGets};

maybe_set_snapshot(_GetResult, State) -> State.

check_get_conflict({ok, Object, Snapshot}, Snapshot) ->
    Metadata = riak_object:get_metadata(Object),
    get_version(Metadata) > Snapshot;

check_get_conflict(_Object, _Snapshot) -> false.

abort_transaction(State) ->
    % TODO eventually send message to vnodes do clean up temporary values
    clean_transaction_state(State).
