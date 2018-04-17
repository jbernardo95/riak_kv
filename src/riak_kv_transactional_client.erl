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

-define(DEFAULT_TIMEOUT, 60000).
-define(DEFAULT_BUCKET, <<"default_bucket">>).

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
    gen_server:call(Client, {get, list_to_binary(Key)}, ?DEFAULT_TIMEOUT).

put(Key, Value, Client) when is_list(Key) ->
    gen_server:call(Client, {put, list_to_binary(Key), Value}, ?DEFAULT_TIMEOUT).

begin_transaction(Client) ->
    gen_server:call(Client, begin_transaction, ?DEFAULT_TIMEOUT).

commit_transaction(Client) ->
    gen_server:call(Client, commit_transaction, ?DEFAULT_TIMEOUT).

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
                           gets = dict:new(),
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
    Bkey = {?DEFAULT_BUCKET, Key},
    ReadOnly = dict:size(Puts) == 0,
    GetResult = do_get(Bkey, State, ReadOnly),
    NewState2 = maybe_set_snapshot(GetResult, State),
    NewState1 = maybe_record_get(GetResult, NewState2),
    Conflict = check_get_conflict(GetResult, Snapshot),
    if
        Conflict ->
            Reply = get_reply({error, transaction_aborted}),
            NewState = abort_transaction(NewState1),
            {reply, Reply, NewState};
        true ->
            Reply = get_reply(GetResult),
            {reply, Reply, NewState1}
    end;

% Single get 
handle_call({get, Key}, _From, State) ->
    Bkey = {?DEFAULT_BUCKET, Key},
    GetResult = do_get(Bkey, State, false),

    Reply = get_reply(GetResult),
    NewState = maybe_update_clock(GetResult, State),
    {reply, Reply, NewState};

% Transactional put 
handle_call(
  {put, Key, Value},
  _From,
  #state{in_transaction = true, id = Id, puts = Puts} = State
 ) ->
    Object = create_object(?DEFAULT_BUCKET, Key, Value, Id),
    Bkey = riak_object:bkey(Object),
    NewPuts = dict:store(Bkey, Object, Puts),
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
    Object = create_object(?DEFAULT_BUCKET, Key, Value, Id),
    {Status, Lsn} = Client:commit_transaction(Id, Clock, [], [Object]),

    Reply = commit_reply(Status),
    NewState = State#state{clock = Lsn},
    {reply, Reply, NewState};

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
         gets = GetsDict,
         puts = PutsDict} = State
) ->
    % TODO handle cases in which the transaction did no do anything -> empty gets and puts

    Gets = dict:fetch_keys(GetsDict),
    Puts = dict:fold(fun(_, Value, Acc) -> [Value | Acc] end, [], PutsDict),

    {Status, Lsn} = Client:commit_transaction(Id, Snapshot, Gets, Puts),

    Reply = commit_reply(Status),
    NewState1 = clean_transaction_state(State),
    NewState = NewState1#state{clock = Lsn},
    {reply, Reply, NewState};

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
                gets = dict:new(),
                puts = dict:new()}.

% Non transactional get
do_get(
  Bkey,
  #state{client = Client,
         in_transaction = false,
         snapshot = undefined,
         clock = Clock},
  ReadOnly
) ->
    do_get_remote(Client, Bkey, Clock, ReadOnly);

% Transactional get
do_get(
  Bkey,
  #state{client = Client,
         in_transaction = true,
         snapshot = Snapshot,
         puts = Puts},
  ReadOnly
) ->
    case do_get_local(Bkey, Puts) of
        {ok, Object} -> {ok, Object, Snapshot};
        _ -> do_get_remote(Client, Bkey, Snapshot, ReadOnly)
    end.

do_get_local(Bkey, Puts) ->
    case dict:find(Bkey, Puts) of
        {ok, _} = Reply -> Reply;
        error -> {error, not_found}  
    end.

do_get_remote(Client, {Bucket, Key}, Snapshot, ReadOnly) ->
    case Client:get(Bucket, Key) of
        {ok, _Object, VnodeSnapshot} = Reply ->
            if
                Snapshot > VnodeSnapshot ->
                    % TODO wait for node to be up to date
                    {error, node_snapshot_behind_local_snapshot};
                true ->
                    select_object_contents(Reply, Snapshot, ReadOnly)
            end;
        {error, _, _} = Error ->
            Error
    end.

select_object_contents({ok, Object, VnodeSnapshot}, Snapshot, ReadOnly) ->
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
    end.

get_version({Metadata, _}) ->
    case dict:find(<<"version">>, Metadata) of
        {ok, Value} -> Value;
        error -> -1 
    end.

maybe_set_snapshot({_, _, VnodeSnapshot}, #state{snapshot = undefined} = State)
  when is_integer(VnodeSnapshot), VnodeSnapshot /= -1 ->
    State#state{snapshot = VnodeSnapshot};
maybe_set_snapshot(_GetResult, State) -> State.

maybe_record_get({ok, Object, _VnodeSnapshot}, #state{gets = Gets} = State) ->
    NewGets = dict:store(riak_object:bkey(Object), ok, Gets),
    State#state{gets = NewGets};
maybe_record_get(_GetResult, State) -> State.

maybe_update_clock({_, _, VnodeSnapshot}, State)
  when is_integer(VnodeSnapshot), VnodeSnapshot /= -1 ->
    State#state{clock = VnodeSnapshot};
maybe_update_clock(_GetResult, State) -> State.

check_get_conflict({ok, Object, _VnodeSnapshot}, Snapshot) ->
    riak_object:get_version(Object) > Snapshot;
check_get_conflict(_GetResult, _Snapshot) -> false.

get_reply({ok, Object, _VnodeSnapshot}) -> {ok, Object};
get_reply(GetResult) -> GetResult.

commit_reply(committed) -> ok;
commit_reply(aborted) -> {error, aborted};
commit_reply(_Status) -> error.

abort_transaction(State) ->
    % TODO eventually send message to vnodes to inform that the transaction aborted 
    clean_transaction_state(State).

create_object(Bucket, Key, Value, Id) ->
    Object = riak_object:new(Bucket, Key, nil),
    Metadata = dict:store(<<"transaction_id">>, Id, dict:new()),
    riak_object:set_contents(Object, [{Metadata, Value}]).
