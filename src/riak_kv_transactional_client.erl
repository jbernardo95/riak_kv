-module(riak_kv_transactional_client).

-behaviour(gen_server).

-export([start_link/1,
         get/2,
         put/3,
         begin_transaction/1,
         commit_transaction/1,
         print_state/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(DefaultBucket, <<"default_bucket">>).

-record(state, {client, clock, in_transaction, snapshot}).


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
                           snapshot = nil},
            {ok, State};

        _ ->
            {stop, error}
    end.


% Transactional get
handle_call(
  {get, Key},
  _From,
  #state{client = Client, clock = Clock, in_transaction = true, snapshot = nil} = State
 ) ->
    {ok, Object} = Client:get(?DefaultBucket, Key),
    Timestamp = riak_object:get_timestamp(Object),

    NewState = State#state{snapshot = max(Clock, Timestamp)},
    {reply, {ok, Object}, NewState};

% Normal get
handle_call({get, Key}, _From, #state{client = Client, clock = Clock} = State) ->
    {ok, Object} = Client:get(?DefaultBucket, Key),
    Timestamp = riak_object:get_timestamp(Object),

    NewState = State#state{clock = max(Clock, Timestamp)},
    {reply, {ok, Object}, NewState};

handle_call({put, Key, Value}, _From, #state{client = Client, clock = Clock} = State) ->
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
    NewState = State#state{in_transaction = false, snapshot = nil},
    {reply, ok, NewState};

handle_call(_Request, _From, State) ->
    lager:error("Unexpected message received at hanlde_call~n"),
    {reply, ok, State}.


handle_cast(print_state, State) ->
    io:format("~p~n", [State]),
    {noreply, State};

handle_cast(_Request, State) ->
    lager:error("Unexpected message received at hanlde_cast~n"),
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
