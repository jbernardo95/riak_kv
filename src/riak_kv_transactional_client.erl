-module(riak_kv_transactional_client).

-behaviour(gen_server).

-export([begin_transaction/1,
         commit_transaction/1,
         get/4,
         put/5,
         start_link/1]).

-export([code_change/3,
         handle_call/3,
         handle_cast/2,
	     handle_info/2,
	     init/1,
	     terminate/2]).

-define(DEFAULT_TIMEOUT, 60000).

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

get(Node, Bucket, Key, Client) when is_binary(Bucket), is_binary(Key) ->
    gen_server:call(Client, {get, Node, Bucket, Key}, ?DEFAULT_TIMEOUT).

put(Node, Bucket, Key, Value, Client) when is_binary(Bucket), is_binary(Key) ->
    gen_server:call(Client, {put, Node, Bucket, Key, Value}, ?DEFAULT_TIMEOUT).

begin_transaction(Client) ->
    gen_server:call(Client, begin_transaction, ?DEFAULT_TIMEOUT).

commit_transaction(Client) ->
    gen_server:call(Client, commit_transaction, ?DEFAULT_TIMEOUT).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Node) ->
    {ok, Client} = riak:client_connect(Node),

    {ok, #state{client = Client,
                clock = 0,
                in_transaction = false,
                id = undefined,
                snapshot = undefined,
                gets = dict:new(),
                puts = dict:new()}}.

% Transactional get
handle_call(
  {get, Node, Bucket, Key},
  _From,
  #state{in_transaction = true,
         snapshot = Snapshot} = State
) ->
    GetResult = do_get(Node, Bucket, Key, State),
    NewState2 = maybe_set_snapshot(GetResult, State),
    NewState1 = maybe_record_get(GetResult, NewState2),
    Conflict = check_get_conflict(GetResult, Snapshot),
    if
        Conflict ->
            Reply = {error, aborted},
            NewState = clean_transaction_state(NewState1),
            {reply, Reply, NewState};
        true ->
            {reply, GetResult, NewState1}
    end;

% Single get 
handle_call({get, Node, Bucket, Key}, _From, State) ->
    GetResult = do_get(Node, Bucket, Key, State),
    NewState = maybe_update_clock(GetResult, State),
    {reply, GetResult, NewState};

% Transactional put 
handle_call(
  {put, Node, Bucket, Key, Value},
  _From,
  #state{clock = Clock,
         in_transaction = true,
         id = Id,
         snapshot = Snapshot,
         puts = Puts} = State
 ) ->
    Object = case Snapshot of
                 undefined ->
                     create_object(Node, Bucket, Key, Value, Id, Clock + 1);
                 _ ->
                     create_object(Node, Bucket, Key, Value, Id, Snapshot + 1)
             end,
    NewPuts = dict:store({Node, Bucket, Key}, Object, Puts),
    NewState = State#state{puts = NewPuts},
    {reply, ok, NewState};

% Single put
% Acts as a single operation transaction
handle_call(
  {put, Node, Bucket, Key, Value},
  _From,
  #state{clock = Clock} = State
 ) ->
    Id = erlang:phash2({self(), os:timestamp()}),
    Object = create_object(Node, Bucket, Key, Value, Id, Clock + 1),
    do_commit_transaction(Id, Clock, [], [Object], State);

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
  #state{id = Id,
         snapshot = Snapshot,
         gets = GetsDict,
         puts = PutsDict} = State
) ->
    Gets = dict:fetch_keys(GetsDict),
    Puts = dict:fold(fun(_, Value, Acc) -> [Value | Acc] end, [], PutsDict),
    do_commit_transaction(Id, Snapshot, Gets, Puts, State);


handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

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
  Node, Bucket, Key,
  #state{client = Client,
         in_transaction = false,
         snapshot = undefined,
         clock = Clock}
) ->
    do_get_remote(Node, Bucket, Key, Client, Clock, false);

% Transactional get
% Without snapshot 
do_get(
  Node, Bucket, Key,
  #state{client = Client,
         in_transaction = true,
         snapshot = undefined,
         clock = Clock,
         puts = Puts}
) ->
    case do_get_local(Node, Bucket, Key, Puts) of
        {ok, Object} -> {ok, Object};
        _ ->
            do_get_remote(Node, Bucket, Key, Client, Clock, false)
    end;

% Transactional get
% With snapshot 
do_get(
  Node, Bucket, Key,
  #state{client = Client,
         in_transaction = true,
         snapshot = Snapshot,
         puts = Puts}
) ->
    case do_get_local(Node, Bucket, Key, Puts) of
        {ok, Object} -> {ok, Object};
        _ ->
            ReadOnly = dict:size(Puts),
            do_get_remote(Node, Bucket, Key, Client, Snapshot, ReadOnly)
    end.

do_get_local(Node, Bucket, Key, Puts) ->
    case dict:find({Node, Bucket, Key}, Puts) of
        {ok, _} = Reply -> Reply;
        error -> {error, not_found}  
    end.

do_get_remote(Node, Bucket, Key, {_, [ClientNode, _]}, Snapshot, ReadOnly) ->
    proc_lib:spawn_link(ClientNode, riak_kv_node_get_fsm, start_link,
                        [Node, Bucket, Key, self()]),

    receive
        {ok, Object} ->
            select_object_content(Object, Snapshot, ReadOnly);
        {error, _Reason} = Error ->
            Error
    end.

% Selects latest r_content taking in account the snapshot for tentative r_content 
% 
% For an object with the following content: 1, 2, 5, 8, t10, t15 a client with snapshot
% 12 will get an object with content t10
%
% For an object with the following content: 1, 2, 5, 8, 10 a client with snapshot 9
% will get an object with content 10
select_object_content(Object, Snapshot, false = _ReadOnly) ->
    SelectFun = fun(Content, Acc) ->
                        ContentVersion = riak_object:get_version(Content),
                        if
                            Acc == nil -> Content;
                            Acc /= nil ->
                                AccVersion = case riak_object:get_version(Acc) of
                                                 -1 -> riak_object:get_tentative_version(Acc); 
                                                 Version -> Version
                                             end,
                                if
                                    ContentVersion == -1 -> % Tentative content
                                        ContentVersion1 = riak_object:get_tentative_version(Content),
                                        if
                                            ContentVersion1 > AccVersion andalso ContentVersion1 =< Snapshot -> Content;
                                            true -> Acc
                                        end;

                                    true -> % Committed content
                                        if
                                            ContentVersion > AccVersion -> Content;
                                            true -> Acc
                                        end
                                end
                        end
                end,
    do_select_object_content(Object, SelectFun);

% Selects the r_content consistent with the given snapshot
% 
% For an object with the following content: 1, 2, 5, 8, t10, t15 a client with snapshot
% 12 will get an object with content t10
%
% For an object with the following content: 1, 2, 5, 8, 10 a client with snapshot 9
% will get an object with content 8
select_object_content(Object, Snapshot, true = _ReadOnly) ->
    SelectFun = fun(Content, Acc) ->
                        ContentVersion = case riak_object:get_version(Content) of
                                             -1 -> riak_object:get_tentative_version(Content); 
                                             Version1 -> Version1
                                         end,
                        if
                            Acc == nil ->
                                if
                                    ContentVersion =< Snapshot -> Content;
                                    true -> Acc
                                end;
                            Acc /= nil ->
                                AccVersion = case riak_object:get_version(Acc) of
                                                 -1 -> riak_object:get_tentative_version(Acc); 
                                                 Version2 -> Version2
                                             end,
                                if
                                    ContentVersion > AccVersion andalso ContentVersion =< Snapshot -> Content;
                                    true -> Acc
                                end
                        end
                end,
    do_select_object_content(Object, SelectFun).

do_select_object_content(Object, SelectFun) ->
    Contents = riak_object:get_contents(Object),
    SelectedContent = lists:foldl(SelectFun, nil, Contents),
    if
        SelectedContent == nil ->
            {error, not_found};
        true ->
            Version = riak_object:get_version(SelectedContent),
            case Version of
                -1 -> % Selected content has not yet been committed
                    {error, try_again};
                _ -> % Selected content has been committed
                    NewObject = riak_object:set_contents(Object, [SelectedContent]),
                    {ok, NewObject}
            end
    end.

maybe_set_snapshot({ok, Object}, #state{snapshot = undefined} = State) ->
    Version = riak_object:get_version(Object),
    if
        Version /= -1 -> State#state{snapshot = Version};
        true -> State
    end;
maybe_set_snapshot(_GetResult, State) -> State.

maybe_record_get({ok, Object}, #state{gets = Gets} = State) ->
    NewGets = dict:store(riak_object:nbkey(Object), ok, Gets),
    State#state{gets = NewGets};
maybe_record_get(_GetResult, State) -> State.

maybe_update_clock({ok, Object}, State) ->
    Version = riak_object:get_version(Object),
    if
        Version /= -1 -> State#state{clock = Version};
        true -> State
    end;
maybe_update_clock(_GetResult, State) -> State.

check_get_conflict({ok, Object}, Snapshot) ->
    riak_object:get_version(Object) > Snapshot;
check_get_conflict(_GetResult, _Snapshot) -> false.

commit_reply(true) -> {error, aborted};
commit_reply(false) -> ok;
commit_reply(_) -> error.

create_object(Node, Bucket, Key, Value, Id, TentativeVersion) ->
    Object = riak_object:new(Node, Bucket, Key),
    Metadata1 = dict:store(<<"transaction_id">>, Id, dict:new()),
    Metadata = dict:store(<<"tentative_version">>, TentativeVersion, Metadata1),
    riak_object:set_contents(Object, [{Metadata, Value}]).

do_commit_transaction(Id, undefined, Gets, Puts, #state{clock = Clock} = State) ->
    do_commit_transaction(Id, Clock, Gets, Puts, State);

do_commit_transaction(_Id, Snapshot, _Gets, [], State) ->
    Reply = commit_reply(false),
    NewState1 = clean_transaction_state(State),
    NewState = NewState1#state{clock = Snapshot},
    {reply, Reply, NewState};

do_commit_transaction(Id, Snapshot, Gets, Puts, #state{client = {_, [Node, _]}} = State) ->
    proc_lib:spawn_link(Node, riak_kv_commit_transaction_fsm, start_link,
                        [Id, Snapshot, Gets, Puts, self()]),

    receive
        {ok, Conflicts, Lsn} ->
            Reply = commit_reply(Conflicts),
            NewState1 = clean_transaction_state(State),
            NewState = NewState1#state{clock = Lsn},
            {reply, Reply, NewState};
        {error, _Reason} = Reply ->
            {reply, Reply, State}
    end.
