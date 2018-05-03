-module(riak_kv_transactions_committer).

-behaviour(gen_server).

-export([start_link/0,
         process_record/2,
         print_state/0]).

-export([code_change/3,
         handle_call/3,
         handle_cast/2,
	     handle_info/2,
	     init/1,
	     terminate/2]).

-include("riak_kv_log.hrl").

-define(MESSAGE_QUEUE_LENGTH_THRESHOLD, 100000).

-record(state, {next_lsn}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

process_record(Lsn, Record) ->
    gen_server:cast({global, ?MODULE}, {process_record, Lsn, Record}).

print_state() ->
    gen_server:cast({global, ?MODULE}, print_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_Args) ->
    State = #state{next_lsn = 1},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({process_record, Lsn, Record}, #state{next_lsn = Lsn} = State) ->
    verify_message_queue_length(),
    NewState = do_process_record(Record, State),
    {noreply, NewState};

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

verify_message_queue_length() ->
    {message_queue_len, MessageQueueLength} = process_info(self(), message_queue_len),
    if
        MessageQueueLength > ?MESSAGE_QUEUE_LENGTH_THRESHOLD ->
            lager:critical("Transactions committer message queue length is past the threshold, currently at ~p~n", [MessageQueueLength]);
        true ->
            ok
    end.

do_process_record(
  #log_record{type = transaction_commit,
              payload = {Id, _Snapshot, _Gets, Puts}} = _Record,
  #state{next_lsn = Lsn} = State
) ->
    %lager:info("Processing log record #~p ~p~n", [Lsn, Record]),

    commit_transaction(Id, Puts, Lsn),

    State#state{next_lsn = Lsn + 1};

do_process_record(_Record, #state{next_lsn = Lsn} = State) ->
    State#state{next_lsn = Lsn + 1}.

commit_transaction(Id, Puts, Lsn) ->
    % Groups puts by vnode
    PutsDict = lists:foldl(fun(Object, PutsDict1) ->
                                   Vnode = get_vnode(Object),
                                   case dict:find(Vnode, PutsDict1) of
                                       {ok, Objects} ->
                                           dict:store(Vnode, [Objects | Objects], PutsDict1);
                                       error ->
                                           dict:store(Vnode, [Object], PutsDict1)
                                   end
                           end, dict:new(), Puts),

    % Send commit message to vnodes
    lists:foreach(fun(Vnode) ->
                          Puts = dict:fetch(Vnode, PutsDict),
                          riak_kv_vnode:commit_transaction([Vnode], Id, Puts, Lsn)
                  end, dict:fetch_keys(PutsDict)).

get_vnode(Object) ->
    Bkey = riak_object:bkey(Object),
    DocIdx = riak_core_util:chash_key(Bkey),
    [Vnode] = riak_core_apl:get_apl(DocIdx, 1, riak_kv),
    Vnode.
