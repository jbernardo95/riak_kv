-module(riak_kv_transactions_committer).

-behaviour(gen_server).

-export([start_link/1,
         commit/7,
         print_state/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {n, running_transactions}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(N) ->
    gen_server:start_link({global, {?MODULE, N}}, ?MODULE, N, []).

commit(N, Id, Puts, NValidations, Client, Conflicts, Lsn) ->
    gen_server:cast({global, {?MODULE, N}}, {commit, Id, Puts, NValidations, Client, Conflicts, Lsn}).

print_state(N) ->
    gen_server:cast({global, {?MODULE, N}}, print_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(N) ->
    State = #state{n = N, running_transactions = dict:new()},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({commit, Id, Puts, NValidations, Client, Conflicts, Lsn}, State) ->
    do_commit(Id, Puts, NValidations, Client, Conflicts, Lsn, State);

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

% Root committer
do_commit(Id, Puts, NValidations, Client, Conflicts, Lsn1,
          #state{n = root, running_transactions = RunningTransactions} = State) ->

    % Update transaction metadata
    Vnode = get_vnode(hd(Puts)),
    case dict:find(Id, RunningTransactions) of
        {ok, {ReceivedValidations1, VnodesPuts1, Lsn2}} ->
            NewVnodePuts = dict:update(Vnode, fun(Old) -> Old ++ Puts end, Puts, VnodesPuts1),
            NewRunningTransactions = dict:store(Id, {ReceivedValidations1 + 1, NewVnodePuts, max(Lsn1, Lsn2)}, RunningTransactions);
        error ->
            NewVnodePuts = dict:store(Vnode, Puts, dict:new()),
            NewRunningTransactions = dict:store(Id, {1, NewVnodePuts, Lsn1}, RunningTransactions)
    end,

    {ReceivedValidations, VnodePuts, Lsn} = dict:fetch(Id, NewRunningTransactions),
    case ReceivedValidations of
        NValidations ->
            send_validation_result_to_client(Id, Conflicts, Lsn, Client),
            send_validation_result_to_vnodes(Id, Conflicts, Lsn, VnodePuts),
            lager:info("Transaction ~p committed~n", [Id]),
            NewState = State#state{running_transactions = dict:erase(Id, NewRunningTransactions)};
        true ->
            NewState = State#state{running_transactions = NewRunningTransactions}
    end,

    {noreply, NewState};

% Leaf committer
% Transaction only needs one validation 
% So it can be committed right away 
do_commit(Id, Puts, 1 = _NValidations, Client, Conflicts, Lsn,
          #state{running_transactions = RunningTransactions} = State) ->

    send_validation_result_to_client(Id, Conflicts, Lsn, Client),

    Vnode = get_vnode(hd(Puts)),
    riak_kv_vnode:transaction_validation([Vnode], Id, Puts, Conflicts, Lsn),

    lager:info("Transaction ~p committed~n", [Id]),

    NewState = State#state{running_transactions = dict:erase(Id, RunningTransactions)},
    {noreply, NewState};

% Leaf committer
% Transaction needs more than one validation
% So it is sent to a committer a level up in the tree
% For now the transaction is sent to the root committer automatically
% In the future the routing code should be changed so that the transaction is sent to the correct committer
do_commit(Id, Puts, NValidations, Client, Conflicts, Lsn, State) ->
    riak_kv_transactions_committer:commit(root, Id, Puts, NValidations, Client, Conflicts, Lsn),
    {noreply, State}.

get_vnode(Bkey) ->
    DocIdx = riak_core_util:chash_key(Bkey),
    [Vnode] = riak_core_apl:get_apl(DocIdx, 1, riak_kv),
    Vnode.

send_validation_result_to_client(Id, Conflicts, Lsn, Client) ->
    Reply = {transaction_commit_result, Id, Conflicts, Lsn},
    riak_core_vnode:reply(Client, Reply).

send_validation_result_to_vnodes(Id, Conflicts, Lsn, VnodePuts) ->
    Vnodes = dict:fetch_keys(VnodePuts),
    lists:foreach(fun(Vnode) ->
                          Puts = dict:fetch(Vnode, VnodePuts),
                          riak_kv_vnode:transaction_validation([Vnode], Id, Puts, Conflicts, Lsn)
                  end, Vnodes).
