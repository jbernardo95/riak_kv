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

-define(RUNNING_TRANSACTIONS, running_transactions).

-record(state, {id}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    gen_server:start_link({global, {?MODULE, Id}}, ?MODULE, Id, []).

commit(Id, TransactionId, Puts, NValidations, Client, Conflicts, Lsn) ->
    gen_server:cast({global, {?MODULE, Id}}, {commit, TransactionId, Puts, NValidations, Client, Conflicts, Lsn}).

print_state(Id) ->
    gen_server:cast({global, {?MODULE, Id}}, print_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Id) ->
    ets:new(?RUNNING_TRANSACTIONS, [private, named_table]), 

    State = #state{id = Id},
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request received at hanlde_call: ~p~n", [Request]),
    {reply, error, State}.

handle_cast({commit, TransactionId, Puts, NValidations, Client, Conflicts, Lsn}, State) ->
    do_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn, State);

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
do_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn, #state{id = Id} = State) ->
    {ok, NLeafTransactionsManagers} = application:get_env(riak_kv, n_leaf_transactions_managers),

    if
        Id < NLeafTransactionsManagers ->
            leaf_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn);
        true ->
            root_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn)
    end,

    {noreply, State}.

% Root committer
root_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn1) ->
    % Update transaction metadata
    Vnode = get_vnode(hd(Puts)),
    case ets:lookup(?RUNNING_TRANSACTIONS, TransactionId) of
        [{TransactionId, ReceivedValidations1, VnodesPuts1, Lsn2}] ->
            NewVnodePuts = dict:update(Vnode, fun(Old) -> Old ++ Puts end, Puts, VnodesPuts1),
            ets:insert(?RUNNING_TRANSACTIONS, {TransactionId, ReceivedValidations1 + 1, NewVnodePuts, max(Lsn1, Lsn2)});
        [] ->
            NewVnodePuts = dict:store(Vnode, Puts, dict:new()),
            ets:insert(?RUNNING_TRANSACTIONS, {TransactionId, 1, NewVnodePuts, Lsn1})
    end,

    [{ReceivedValidations, VnodePuts, Lsn}] = ets:lookup(?RUNNING_TRANSACTIONS, TransactionId),
    case ReceivedValidations of
        NValidations ->
            send_validation_result_to_client(TransactionId, Conflicts, Lsn, Client),
            send_validation_result_to_vnodes(TransactionId, Conflicts, Lsn, VnodePuts),
            lager:info("Transaction ~p committed~n", [TransactionId]),
            ets:delete(?RUNNING_TRANSACTIONS, TransactionId);
        _ ->
            ok
    end.

% Leaf committer
% Transaction only needs one validation 
% So it can be committed right away 
leaf_commit(TransactionId, Puts, 1 = _NValidations, Client, Conflicts, Lsn) ->
    send_validation_result_to_client(TransactionId, Conflicts, Lsn, Client),

    Vnode = get_vnode(hd(Puts)),
    riak_kv_vnode:transaction_validation([Vnode], TransactionId, Puts, Conflicts, Lsn),

    lager:info("Transaction ~p committed~n", [TransactionId]),

    ets:delete(?RUNNING_TRANSACTIONS, TransactionId);

% Leaf committer
% Transaction needs more than one validation
% So it is sent to a committer a level up in the tree
% For now the transaction is sent to the root committer automatically
% In the future the routing code should be changed so that the transaction is sent to the correct committer
leaf_commit(TransactionId, Puts, NValidations, Client, Conflicts, Lsn) ->
    {ok, Root} = application:get_env(riak_kv, n_leaf_transactions_managers),
    riak_kv_transactions_committer:commit(Root, TransactionId, Puts, NValidations, Client, Conflicts, Lsn).

get_vnode(Bkey) ->
    DocIdx = riak_core_util:chash_key(Bkey),
    [Vnode] = riak_core_apl:get_apl(DocIdx, 1, riak_kv),
    Vnode.

send_validation_result_to_client(TransactionId, Conflicts, Lsn, Client) ->
    Reply = {transaction_commit_result, TransactionId, Conflicts, Lsn},
    riak_core_vnode:reply(Client, Reply).

send_validation_result_to_vnodes(TransactionId, Conflicts, Lsn, VnodePuts) ->
    Vnodes = dict:fetch_keys(VnodePuts),
    lists:foreach(fun(Vnode) ->
                          Puts = dict:fetch(Vnode, VnodePuts),
                          riak_kv_vnode:transaction_validation([Vnode], TransactionId, Puts, Conflicts, Lsn)
                  end, Vnodes).
