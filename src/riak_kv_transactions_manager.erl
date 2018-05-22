-module(riak_kv_transactions_manager).

-behaviour(supervisor).

-export([start_link/1, validate_and_commit/7]).

-export([init/1]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    supervisor:start_link({global, {?MODULE, Id}}, ?MODULE, Id).

validate_and_commit(HashedIdx, TransactionId, Snapshot, Gets, Puts, NValidations, Client) ->
    {ok, NLeafTransactionsManagers} = application:get_env(riak_kv, n_leaf_transactions_managers),
    TransactionsManagerId = HashedIdx rem NLeafTransactionsManagers,
    riak_kv_transactions_validator:validate(TransactionsManagerId, TransactionId, Snapshot, Gets, Puts, NValidations, Client).

%%%===================================================================
%%% supervisor callbacks
%%%===================================================================

init(Id) ->
    Validator = {riak_kv_transactions_validator,
                 {riak_kv_transactions_validator, start_link, [Id]},
                 permanent, 5000, worker, [riak_kv_transactions_validator]},
    Log = {riak_kv_transactions_log,
           {riak_kv_transactions_log, start_link, [Id]},
           permanent, 5000, worker, [riak_kv_transactions_log]},
    Committer = {riak_kv_transactions_committer,
                 {riak_kv_transactions_committer, start_link, [Id]},
                 permanent, 5000, worker, [riak_kv_transactions_committer]},

    SupFlags = {one_for_one, 10, 10},
    ChildSpecs = [Validator, Log, Committer],

    {ok, {SupFlags, ChildSpecs}}.
