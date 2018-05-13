-module(riak_kv_transactions_manager).

-behaviour(supervisor).

-export([start_link/1, validate_and_commit/7]).

-export([init/1]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(N) ->
    supervisor:start_link({global, {?MODULE, N}}, ?MODULE, N).

validate_and_commit(Idx, Id, Snapshot, Gets, Puts, NValidations, Client) ->
    {ok, NTransactionsManagers} = application:get_env(riak_kv, n_transactions_managers),
    N = Idx rem NTransactionsManagers,
    riak_kv_transactions_validator:validate(N, Id, Snapshot, Gets, Puts, NValidations, Client).

%%%===================================================================
%%% supervisor callbacks
%%%===================================================================

init(N) ->
    Validator = {riak_kv_transactions_validator,
                 {riak_kv_transactions_validator, start_link, [N]},
                 permanent, 5000, worker, [riak_kv_transactions_validator]},
    Log = {riak_kv_transactions_log,
           {riak_kv_transactions_log, start_link, [N]},
           permanent, 5000, worker, [riak_kv_transactions_log]},
    Committer = {riak_kv_transactions_committer,
                 {riak_kv_transactions_committer, start_link, [N]},
                 permanent, 5000, worker, [riak_kv_transactions_committer]},

    SupFlags = {one_for_one, 10, 10},
    ChildSpecs = [Validator, Log, Committer],

    {ok, {SupFlags, ChildSpecs}}.
