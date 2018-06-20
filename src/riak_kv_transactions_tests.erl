-module(riak_kv_transactions_tests).

-export([run/1]).

run(Vnodes) ->
    io:format("WARNING: the tests in this module assume a deploy scenario with 2 vnodes and a transactions manager tree with 3 nodes~n", []),
    blind_write_transaction(Vnodes),
    read_write_transaction(Vnodes),
    read_only_transaction(Vnodes),
    read_write_conflict_transaction_1(Vnodes),
    read_write_conflict_transaction_2(Vnodes),
    write_write_conflict_transaction(Vnodes),
    test_max_n_versions(Vnodes),
    ok.

% Initial state:
% Empty
blind_write_transaction([Vnode | _]) ->
    {ok, Client} = riak_kv_transactional_client:start_link(Vnode),
    ok = riak_kv_transactional_client:put(Vnode, <<"bucket">>, <<"a">>, 1, Client),
    timer:sleep(100).

% Initial state:
% a: 1 
read_write_transaction([Vnode1, Vnode2]) ->
    {ok, Client} = riak_kv_transactional_client:start_link(Vnode1),
    ok = riak_kv_transactional_client:begin_transaction(Client),
    {ok, Object} = riak_kv_transactional_client:get(Vnode1, <<"bucket">>, <<"a">>, Client),
    1 = riak_object:get_value(Object),
    ok = riak_kv_transactional_client:put(Vnode2,  <<"bucket">>, <<"b">>, 1, Client),
    ok = riak_kv_transactional_client:commit_transaction(Client),
    timer:sleep(100).

% Initial state:
% a: 1 
% b: 1
read_only_transaction([Vnode1, Vnode2]) ->
    {ok, Client1} = riak_kv_transactional_client:start_link(Vnode1),
    ok = riak_kv_transactional_client:begin_transaction(Client1),
    {ok, Object1} = riak_kv_transactional_client:get(Vnode1, <<"bucket">>, <<"a">>, Client1),
    1 = riak_object:get_value(Object1),
    {error, not_found} = riak_kv_transactional_client:get(Vnode2, <<"bucket">>, <<"b">>, Client1),
    ok = riak_kv_transactional_client:commit_transaction(Client1),

    {ok, Client2} = riak_kv_transactional_client:start_link(Vnode1),
    ok = riak_kv_transactional_client:begin_transaction(Client2),
    {ok, Object2} = riak_kv_transactional_client:get(Vnode2, <<"bucket">>, <<"b">>, Client2),
    1 = riak_object:get_value(Object2),
    {ok, Object1} = riak_kv_transactional_client:get(Vnode1, <<"bucket">>, <<"a">>, Client2),
    ok = riak_kv_transactional_client:commit_transaction(Client2).

% Initial state:
% a: 1 
% b: 1
read_write_conflict_transaction_1([Vnode1, Vnode2]) ->
    {ok, Client1} = riak_kv_transactional_client:start_link(Vnode1),
    {ok, Client2} = riak_kv_transactional_client:start_link(Vnode1),

    ok = riak_kv_transactional_client:begin_transaction(Client1),
    {ok, Object} = riak_kv_transactional_client:get(Vnode1, <<"bucket">>, <<"a">>, Client1),
    1 = riak_object:get_value(Object),

    ok = riak_kv_transactional_client:put(Vnode1,  <<"bucket">>, <<"a">>, 2, Client2),
    timer:sleep(100),

    ok = riak_kv_transactional_client:put(Vnode2,  <<"bucket">>, <<"b">>, 2, Client1),
    {error, aborted} = riak_kv_transactional_client:commit_transaction(Client1).

% Initial state:
% a: 1, 2
% b: 1
read_write_conflict_transaction_2([Vnode1, Vnode2]) ->
    {ok, Client1} = riak_kv_transactional_client:start_link(Vnode1),
    {ok, Client2} = riak_kv_transactional_client:start_link(Vnode1),

    ok = riak_kv_transactional_client:begin_transaction(Client1),
    {ok, Object} = riak_kv_transactional_client:get(Vnode1, <<"bucket">>, <<"a">>, Client1),
    2 = riak_object:get_value(Object),
    ok = riak_kv_transactional_client:put(Vnode2,  <<"bucket">>, <<"b">>, 2, Client1),

    ok = riak_kv_transactional_client:put(Vnode2,  <<"bucket">>, <<"c">>, 1, Client2),
    timer:sleep(100),

    {error, aborted} = riak_kv_transactional_client:get(Vnode2, <<"bucket">>, <<"c">>, Client1).

% Initial state:
% a: 1, 2
% b: 1
% c: 1
write_write_conflict_transaction([Vnode1, Vnode2]) ->
    {ok, Client1} = riak_kv_transactional_client:start_link(Vnode1),
    {ok, Client2} = riak_kv_transactional_client:start_link(Vnode1),

    ok = riak_kv_transactional_client:begin_transaction(Client1),
    {ok, Object} = riak_kv_transactional_client:get(Vnode2, <<"bucket">>, <<"b">>, Client1),
    1 = riak_object:get_value(Object),

    ok = riak_kv_transactional_client:put(Vnode2,  <<"bucket">>, <<"b">>, 2, Client2),
    timer:sleep(100),

    ok = riak_kv_transactional_client:put(Vnode2,  <<"bucket">>, <<"b">>, 3, Client1),
    {error, aborted} = riak_kv_transactional_client:commit_transaction(Client1).

% This test assumes riak_kv.maximum_object_versions = 5
% Initial state:
% a: 1, 2
% b: 1, 2
% c: 1
test_max_n_versions([Vnode | _]) ->
    {ok, Client1} = riak_kv_transactional_client:start_link(Vnode),
    {ok, Client2} = riak_kv_transactional_client:start_link(Vnode),

    ok = riak_kv_transactional_client:put(Vnode,  <<"bucket">>, <<"max_n_versions">>, 1, Client1),
    ok = riak_kv_transactional_client:put(Vnode,  <<"bucket">>, <<"a">>, 3, Client1),
    timer:sleep(100),

    ok = riak_kv_transactional_client:put(Vnode,  <<"bucket">>, <<"max_n_versions">>, 2, Client1),
    ok = riak_kv_transactional_client:put(Vnode,  <<"bucket">>, <<"max_n_versions">>, 3, Client1),
    ok = riak_kv_transactional_client:put(Vnode,  <<"bucket">>, <<"max_n_versions">>, 4, Client1),
    ok = riak_kv_transactional_client:put(Vnode,  <<"bucket">>, <<"max_n_versions">>, 5, Client1),
    ok = riak_kv_transactional_client:put(Vnode,  <<"bucket">>, <<"max_n_versions">>, 6, Client1),
    timer:sleep(100),

    riak_kv_transactional_client:begin_transaction(Client2),
    {ok, Object} = riak_kv_transactional_client:get(Vnode, <<"bucket">>, <<"a">>, Client2),
    3 = riak_object:get_value(Object),
    {error, not_found} = riak_kv_transactional_client:get(Vnode, <<"bucket">>, <<"max_n_versions">>, Client2),
    ok = riak_kv_transactional_client:commit_transaction(Client2).
