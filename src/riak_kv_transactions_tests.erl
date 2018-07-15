-module(riak_kv_transactions_tests).

-export([run/1]).

run(Vnodes) ->
    io:format("WARNING~n", []),
    io:format("The tests in this module assume the following deploy scenario and configuration:~n", []),
    io:format("- 2 vnodes~n", []),
    io:format("- maximum_object_versions = 5~n", []),

    blind_write_transaction(Vnodes),
    read_write_transaction(Vnodes),
    read_only_transaction(Vnodes),
    read_write_conflict_transaction_1(Vnodes),
    read_write_conflict_transaction_2(Vnodes),
    write_write_conflict_transaction(Vnodes),
    test_maximum_object_versions(Vnodes),
    read_clock_increase(Vnodes),
    ok.

% Initial state:
% - Empty
% Final state:
% - a: (1, 1), (3, 2)
% - b: (2, 1), (4, 2)
blind_write_transaction([Vnode1, Vnode2]) ->
    {ok, Client} = riak_kv_transactional_client:start_link(Vnode1),
    ok = riak_kv_transactional_client:put(Vnode1, <<"bucket">>, <<"a">>, 1, Client),
    ok = riak_kv_transactional_client:put(Vnode1, <<"bucket">>, <<"a">>, 2, Client),
    ok = riak_kv_transactional_client:put(Vnode2, <<"bucket">>, <<"b">>, 1, Client),
    ok = riak_kv_transactional_client:put(Vnode2, <<"bucket">>, <<"b">>, 2, Client),
    timer:sleep(100).

% Initial state:
% - a: (1, 1), (3, 2)
% - b: (2, 1), (4, 2)
% Final state:
% - a: (1, 1), (3, 2), (5, 3)
% - b: (2, 1), (4, 2)
read_write_transaction([Vnode1, Vnode2]) ->
    {ok, Client} = riak_kv_transactional_client:start_link(Vnode1),
    ok = riak_kv_transactional_client:begin_transaction(Client),
    {ok, Object} = riak_kv_transactional_client:get(Vnode2, <<"bucket">>, <<"b">>, Client),
    2 = riak_object:get_value(Object),
    ok = riak_kv_transactional_client:put(Vnode1, <<"bucket">>, <<"a">>, 3, Client),
    ok = riak_kv_transactional_client:commit_transaction(Client),
    timer:sleep(100).

% Initial state:
% - a: (1, 1), (3, 2), (5, 3)
% - b: (2, 1), (4, 2)
% Final state:
% - a: (1, 1), (3, 2), (5, 3)
% - b: (2, 1), (4, 2)
read_only_transaction([Vnode1, Vnode2]) ->
    {ok, Client} = riak_kv_transactional_client:start_link(Vnode1),
    ok = riak_kv_transactional_client:begin_transaction(Client),
    {ok, Object1} = riak_kv_transactional_client:get(Vnode1, <<"bucket">>, <<"a">>, Client),
    3 = riak_object:get_value(Object1),
    {ok, Object2} = riak_kv_transactional_client:get(Vnode2, <<"bucket">>, <<"b">>, Client),
    2 = riak_object:get_value(Object2),
    ok = riak_kv_transactional_client:commit_transaction(Client).

% Initial state:
% - a: (1, 1), (3, 2), (5, 3)
% - b: (2, 1), (4, 2)
% Final state:
% - a: (1, 1), (3, 2), (5, 3), (7, 4)
% - b: (2, 1), (4, 2)
read_write_conflict_transaction_1([Vnode1, Vnode2]) ->
    {ok, Client1} = riak_kv_transactional_client:start_link(Vnode1),
    {ok, Client2} = riak_kv_transactional_client:start_link(Vnode1),

    ok = riak_kv_transactional_client:begin_transaction(Client1),
    {ok, Object} = riak_kv_transactional_client:get(Vnode1, <<"bucket">>, <<"a">>, Client1),
    3 = riak_object:get_value(Object),

    ok = riak_kv_transactional_client:put(Vnode1, <<"bucket">>, <<"a">>, 4, Client2),
    timer:sleep(100),

    ok = riak_kv_transactional_client:put(Vnode2,  <<"bucket">>, <<"b">>, 3, Client1),
    {error, aborted} = riak_kv_transactional_client:commit_transaction(Client1).

% Initial state:
% - a: (1, 1), (3, 2), (5, 3), (7, 4)
% - b: (2, 1), (4, 2)
% Final state:
% - a: (1, 1), (3, 2), (5, 3), (7, 4)
% - b: (2, 1), (4, 2)
% - c: (6, 1)
read_write_conflict_transaction_2([Vnode1, Vnode2]) ->
    {ok, Client1} = riak_kv_transactional_client:start_link(Vnode1),
    {ok, Client2} = riak_kv_transactional_client:start_link(Vnode1),

    ok = riak_kv_transactional_client:begin_transaction(Client1),
    {ok, Object} = riak_kv_transactional_client:get(Vnode2, <<"bucket">>, <<"b">>, Client1),
    2 = riak_object:get_value(Object),
    ok = riak_kv_transactional_client:put(Vnode1,  <<"bucket">>, <<"a">>, 5, Client1),

    ok = riak_kv_transactional_client:put(Vnode2,  <<"bucket">>, <<"c">>, 1, Client2),
    timer:sleep(100),

    {error, aborted} = riak_kv_transactional_client:get(Vnode2, <<"bucket">>, <<"c">>, Client1).

% Initial state:
% - a: (1, 1), (3, 2), (5, 3), (7, 4)
% - b: (2, 1), (4, 2)
% - c: (6, 1)
% Final state:
% - a: (1, 1), (3, 2), (5, 3), (7, 4), (9, 5)
% - b: (2, 1), (4, 2)
% - c: (6, 1)
write_write_conflict_transaction([Vnode | _]) ->
    {ok, Client1} = riak_kv_transactional_client:start_link(Vnode),
    {ok, Client2} = riak_kv_transactional_client:start_link(Vnode),

    ok = riak_kv_transactional_client:begin_transaction(Client1),
    {ok, Object} = riak_kv_transactional_client:get(Vnode, <<"bucket">>, <<"a">>, Client1),
    4 = riak_object:get_value(Object),

    ok = riak_kv_transactional_client:put(Vnode,  <<"bucket">>, <<"a">>, 5, Client2),
    timer:sleep(100),

    ok = riak_kv_transactional_client:put(Vnode,  <<"bucket">>, <<"a">>, 6, Client1),
    {error, aborted} = riak_kv_transactional_client:commit_transaction(Client1).

% This test assumes maximum_object_versions = 5
% Initial state:
% - a: (1, 1), (3, 2), (5, 3), (7, 4), (9, 5)
% - b: (2, 1), (4, 2)
% - c: (6, 1)
% Final state:
% - a: (3, 2), (5, 3), (7, 4), (9, 5), (13, 6)
% - b: (2, 1), (4, 2)
% - c: (6, 1)
% - maximum_object_versions: (15, 2), (17, 3), (19, 4), (21, 5), (23, 6)
test_maximum_object_versions([Vnode | _]) ->
    {ok, Client1} = riak_kv_transactional_client:start_link(Vnode),
    {ok, Client2} = riak_kv_transactional_client:start_link(Vnode),

    ok = riak_kv_transactional_client:put(Vnode, <<"bucket">>, <<"maximum_object_versions">>, 1, Client1),
    ok = riak_kv_transactional_client:put(Vnode, <<"bucket">>, <<"a">>, 6, Client1),
    timer:sleep(100),

    ok = riak_kv_transactional_client:put(Vnode, <<"bucket">>, <<"maximum_object_versions">>, 2, Client1),
    ok = riak_kv_transactional_client:put(Vnode, <<"bucket">>, <<"maximum_object_versions">>, 3, Client1),
    ok = riak_kv_transactional_client:put(Vnode, <<"bucket">>, <<"maximum_object_versions">>, 4, Client1),
    ok = riak_kv_transactional_client:put(Vnode, <<"bucket">>, <<"maximum_object_versions">>, 5, Client1),
    ok = riak_kv_transactional_client:put(Vnode, <<"bucket">>, <<"maximum_object_versions">>, 6, Client1),
    timer:sleep(100),

    riak_kv_transactional_client:begin_transaction(Client2),
    {ok, Object} = riak_kv_transactional_client:get(Vnode, <<"bucket">>, <<"a">>, Client2),
    6 = riak_object:get_value(Object),
    {error, not_found} = riak_kv_transactional_client:get(Vnode, <<"bucket">>, <<"maximum_object_versions">>, Client2),
    ok = riak_kv_transactional_client:commit_transaction(Client2).

% Initial state:
% - a: (3, 2), (5, 3), (7, 4), (9, 5), (13, 6)
% - b: (2, 1), (4, 2)
% - c: (6, 1)
% - maximum_object_versions: (15, 2), (17, 3), (19, 4), (21, 5), (23, 6)
% Final state:
% - a: (3, 2), (5, 3), (7, 4), (9, 5), (13, 6), (26, 7)
% - b: (2, 1), (4, 2), (8, 3)
% - c: (6, 1)
% - maximum_object_versions: (15, 2), (17, 3), (19, 4), (21, 5), (23, 6)
read_clock_increase([Vnode1, Vnode2]) ->
    {ok, Client1} = riak_kv_transactional_client:start_link(Vnode1),
    {ok, Client2} = riak_kv_transactional_client:start_link(Vnode1),

    riak_kv_transactional_client:begin_transaction(Client1),
    {ok, Object1} = riak_kv_transactional_client:get(Vnode1, <<"bucket">>, <<"maximum_object_versions">>, Client1),
    6 = riak_object:get_value(Object1),

    {ok, Object2} = riak_kv_transactional_client:get(Vnode2, <<"bucket">>, <<"b">>, Client1),
    2 = riak_object:get_value(Object2),

    ok = riak_kv_transactional_client:put(Vnode2, <<"bucket">>, <<"b">>, 3, Client2),
    timer:sleep(100),

    ok = riak_kv_transactional_client:put(Vnode1, <<"bucket">>, <<"a">>, 7, Client1),
    {error, aborted} = riak_kv_transactional_client:commit_transaction(Client1).
