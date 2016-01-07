%%====================================================================
%%
%% Leo Backend DB
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% Leo Backend DB - TEST
%% @doc
%% @end
%%====================================================================
-module(leo_backend_db_api_tests).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_INSTANCE_NAME1, 'test_bitcask').
-define(TEST_INSTANCE_NAME2, 'test_leveldb').
-define(TEST_INSTANCE_NAME3, 'test_ets').
-define(NUM_OF_PROCS,       8).

-define(BACKEND_DB_BITCASK, 'bitcask').
-define(BACKEND_DB_LEVELDB, 'leveldb').
-define(BACKEND_DB_ETS, 'ets').

-define(PATH1, "./work/backenddb1").
-define(PATH2, "./work/backenddb2").
-define(PATH3, "./work/backenddb3").

-define(TEST_BUCKET_BIN, list_to_binary("air")).
-define(TEST_KEY_BIN, list_to_binary("air/on/g/string/music")).
-define(TEST_VAL_BIN, term_to_binary([{genre, "Classical"},{composer, "J.S.Bach"}])).
-define(TEST_KEY_BIN1, list_to_binary("air/on/g/string/music/1")).
-define(TEST_VAL_BIN1, term_to_binary([{genre, "Classical"},{composer, "J.S.Bach"}])).
-define(TEST_KEY_BIN2, list_to_binary("air/on/g/string/music/2")).
-define(TEST_VAL_BIN2, term_to_binary([{genre, "Classical"},{composer, "J.S.Bach"}])).
-define(TEST_KEY_BIN3, list_to_binary("air/on/g/string/music/3")).
-define(TEST_VAL_BIN3, term_to_binary([{genre, "Classical"},{composer, "J.S.Bach"}])).
-define(TEST_KEY_BIN4, list_to_binary("air/on/g/string/music/4")).
-define(TEST_VAL_BIN4, term_to_binary([{genre, "Classical"},{composer, "J.S.Bach"}])).
-define(TEST_KEY_BIN5, list_to_binary("air/on/g/string/music/5")).
-define(TEST_VAL_BIN5, term_to_binary([{genre, "Classical"},{composer, "J.S.Bach"}])).

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

backend_db_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun all_bitcask_/1,
                           fun all_eleveldb_/1,
                           fun all_ets_/1,
                           fun first_/1,
                           fun compact_/1
                          ]]}.

setup() ->
    ok.

teardown(_) ->
    catch application:stop(leo_backend_db),
    timer:sleep(200),
    os:cmd("rm -rf ./work"),
    os:cmd("rm -rf ./db"),
    meck:unload(),
    timer:sleep(1000),
    ok.

all_bitcask_(_) ->
    inspect(?TEST_INSTANCE_NAME1, ?BACKEND_DB_BITCASK, ?PATH1),
    ok.

all_eleveldb_(_) ->
    inspect(?TEST_INSTANCE_NAME2, ?BACKEND_DB_LEVELDB, ?PATH2),
    ok.

all_ets_(_) ->
    inspect(?TEST_INSTANCE_NAME3, ?BACKEND_DB_ETS, "test_table"),
    ok.

inspect(Instance, BackendDb, Path) ->
    ok = leo_backend_db_api:new(Instance, ?NUM_OF_PROCS, BackendDb, Path),
    true  = leo_backend_db_api:has_instance(Instance),
    false = leo_backend_db_api:has_instance('not_exist_instance'),

    %% #1
    ok = leo_backend_db_api:put(Instance, ?TEST_KEY_BIN, ?TEST_VAL_BIN),
    {ok, Res0} = leo_backend_db_api:get(Instance, ?TEST_KEY_BIN),
    ?assertEqual(?TEST_VAL_BIN, Res0),

    ok = leo_backend_db_api:delete(Instance, ?TEST_KEY_BIN),
    Res1 = leo_backend_db_api:get(Instance, ?TEST_KEY_BIN),
    ?assertEqual(not_found, Res1),


    %% #2 not-found.
    ?assertEqual(not_found, leo_backend_db_api:first(Instance)),

    Fun_1 = case BackendDb of
              ets ->
                  fun({K, V}, Acc) ->
                          [{K,V} | Acc]
                  end;
                _ ->
                  fun(K, V, Acc) ->
                          [{K,V} | Acc]
                  end
          end,
    ?assertEqual(not_found, leo_backend_db_api:fetch(Instance, ?TEST_KEY_BIN, Fun_1)),

    %% #3 [get, fetch, first, status]
    lists:foreach(fun({K,V}) ->
                          ok = leo_backend_db_api:put(Instance, K, V)
                  end, [{?TEST_KEY_BIN1, ?TEST_VAL_BIN1},
                        {?TEST_KEY_BIN2, ?TEST_VAL_BIN2},
                        {?TEST_KEY_BIN3, ?TEST_VAL_BIN3},
                        {?TEST_KEY_BIN4, ?TEST_VAL_BIN4},
                        {?TEST_KEY_BIN5, ?TEST_VAL_BIN5}
                       ]),

    lists:foreach(fun(K) ->
                          {ok, _} = leo_backend_db_api:get(Instance, K)
                  end, [?TEST_KEY_BIN1,?TEST_KEY_BIN2,?TEST_KEY_BIN3,?TEST_KEY_BIN4,?TEST_KEY_BIN5,
                        ?TEST_KEY_BIN1,?TEST_KEY_BIN2,?TEST_KEY_BIN3,?TEST_KEY_BIN4,?TEST_KEY_BIN5,
                        ?TEST_KEY_BIN1,?TEST_KEY_BIN2,?TEST_KEY_BIN3,?TEST_KEY_BIN4,?TEST_KEY_BIN5]),

    {ok, {_, _}} = leo_backend_db_api:first(Instance),
    Res4 = leo_backend_db_api:status(Instance),
    ?assertEqual(?NUM_OF_PROCS, length(Res4)),

    {ok, Res5} = leo_backend_db_api:fetch(Instance, ?TEST_KEY_BIN, Fun_1),
    ?assertEqual(5, length(Res5)),

    {ok, Res6} = leo_backend_db_api:fetch(Instance, ?TEST_KEY_BIN, Fun_1, 3),
    ?assertEqual(3, length(Res6)),

    case Instance of
        test_leveldb ->
            lists:foreach(fun({K,V}) ->
                                  ok = leo_backend_db_api:put(Instance, K, V)
                          end, [{term_to_binary({1,    "dir_1","dir_1/1"}), ?TEST_VAL_BIN1},
                                {term_to_binary({1024, "dir_1","dir_1/2"}), ?TEST_VAL_BIN2},
                                {term_to_binary({2048, "dir_1","dir_1/3"}), ?TEST_VAL_BIN3},
                                {term_to_binary({4096, "dir_1","dir_1/4"}), ?TEST_VAL_BIN4},
                                {term_to_binary({8192, "dir_1","dir_1/5"}), ?TEST_VAL_BIN5}
                               ]),

            Fun_2 = fun(K, V, Acc) ->
                            ?debugVal({leveldb, K, V, Acc}),
                            [{K,V} | Acc]
                    end,
            {ok, Res7} = leo_backend_db_api:fetch(Instance, <<131,104>>, Fun_2),
            ?assertEqual(5, length(Res7)),
            ok;
        _ ->
            void
    end,
    ok.


compact_(_) ->
    Id = ?TEST_INSTANCE_NAME2,
    Key = <<"key">>,
    Val = <<"val">>,

    ok = leo_backend_db_api:new(Id, 1, ?BACKEND_DB_BITCASK, ?PATH2),
    ok = leo_backend_db_api:put(Id, Key, Val),
    not_found = leo_backend_db_api:get(Id, <<"hoge">>),
    {ok, Val} = leo_backend_db_api:get(Id, Key),
    ok = leo_backend_db_api:delete(Id, Key),
    not_found = leo_backend_db_api:get(Id, Key),

    {ok, _Path} = leo_backend_db_api:get_db_raw_filepath(Id),

    %% Execute data-compaction depends on leo-object-storage
    timer:sleep(1000),
    ok = leo_backend_db_api:run_compaction(Id),
    not_found = leo_backend_db_api:get(Id, Key),
    ok = leo_backend_db_api:put(Id, Key, Val),
    ok = leo_backend_db_api:delete(Id, Key),

    ok = leo_backend_db_api:put_value_to_new_db(Id, Key, Val),
    ok = leo_backend_db_api:finish_compaction(Id, true),
    {ok, Val} = leo_backend_db_api:get(Id, Key),
    ok.

first_(_) ->
    Id = ?TEST_INSTANCE_NAME1,
    Key = <<"key">>,
    Val = <<"val">>,

    ok = leo_backend_db_api:new(Id, 1, ?BACKEND_DB_BITCASK, ?PATH3),
    TestData = [leo_backend_db_api:put(Id, <<Key/binary, Idx>>, Val) || Idx <- lists:seq($a, $z)],
    DelCount = delete_all(Id),
    ?assertEqual(DelCount, length(TestData)),
    ok.

delete_all(Id) ->
    delete_all(Id, leo_backend_db_api:first(Id), 0).

delete_all(_Id, not_found, Count) ->
    Count;
delete_all(Id, {ok, {K, _}}, Count) ->
    leo_backend_db_api:delete(Id, K),
    delete_all(Id, leo_backend_db_api:first(Id), Count + 1).

proper_test_() ->
    {timeout, 60000, ?_assertEqual([], proper:module(leo_backend_db_api_prop))}.

-endif.
