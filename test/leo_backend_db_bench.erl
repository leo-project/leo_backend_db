%%====================================================================
%%
%% Leo Backend DB
%%
%% Copyright (c) 2012-2016 Rakuten, Inc.
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
%%====================================================================
-module(leo_backend_db_bench).

-export([prepare/1,
         benchmark/3
        ]).

%% @doc Prepare a benchmark
-spec(prepare(NumOfKeys) ->
             ok when NumOfKeys::pos_integer()).
prepare(NumOfKeys) ->
    %% Prepare the Test
    DBInstanceName = 'test',
    Dir = "db/",
    leo_backend_db_api:new(DBInstanceName, 3, leveldb, Dir, false),

    %% PUT Test Values
    ok = put_test_values(NumOfKeys, DBInstanceName),

    %% Close the db
    ok = leo_backend_db_api:stop(DBInstanceName),
    application:stop(leo_backend_db),
    ok.

%% @doc Benchmark re-launch of the DBs
-spec(benchmark(Ver, DBInstanceName, Dir) ->
             {ok, Duration} when Ver::'1.1'|'1.2',
                                 DBInstanceName::atom(),
                                 Dir::string(),
                                 Duration::pos_integer()).
benchmark(Ver, DBInstanceName, Dir) ->
    _ = erlang:statistics(wall_clock),
    case Ver of
        '1.1' ->
            leo_backend_db_api:new(DBInstanceName, 3, leveldb, Dir);
        _ ->
            leo_backend_db_api:new(DBInstanceName, 3, leveldb, Dir, false)
    end,
    {_,Duration} = erlang:statistics(wall_clock),
    {ok, Duration}.


%% @private
put_test_values(0,_) ->
    ok;
put_test_values(Index, DBInstanceName) ->
    case (Index rem 1000 == 0) of
        true ->
            io:format("*:~w~n", [Index]);
        false ->
            void
    end,
    KeyBin = term_to_binary({key, Index}),
    ok = leo_backend_db_api:put(DBInstanceName, KeyBin, crypto:rand_bytes(128)),
    put_test_values(Index - 1, DBInstanceName).
