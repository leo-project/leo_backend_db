%% -------------------------------------------------------------------
%%
%% Leo Backend DB - Benchmarking Suite
%%
%% Copyright (c) 2012-2013 Rakuten, Inc.
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
-module(basho_bench_driver_leo_backend_db).
-author("Yosuke Hara").

-export([new/1,
         run/4]).

-include_lib("eunit/include/eunit.hrl").

%% ====================================================================
%% API
%% ====================================================================
new(_Id) ->
    InstanceName = basho_bench_config:get(instance_name, 'test_backend_db'),
    NumOfDBProcs = basho_bench_config:get(num_of_procs,  8),
    BackendDB    = basho_bench_config:get(backend_db,    'bitcask'),
    DBRootPath   = basho_bench_config:get(db_root_path,   "./db/"),
    ?debugVal({InstanceName, NumOfDBProcs, BackendDB, DBRootPath}),

    leo_backend_db_api:new(InstanceName, NumOfDBProcs, BackendDB, DBRootPath),
    {ok, InstanceName}.

run(get, KeyGen, _ValueGen, State) ->
    case leo_backend_db_api:get(State, KeyGen()) of
        {ok, _Value} ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;

run(put, KeyGen, ValueGen, State) ->
    case leo_backend_db_api:put(State, KeyGen(), ValueGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

