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
         run/4,
         terminate/2]).

-include_lib("eunit/include/eunit.hrl").

%% ====================================================================
%% API
%% ====================================================================
new(1) ->
    %% initializing only once
    InstanceName = basho_bench_config:get(instance_name, 'test_backend_db'),
    NumOfDBProcs = basho_bench_config:get(num_of_procs,  8),
    BackendDB    = basho_bench_config:get(backend_db,    'bitcask'),
    DBRootPath   = basho_bench_config:get(db_root_path,   "./db/"),
    leo_backend_db_api:new(InstanceName, NumOfDBProcs, BackendDB, DBRootPath),
    random:seed(),
    Ret = leo_backend_db_api:status(InstanceName),
    io:format(user, "[start]status:~p~n", [Ret]),
    {ok, InstanceName};
new(_) ->
    InstanceName = basho_bench_config:get(instance_name, 'test_backend_db'),
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
    end;

run(prefix_put, KeyGen, ValueGen, State) ->
    BinPrefix = gen_prefix(),
    BinKey = KeyGen(),
    Key = << BinPrefix/binary, $:, BinKey/binary >>,
    case leo_backend_db_api:put(State, Key, ValueGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;

run(prefix_get, _KeyGen, _ValueGen, State) ->
    TargetPrefix = gen_prefix(),
    Fun = fun(K, _V, Acc) ->
        [PrefixBin|_]= binary:split(K, <<$:>>),
        case PrefixBin of
            TargetPrefix ->
                [K|Acc];
            _ ->
                Acc
        end
    end,
    _List = leo_backend_db_api:fetch(State, TargetPrefix, Fun),
%%%    io:format(user, "[debug]prefix 1: list:~p~n", [List]),
    {ok, State}.

%% ${integer}_
gen_prefix() ->
    NumOfPrefix = basho_bench_config:get(num_of_prefix, 1000),
    IntPrefix = random:uniform(NumOfPrefix),
    integer_to_binary(IntPrefix).

% print status when finished benchmark
terminate(Reason, State) ->
    % debug output for prefix data
%%%    Fun = fun(K, _V, Acc) ->
%%%        [PrefixBin|_]= binary:split(K, <<$:>>),
%%%        case PrefixBin of
%%%            <<"1">> ->
%%%                [K|Acc];
%%%            _ ->
%%%                Acc
%%%        end
%%%    end,
%%%    List = leo_backend_db_api:fetch(State, <<"1:">>, Fun),
%%%    io:format(user, "[debug]prefix 1: list:~p~n", [List]),
    Ret = leo_backend_db_api:status(State),
    io:format(user, "[terminate]reason:~p status:~p~n", [Reason, Ret]),
    ok.
