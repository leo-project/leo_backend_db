%%======================================================================
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
%%======================================================================
-module(leo_backend_db_eleveldb).
-author('Yosuke Hara').

-include_lib("eunit/include/eunit.hrl").

-export([open/1, open/2, close/1, status/1]).
-export([get/2, put/3, delete/2, prefix_search/4, first/1]).

-define(DEF_MAX_OPEN_FILES,     32).
-define(DEF_CACHE_SIZE,   67108864). %% 64MB
-define(DEF_BLOCK_SIZE,      10240). %% 1KB * 10

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc open leveldb.
%%
-spec(open(string()) ->
             {error, any()} | {ok, pid()}).
open(Path) ->
    open(Path, []).

-spec(open(string(), list()) ->
             {error, any()} | {ok, pid()}).
open(Path, Config) ->
    WriteBufferMin  = leo_misc:get_value(write_buffer_size_min, Config,      256 * 1024),
    WriteBufferMax  = leo_misc:get_value(write_buffer_size_max, Config, 3 * 1024 * 1024),
    WriteBufferSize = WriteBufferMin + random:uniform(1 + WriteBufferMax - WriteBufferMin),
    MaxOpenFiles    = leo_misc:get_value(max_open_files,  Config, ?DEF_MAX_OPEN_FILES),
    CacheSize       = leo_misc:get_value(cache_size,      Config, ?DEF_CACHE_SIZE),
    BlockSize       = leo_misc:get_value(block_size,      Config, ?DEF_BLOCK_SIZE),
    Options = [{create_if_missing, true},
               {write_buffer_size, WriteBufferSize},
               {max_open_files,    MaxOpenFiles},
               {cache_size,        CacheSize},
               {block_size,        BlockSize}
              ],

    case filelib:ensure_dir(Path) of
        ok ->
            open1(Path, Options);
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "open/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.

%% @private
-spec(open1(string(), list()) ->
             {error, any()} | {ok, pid()}).
open1(Path, Options) ->
    case catch eleveldb:open(Path, Options) of
        {ok, Handler} ->
            {ok, Handler};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "open/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "open/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc close bitcask.
%%
-spec(close(pid()) -> ok).
close(Handler) ->
    catch eleveldb:close(Handler),
    ok.


%% @doc Get the status information for this eleveldb backend
-spec status(reference()) -> [{atom(), term()}].
status(Handler) ->
    {ok, Stats} = eleveldb:status(Handler, <<"leveldb.stats">>),
    [{stats, Stats}].


%% @doc Retrieve an object from the eleveldb.
%%
-spec(get(pid(), string()) ->
             {ok, binary()} | not_found | {error, any()}).
get(Handler, Key) ->
    case catch eleveldb:get(Handler, Key, []) of
        not_found ->
            not_found;
        {ok, Value} ->
            {ok, Value};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "get/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "get/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Insert an object into the eleveldb.
%%
-spec(put(pid(), binary(), binary()) ->
             ok | {error, any()}).
put(Handler, Key, Value) ->
    case catch eleveldb:put(Handler, Key, Value, []) of
        ok ->
            ok;
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "put/3"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "put/3"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Delete an object from the eleveldb.
%%
-spec(delete(pid(), binary()) ->
             ok | {error, any()}).
delete(Handler, Key) ->
    case catch eleveldb:delete(Handler, Key, []) of
        ok ->
            ok;
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "delete/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "delete/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Retrieve objects from eleveldb by a keyword.
%%
-spec(prefix_search(pid(), binary(), function(), integer()) ->
             {ok, list()} | not_found | {error, any()}).
prefix_search(Handler, Key, Fun, MaxKeys) ->
    case catch eleveldb:iterator(Handler, []) of
        {ok, Itr} ->
            case fold_loop(eleveldb:iterator_move(Itr, Key), Itr, Fun, [], Key, MaxKeys) of
                [] ->
                    not_found;
                Acc ->
                    {ok, lists:reverse(Acc)}
            end;
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "prefix_search/3"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "prefix_search/3"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Retrieve 'first' record from eleveldb.
%%
-spec(first(pid()) ->
             {ok, any()} | not_found | {error, any()}).
first(Handler) ->
    case catch eleveldb:iterator(Handler, []) of
        {ok, Itr} ->
            case eleveldb:iterator_move(Itr, first)  of
                {ok, K} ->
                    {ok, K};
                {ok, K, V} ->
                    {ok, K, V};
                {error, invalid_iterator} ->
                    not_found
            end;
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "first/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "first/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
fold_loop({error, invalid_iterator}, _Itr, _Fun, Acc0, _Prefix, _MaxKeys) ->
    Acc0;
fold_loop(_, _Itr, _Fun, Acc0, _Prefix, 0) ->
    Acc0;
fold_loop({ok, K}, Itr, Fun, Acc0, Prefix, MaxKeys) ->
    Size = size(Prefix),
    DstPrefix = binary:part(K, 0, Size),
    case DstPrefix of
        Prefix  ->
            Acc1 = Fun(K, [], Acc0),
            fold_loop(eleveldb:iterator_move(Itr, prefetch), Itr, Fun, Acc1, Prefix, MaxKeys - 1);
        _ ->
            Acc0
    end;
fold_loop({ok, K, V}, Itr, Fun, Acc0, Prefix, MaxKeys) ->
    Size = size(Prefix),
    DstPrefix = binary:part(K, 0, Size),
    case DstPrefix of
        Prefix ->
            Acc1 = Fun(K, V, Acc0),
            fold_loop(eleveldb:iterator_move(Itr, prefetch), Itr, Fun, Acc1, Prefix, MaxKeys - 1);
        _ ->
            Acc0
    end.
