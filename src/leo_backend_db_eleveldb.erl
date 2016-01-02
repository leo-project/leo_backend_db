%%======================================================================
%%
%% Leo Backend DB
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
%% @doc Handling database operation for eleveldb
%% @reference https://github.com/leo-project/leo_backend_db/blob/master/src/leo_backend_db_eleveldb.erl
%% @end
%%======================================================================
-module(leo_backend_db_eleveldb).

-include_lib("eunit/include/eunit.hrl").

-export([open/1, open/2, close/1, status/1]).
-export([get/2, put/3, delete/2, prefix_search/4, first/1]).

-define(DEF_MAX_OPEN_FILES, 32).
-define(DEF_CACHE_SIZE, 67108864). %% 64MB
-define(DEF_BLOCK_SIZE, 10240). %% 1KB * 10
-define(ETS_TBL_LEVELDB, 'leo_eleveldb').
-define(ETS_COL_LEVELDB_KEY_CNT, 'key_count').

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Open a new or existing leveldb datastore for read-only access
%%
-spec(open(Path) ->
             {error, any()} | {ok, pid()} when Path::string()).
open(Path) ->
    open(Path, []).

-spec(open(Path, Config) ->
             {error, any()} | {ok, pid()} when Path::string(),
                                               Config::[tuple()]).
open(Path, Config) ->
    WriteBufferMin = leo_misc:get_value(write_buffer_size_min, Config, 256 * 1024),
    WriteBufferMax = leo_misc:get_value(write_buffer_size_max, Config, 3 * 1024 * 1024),
    WriteBufferSize = WriteBufferMin + random:uniform(1 + WriteBufferMax - WriteBufferMin),
    MaxOpenFiles = leo_misc:get_value(max_open_files, Config, ?DEF_MAX_OPEN_FILES),
    CacheSize = leo_misc:get_value(cache_size, Config, ?DEF_CACHE_SIZE),
    BlockSize = leo_misc:get_value(block_size, Config, ?DEF_BLOCK_SIZE),
    Options = [{create_if_missing, true},
               {write_buffer_size, WriteBufferSize},
               {max_open_files, MaxOpenFiles},
               {cache_size, CacheSize},
               {block_size, BlockSize}
              ],

    case filelib:ensure_dir(Path) of
        ok ->
            open_1(Path, Options);
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "open/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.

%% @private
-spec(open_1(string(), list()) ->
             {error, any()} | {ok, pid()}).
open_1(Path, Options) ->
    case catch eleveldb:open(Path, Options) of
        {ok, Handler} ->
            CurCount = eleveldb:fold_keys(Handler, fun(_K, Acc) ->
                                                           Acc + 1
                                                   end, 0, []),
            catch ets:new(?ETS_TBL_LEVELDB,
                          [named_table, public, {write_concurrency, true}]),
            true = ets:insert(?ETS_TBL_LEVELDB,
                              {{?ETS_COL_LEVELDB_KEY_CNT,self()}, CurCount}),
            {ok, Handler};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "open/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "open/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Close a leveldb data store and flush any pending writes to disk
%%
-spec(close(Handler) ->
             ok when Handler::eleveldb:db_ref()).
close(Handler) ->
    catch eleveldb:close(Handler),
    ok.


%% @doc Get the status information for this eleveldb backend
-spec(status(Handler) ->
             [{atom(), term()}] when Handler::eleveldb:db_ref()).
status(_Handler) ->
    case ets:lookup(?ETS_TBL_LEVELDB, {?ETS_COL_LEVELDB_KEY_CNT, self()}) of
        [{{key_count,_Pid},Count}|_] ->
            [{key_count, Count}];
        _ ->
            [{key_count, 0}]
    end.


%% @doc Retrieve an object from the eleveldb.
%%
-spec(get(Handler, Key) ->
             {ok, binary()} |
             not_found |
             {error, any()} when Handler::eleveldb:db_ref(),
                                 Key::binary()).
get(Handler, Key) ->
    case catch eleveldb:get(Handler, Key, []) of
        not_found ->
            not_found;
        {ok, Value} ->
            {ok, Value};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "get/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "get/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Insert an object into the eleveldb.
%%
-spec(put(Handler, Key, Value) ->
             ok | {error, any()} when Handler::eleveldb:db_ref(),
                                      Key::binary(),
                                      Value::binary()).
put(Handler, Key, Value) ->
    case catch eleveldb:put(Handler, Key, Value, []) of
        ok ->
            catch ets:update_counter(
                    ?ETS_TBL_LEVELDB, {?ETS_COL_LEVELDB_KEY_CNT, self()}, 1),
            ok;
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "put/3"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "put/3"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Delete an object from the eleveldb.
%%
-spec(delete(Handler, Key) ->
             ok | {error, any()} when Handler::eleveldb:db_ref(),
                                      Key::binary()).
delete(Handler, Key) ->
    case ?MODULE:get(Handler, Key) of
        {ok,_} ->
            case catch eleveldb:delete(Handler, Key, []) of
                ok ->
                    catch ets:update_counter(
                            ?ETS_TBL_LEVELDB,
                            {?ETS_COL_LEVELDB_KEY_CNT, self()}, -1),
                    ok;
                {'EXIT', Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "delete/2"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause};
                {error, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "delete/2"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end;
        not_found ->
            ok;
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Retrieve objects from eleveldb by a keyword.
%%
-spec(prefix_search(Handler, Key, Fun, MaxKeys) ->
             {ok, [_]} | not_found | {error, any()} when Handler::eleveldb:db_ref(),
                                                         Key::binary(),
                                                         Fun::fun(),
                                                         MaxKeys::integer()).
prefix_search(Handler, Key, Fun, MaxKeys) ->
    try
        {ok, Itr} = eleveldb:iterator(Handler, []),
        Ret = eleveldb:iterator_move(Itr, Key),
        case fold_loop(Ret, Itr, Fun, [], Key, MaxKeys) of
            [] ->
                not_found;
            Acc ->
                {ok, lists:sort(Acc)}
        end
    catch
        _:Cause ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "prefix_search/3"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Retrieve a first record from eleveldb.
%%
-spec(first(Handler) ->
             {ok, any()} | not_found | {error, any()} when Handler::eleveldb:db_ref()).
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
                                   [{module, ?MODULE_STRING},
                                    {function, "first/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "first/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Retrieve object of values
%% @private
-spec(fold_loop({ok, binary(), binary()} |
                {ok, binary()} |
                {error, invalid_iterator|iterator_closed},
                eleveldb:itr_ref(), function(), [], binary(), integer()) ->
             [tuple()]).
fold_loop(_,_Itr,_Fun, Acc,_Prefix, MaxKeys) when MaxKeys =< length(Acc) ->
    Acc;
fold_loop({ok, K}, Itr, Fun, Acc, Prefix, MaxKeys) ->
    KeySize    = byte_size(K),
    PrefixSize = byte_size(Prefix),
    fold_loop_1(K, <<>>, Itr, Fun, Acc,
                Prefix, MaxKeys, KeySize, PrefixSize);
fold_loop({ok, K, V}, Itr, Fun, Acc, Prefix, MaxKeys) ->
    KeySize    = byte_size(K),
    PrefixSize = byte_size(Prefix),
    fold_loop_1(K, V, Itr, Fun, Acc,
                Prefix, MaxKeys, KeySize, PrefixSize);
fold_loop({error,_},_Itr,_Fun, Acc,_Prefix,_MaxKeys) ->
    Acc.


%% @private
-spec(fold_loop_1(binary(),_,_,_,_,_,_,_,integer()) ->
             any()).
fold_loop_1(_K,_V, Itr, Fun, Acc, Prefix, MaxKeys, KeySize, PrefixSize)
  when KeySize =< PrefixSize ->
    Ret = eleveldb:iterator_move(Itr, next),
    fold_loop(Ret, Itr, Fun, Acc, Prefix, MaxKeys);

fold_loop_1(K, V, Itr, Fun, Acc, Prefix, MaxKeys,_KeySize, PrefixSize) ->
    DstPrefix = binary:part(K, 0, PrefixSize),
    case DstPrefix of
        Prefix ->
            Acc_1 = Fun(K, V, Acc),
            Ret = eleveldb:iterator_move(Itr, next),
            fold_loop(Ret, Itr, Fun, Acc_1, Prefix, MaxKeys);
        _ ->
            Acc
    end.
