%%======================================================================
%%
%% Leo Backend DB for RocksDB
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
%% @doc Handling database operation for erocksdb
%% @reference https://github.com/leo-project/leo_backend_db/blob/master/src/leo_backend_db_erocksdb.erl
%% @end
%%======================================================================
-module(leo_backend_db_erocksdb).
-author('Yosuke Hara').

-include_lib("eunit/include/eunit.hrl").

-export([open/1, open/2, close/1, status/1]).
-export([get/2, put/3, delete/2, prefix_search/4, first/1]).

-define(ETS_TBL_ROCKSDB, 'leo_rocksdb').
-define(ETS_COL_ROCKSDB_KEY_CNT, 'key_count').

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Open a new or existing rocksdb datastore
%%
-spec(open(Path) ->
             {error, any()} | {ok, pid()} when Path::string()).
open(Path) ->
    %% @TODO Set reasonable default here
    open(Path, []).

-spec(open(Path, CFOptions) ->
             {error, any()} | {ok, pid()} when Path::string(),
                                               CFOptions::[tuple()]).
open(Path, CFOptions) ->
    case filelib:ensure_dir(Path) of
        ok ->
            open_1(Path, CFOptions);
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
open_1(Path, CFOptions) ->
    DBOptions = [{create_if_missing, true},
                 {max_open_files, -1}
                ],
    case catch erocksdb:open(Path, DBOptions, CFOptions) of
        {ok, Handler} ->
            CurCount = erocksdb:fold_keys(Handler, fun(_K, Acc) ->
                                                           Acc + 1
                                                   end, 0, []),
            catch ets:new(?ETS_TBL_ROCKSDB,
                          [named_table, public, {write_concurrency, true}]),
            true = ets:insert(?ETS_TBL_ROCKSDB,
                              {{?ETS_COL_ROCKSDB_KEY_CNT,self()}, CurCount}),
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


%% @doc Close a rocksdb data store and flush any pending writes to disk
%%
-spec(close(Handler) ->
             ok when Handler::erocksdb:db_handle()).
close(Handler) ->
    catch erocksdb:close(Handler),
    ok.


%% @doc Get the status information for this erocksdb backend
-spec(status(Handler) ->
             [{atom(), term()}] when Handler::erocksdb:db_handle()).
status(_Handler) ->
    case ets:lookup(?ETS_TBL_ROCKSDB, {?ETS_COL_ROCKSDB_KEY_CNT, self()}) of
        [{{key_count,_Pid},Count}|_] ->
            [{key_count, Count}];
        _ ->
            [{key_count, 0}]
    end.


%% @doc Retrieve an object from the erocksdb.
%%
-spec(get(Handler, Key) ->
             {ok, binary()} |
             not_found |
             {error, any()} when Handler::erocksdb:db_handle(),
                                 Key::binary()).
get(Handler, Key) ->
    case catch erocksdb:get(Handler, Key, []) of
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


%% @doc Insert an object into the erocksdb.
%%
-spec(put(Handler, Key, Value) ->
             ok | {error, any()} when Handler::erocksdb:db_handle(),
                                      Key::binary(),
                                      Value::binary()).
put(Handler, Key, Value) ->
    case catch erocksdb:put(Handler, Key, Value, []) of
        ok ->
            catch ets:update_counter(
                    ?ETS_TBL_ROCKSDB, {?ETS_COL_ROCKSDB_KEY_CNT, self()}, 1),
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


%% @doc Delete an object from the erocksdb.
%%
-spec(delete(Handler, Key) ->
             ok | {error, any()} when Handler::erocksdb:db_handle(),
                                      Key::binary()).
delete(Handler, Key) ->
    case erocksdb:get(Handler, Key, []) of
        {ok,_} ->
            case catch erocksdb:delete(Handler, Key, []) of
                ok ->
                    catch ets:update_counter(
                            ?ETS_TBL_ROCKSDB,
                            {?ETS_COL_ROCKSDB_KEY_CNT, self()}, -1),
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


%% @doc Retrieve objects from erocksdb by a keyword.
%%
-spec(prefix_search(Handler, Key, Fun, MaxKeys) ->
             {ok, [_]} | not_found | {error, any()} when Handler::erocksdb:db_handle(),
                                                         Key::binary(),
                                                         Fun::fun(),
                                                         MaxKeys::integer()).
prefix_search(Handler, Key, Fun, MaxKeys) ->
    Ret = erocksdb:iterator(Handler, []),
    prefix_search_1(Ret, Key, Fun, MaxKeys).

prefix_search_1({error, Cause}, _Key, _Fun, _MaxKeys) ->
    error_logger:error_msg("~p,~p,~p,~p~n",
                           [{module, ?MODULE_STRING},
                            {function, "prefix_search/3"},
                            {line, ?LINE}, {body, Cause}]),
    {error, Cause};
prefix_search_1({ok, Itr}, Key, Fun, MaxKeys) ->
    try
        Ret = erocksdb:iterator_move(Itr, Key),
        case fold_loop(Ret, Itr, Fun, [], Key, MaxKeys) of
            [] ->
                not_found;
            Acc ->
                {ok, lists:reverse(Acc)}
        end
    catch
        _:Cause ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "prefix_search/3"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    after
        erocksdb:iterator_close(Itr)
    end.

%% @doc Retrieve a first record from erocksdb.
%%
-spec(first(Handler) ->
             {ok, any()} | not_found | {error, any()} when Handler::erocksdb:db_handle()).
first(Handler) ->
    case catch erocksdb:iterator(Handler, []) of
        {ok, Itr} ->
            try
                case erocksdb:iterator_move(Itr, first)  of
                    {ok, K} ->
                        {ok, K};
                    {ok, K, V} ->
                        {ok, K, V};
                    {error, invalid_iterator} ->
                        not_found
                end
            after
                erocksdb:iterator_close(Itr)
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
                any(), function(), [], binary(), integer()) ->
             [tuple()]).
fold_loop({ok, K}, Itr, Fun, Acc, Prefix, MaxKeys) ->
    KeySize    = byte_size(K),
    PrefixSize = byte_size(Prefix),
    fold_loop_1(K, [], Itr, Fun, Acc,
                Prefix, MaxKeys, KeySize, PrefixSize);

fold_loop({ok, K, V}, Itr, Fun, Acc, Prefix, MaxKeys) ->
    KeySize    = byte_size(K),
    PrefixSize = byte_size(Prefix),
    fold_loop_1(K, V, Itr, Fun, Acc,
                Prefix, MaxKeys, KeySize, PrefixSize);
fold_loop({error,_Reason},_Itr,_Fun, Acc,_Prefix,_MaxKeys) ->
    Acc.


%% @private
-spec(fold_loop_1(binary(),_,_,_,_,_,_,_,integer()) ->
             any()).
fold_loop_1(_K,_V, Itr, Fun, Acc, Prefix, MaxKeys, KeySize, PrefixSize)
  when KeySize =< PrefixSize ->
    fold_loop(erocksdb:iterator_move(Itr, next),
              Itr, Fun, Acc, Prefix, MaxKeys);

fold_loop_1(K, V, Itr, Fun, Acc, Prefix, MaxKeys,_KeySize, PrefixSize) ->
    DstPrefix = binary:part(K, 0, PrefixSize),
    case DstPrefix of
        Prefix ->
            Acc_1 = Fun(K, V, Acc),
            Ret = erocksdb:iterator_move(Itr, next),
            fold_loop(Ret, Itr, Fun, Acc_1, Prefix, MaxKeys - 1);
        _ ->
            Acc
    end.
