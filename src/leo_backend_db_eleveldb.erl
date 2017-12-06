%%======================================================================
%%
%% Leo Backend DB
%%
%% Copyright (c) 2012-2017 Rakuten, Inc.
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
-include_lib("leo_commons/include/leo_commons.hrl").
-include("leo_backend_db.hrl").

-export([open/1, open/2, close/1]).
-export([get/2, put/3, delete/2, prefix_search/4, first/1, first_n/3]).
-export([count/1]).
-export([status_compaction/1]).

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
    Options = [{fadvise_willneed, ?env_eleveldb_fadvise_willneed()},
               {write_buffer_size, ?env_eleveldb_write_buf_size()},
               {max_open_files, ?env_eleveldb_max_open_files()},
               {block_size, ?env_eleveldb_sst_block_size()}],
    open(Path, Options).

-spec(open(Path, Config) ->
             {error, any()} | {ok, pid()} when Path::string(),
                                               Config::[tuple()]).
open(Path, Config) ->
    Fadvise = leo_misc:get_value(fadvise_willneed, Config, ?DEF_ELEVELDB_FADVISE_WILLNEED),
    WriteBufferSize = leo_misc:get_value(write_buffer_size, Config, ?DEF_ELEVELDB_WRITE_BUF_SIZE),
    MaxOpenFiles = leo_misc:get_value(max_open_files, Config, ?DEF_ELEVELDB_MAX_OPEN_FILES),
    BlockSize = leo_misc:get_value(block_size, Config, ?DEF_ELEVELDB_SST_BLOCK_SIZE),
    Options = [{create_if_missing, true},
               {fadvise_willneed, Fadvise},
               {write_buffer_size, WriteBufferSize},
               {max_open_files, MaxOpenFiles},
               {sst_block_size, BlockSize}
              ],

    case leo_file:ensure_dir(Path) of
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

%% @doc Retrieve the compaction status
%%
-spec(status_compaction(Handler) ->
             binary() when Handler::eleveldb:db_ref()).
status_compaction(Handler) ->
    {ok, Ret} = eleveldb:status(Handler, <<"leveldb.stats">>),
    Ret.

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
        try
            Ret = eleveldb:iterator_move(Itr, Key),
            case fold_loop(Ret, Itr, Fun, [], Key, MaxKeys) of
                [] ->
                    not_found;
                Acc ->
                    {ok, lists:sort(Acc)}
            end
        after
            catch eleveldb:iterator_close(Itr)
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
            try
                case eleveldb:iterator_move(Itr, first)  of
                    {ok, K} ->
                        {ok, K};
                    {ok, K, V} ->
                        {ok, K, V};
                    {error, invalid_iterator} ->
                        not_found
                end
            after
                catch eleveldb:iterator_close(Itr)
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

%% @doc Fetch first N records from eleveldb.
%%
-spec(first_n(Handler, N, Condition) ->
             {ok, [_]} | not_found | {error, any()} when Handler::eleveldb:db_ref(),
                                                         N::pos_integer(),
                                                         Condition::function()).
first_n(Handler, N, Condition) ->
    %% Filter Function to fetch the first N records
    Fun = fun({K, V}, Acc0) ->
                  Acc = case Condition(K, V) of
                            true ->
                                [{K, V}|Acc0];
                            false ->
                                Acc0
                        end,
                  Done = (length(Acc) == N),
                  {Done, Acc}
          end,

    case catch fold(Handler, Fun, [], []) of
        {ok, []} ->
            not_found;
        {ok, List} ->
            {ok, lists:reverse(List)};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "first_n/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "first_n/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.

%% @doc Count the number of records from eleveldb.
%%
-spec(count(Handler) ->
             {ok, Count} | {error, any()} when Handler::eleveldb:db_ref(),
                                               Count::non_neg_integer()).
count(Handler) ->
    %% Function to count the number of records
    Fun = fun({_K, _V}, Acc) ->
                  {false, Acc + 1}
          end,
    case catch fold(Handler, Fun, 0, []) of
        {ok, Count} ->
            {ok, Count};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "count/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "count/1"},
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
fold_loop({error, iterator_closed},_Itr,_Fun, Acc,_Prefix,_MaxKeys) ->
    throw({iterator_closed, Acc});
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


%% @private
-type fold_fun() :: fun(({Key::binary(), Value::binary()}, any()) -> {Done::boolean(), any()}).

%% Fold over the keys and values in the database
%% until Fun(fold_fun) return {true, _} or the iterator reach EOF.
%% will throw an exception if the database is closed while the fold runs
-spec fold(eleveldb:db_ref(), fold_fun(), any(), eleveldb:fold_options()) -> {ok, any()}.
fold(Ref, Fun, Acc0, Opts) ->
    {ok, Itr} = eleveldb:iterator(Ref, Opts),
    do_fold(Itr, Fun, Acc0, Opts).

%% @private
do_fold(Itr, Fun, Acc0, Opts) ->
    try
        Start = proplists:get_value(first_key, Opts, first),
        true = is_binary(Start) or (Start == first),
        fold_loop(eleveldb:iterator_move(Itr, Start), Itr, Fun, Acc0)
    after
        catch eleveldb:iterator_close(Itr)
    end.

%% @private
fold_loop({error, iterator_closed}, _Itr, _Fun, Acc0) ->
    throw({iterator_closed, Acc0});
fold_loop({error, invalid_iterator}, _Itr, _Fun, Acc0) ->
    {ok, Acc0};
fold_loop({ok, K, V}, Itr, Fun, Acc0) ->
    {Done, Acc} = Fun({K, V}, Acc0),
    case Done of
        true ->
            {ok, Acc};
        false ->
            fold_loop(eleveldb:iterator_move(Itr, prefetch), Itr, Fun, Acc)
    end.
