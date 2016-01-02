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
%% ---------------------------------------------------------------------
%% Leo Backend DB  - bitcask
%% @doc Handling database operation for bitcask
%% @reference https://github.com/leo-project/leo_backend_db/blob/master/src/leo_backend_db_bitcask.erl
%% @end
%%======================================================================
-module(leo_backend_db_bitcask).

-include_lib("eunit/include/eunit.hrl").
-include_lib("bitcask/include/bitcask.hrl").

-export([open/1, open/2, close/1, status/1]).
-export([get/2, put/3, delete/2, prefix_search/4, first/1]).

%% @doc Open a new or existing bitcask datastore for read-only access
%%
-spec(open(Path) ->
             {ok, pid()} | {error, any()} when Path::string()).
open(Path) ->
    open(Path, [read_write]).

-spec(open(Path, Options) ->
             {ok, pid()} | {error, any()} when Path::string(), Options::list()).
open(Path, Options) ->
    Flags = case application:get_env('leo_backend_db', 'bitcask_flags') of
                {ok, V} -> V;
                _ -> []
            end,
    case catch bitcask:open(Path, Options ++ Flags) of
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "open/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "open/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        Handler ->
            io:format("* bitcask - path:~s, handle:~p~n", [Path, Handler]),
            {ok, Handler}
    end.


%% @doc Close a bitcask data store and flush any pending writes to disk
%%
-spec(close(Handler) ->
             ok when Handler::reference()).
close(Handler) ->
    bitcask:close(Handler).


%% @doc Retrive the status information for this bitcask backend
-spec(status(Handler) ->
                    [{atom(), term()}] when Handler::reference()).
status(Handler) ->
    {KeyCount, Status} = bitcask:status(Handler),
    [{key_count, KeyCount}, {status, Status}].


%% @doc Retrieve an object from bitcask.
%%
-spec(get(Handler, Key) ->
             not_found |
             {ok, binary()} |
             {error, any()} when Handler::reference(),
                                 Key::binary()).
get(Handler, Key) ->
    case catch bitcask:get(Handler, Key) of
        {ok, Value} ->
            {ok, Value};
        not_found ->
            not_found;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "get/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "get/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Insert an object into bitcask.
%%
-spec(put(Handler, Key, Value) ->
             ok | {error, any()} when Handler::reference(),
                                      Key::binary(),
                                      Value::binary()).
put(Handler, Key, Value) ->
    case catch bitcask:put(Handler, Key, Value) of
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


%% @doc Delete an object from bitcask.
%%
-spec(delete(Handler, Key) ->
             ok | {error, any()} when Handler::reference(),
                                      Key::binary()).
delete(Handler, Key) ->
    case catch bitcask:delete(Handler, Key) of
        ok ->
            ok;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "delete/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "delete/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Retrieve an objects from bitcask by the keyword and the function
%%
-spec(prefix_search(Handler, Key, Fun, MaxKeys) ->
             {ok, [any()]} |
             not_found |
             {error, any()} when Handler::reference(),
                                 Key::binary(),
                                 Fun::function(),
                                 MaxKeys::pos_integer()).
prefix_search(Handler, _Key, Fun, MaxKeys) ->
    fold(fold, Handler, Fun, MaxKeys).


%% @doc Retrieve a first record from bitcask
%%
-spec(first(Handler) ->
             {ok, any()} |
             not_found |
             {error, any()} when Handler::reference()).
first(Handler) ->
    ok = bitcask:iterator(Handler, 0, 0),
    try
        case bitcask:iterator_next(Handler) of
            #bitcask_entry{ key = Key } ->
                {ok, Val} = get(Handler, Key),
                {ok, Key, Val};
            not_found ->
                not_found;
            Error ->
                {error, Error}
        end
    after
        bitcask:iterator_release(Handler)
    end.

%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
%% @private
-spec(fold(fold, reference(), function(), pos_integer()) ->
             {ok, any()} | not_found | {error, any()}).
fold(Mode, Handler, Fun, MaxKeys) ->
    fold_1(Mode, catch bitcask:fold(Handler, Fun, []), MaxKeys).

%% @private
fold_1(_, [],_) ->
    not_found;
fold_1(fold,  List, MaxKeys) when is_list(List) ->
    {ok, lists:sublist(lists:sort(List), MaxKeys)};
fold_1(_, {'EXIT', Cause},_) ->
    {error, Cause};
fold_1(_, {error, Cause},_) ->
    {error, Cause};
fold_1(_,_,_) ->
    {error, 'badarg'}.
