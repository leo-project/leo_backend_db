%%======================================================================
%%
%% Leo Backend DB
%%
%% Copyright (c) 2014 Rakuten, Inc.
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
%% @doc
%% @end
%%======================================================================
-module(leo_backend_db_bitcask).
-author('Yosuke Hara').

-include_lib("eunit/include/eunit.hrl").

-export([open/1, open/2, close/1, status/1]).
-export([get/2, put/3, delete/2, prefix_search/4, first/1]).

%% @doc open bitcask.
%%
-spec(open(string()) ->
             {ok, pid()} | {error, any()}).
open(Path) ->
    open(Path, [read_write]).

-spec(open(string(), list()) ->
             {ok, pid()} | {error, any()}).
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


%% @doc close bitcask.
%%
-spec(close(pid()) -> ok).
close(Handler) ->
    bitcask:close(Handler).


%% @doc Get the status information for this bitcask backend
-spec status(reference()) -> [{atom(), term()}].
status(Handler) ->
    {KeyCount, Status} = bitcask:status(Handler),
    [{key_count, KeyCount}, {status, Status}].


%% @doc Retrieve an object from bitcask.
%%
-spec(get(pid(), binary()) ->
             not_found | {ok, binary()} | {error, any()}).
get(Handler, Key) ->
    case catch bitcask:get(Handler, Key) of
        {ok, Value} ->
            {ok, Value};
        not_found ->
            not_found;
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "get/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Insert an object into bitcask.
%%
-spec(put(pid(), binary(), binary()) ->
             ok | {error, any()}).
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
-spec(delete(pid(), binary()) ->
             ok | {error, any()}).
delete(Handler, Key) ->
    case catch bitcask:delete(Handler, Key) of
        ok ->
            ok;
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "delete/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Retrieve objects from bitcask by a keyword.
%%
-spec(prefix_search(pid(), binary(), function(), integer()) ->
             {ok, list()} | not_found | {error, any()}).
prefix_search(Handler, _Key, Fun, MaxKeys) ->
    fold(fold, Handler, Fun, MaxKeys).


%% @doc Retrieve first record from bitcask
%%
-spec(first(pid()) ->
             {ok, any()} | not_found | {error, any()}).
first(Handler) ->
    fold(first, Handler, fun(K, V, Acc0) ->
                                 case Acc0 of
                                     [] -> [{K, V} | Acc0];
                                     _  -> Acc0
                                 end
                         end, 1).


%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
-spec(fold(first|fold, pid(), function(), integer()) ->
             {ok, any()} | not_found | {error, any()}).
fold(Mode, Handler, Fun, MaxKeys) ->
    fold1(Mode, catch bitcask:fold(Handler, Fun, []), MaxKeys).

fold1(_, [], _)                                -> not_found;
fold1(first, [{K, V}|_], _)                    -> {ok, K, V};
fold1(fold,  List, MaxKeys) when is_list(List) -> {ok, lists:sublist(lists:reverse(List), MaxKeys)};
fold1(_, {'EXIT', Cause}, _)                   -> {error, Cause};
fold1(_, {error, Cause}, _)                    -> {error, Cause};
fold1(_, _, _)                                 -> {error, 'badarg'}.
