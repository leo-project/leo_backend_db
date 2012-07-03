%%======================================================================
%%
%% Leo Backend DB
%%
%% Copyright (c) 2012 Rakuten, Inc.
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
%% Leo Backend DB - bitcask
%% @doc
%% @end
%%======================================================================
-module(leo_backend_db_ets).
-author('Yosuke Hara').
-vsn('0.9.0').

-include_lib("eunit/include/eunit.hrl").

-export([open/1, open/2, close/1, status/1]).
-export([get/2, put/3, delete/2, prefix_search/3, first/1]).

%% @doc
%%
-spec(open(string()) ->
             {ok, pid()} | {error, any()}).
open(Table) ->
    open(Table, [named_table, public, {read_concurrency, true}]).

-spec(open(string() | atom(), list()) ->
             {ok, pid()} | {error, any()}).
open(Table, Option) when is_atom(Table) ->
    Table = ets:new(Table, Option),
    ok;
open(Table, Option) ->
    open(list_to_atom(Table), Option).


%% @doc close.
%%
-spec(close(pid()) -> ok).
close(_Table) ->
    ok.


%% @doc Get the status information for this ets.
-spec status(reference()) -> [{atom(), term()}].
status(Table) ->
    ets:info(Table).


%% @doc Retrieve an object from ets.
%%
-spec(get(pid(), binary()) ->
             not_found | {ok, binary()} | {error, any()}).
get(Table, Key) ->
    case catch ets:lookup(Table, Key) of
        [] ->
            not_found;
        [{_Key, Value}] ->
            {ok, Value};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "get/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Insert an object into ets.
%%
-spec(put(pid(), binary(), binary()) ->
             ok | {error, any()}).
put(Table, Key, Value) ->
    case catch ets:insert(Table, {Key, Value}) of
        true ->
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


%% @doc Delete an object from ets.
%%
-spec(delete(pid(), binary()) ->
             ok | {error, any()}).
delete(Table, Key) ->
    case get(Table, Key) of
        {ok, Value} ->
            case catch ets:delete_object(Table, {Key, Value}) of
                true ->
                    ok;
                {'EXIT', Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "delete/3"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end;
        Other ->
            Other
    end.


%% @doc Retrieve objects from ets by a keyword.
%%
-spec(prefix_search(pid(), binary(), function()) ->
             {ok, list()} | not_found | {error, any()}).
prefix_search(Table, _Key, Fun) ->
    fold1(catch ets:foldl(Fun, [], Table)).

fold1([])                      -> not_found;
fold1(List) when is_list(List) -> {ok, lists:reverse(List)};
fold1({'EXIT', Cause})         -> {error, Cause};
fold1({error, Cause})          -> {error, Cause};
fold1(_)                       -> {error, 'badarg'}.


%% @doc Retrieve first record from ets.
%%
-spec(first(pid()) ->
             {ok, any()} | not_found | {error, any()}).
first(Table) ->
    case catch ets:first(Table) of
        '$end_of_table' ->
            not_found;
        {'EXT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "first/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        Key ->
            case get(Table, Key) of
                {ok, Value} ->
                    {ok, Key, Value};
                Other ->
                    Other
            end
    end.

%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------

