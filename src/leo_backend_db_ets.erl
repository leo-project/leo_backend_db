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
%% Leo Backend DB - ETS
%% @doc Handling database operation for ETS
%% @reference https://github.com/leo-project/leo_backend_db/blob/master/src/leo_backend_db_ets.erl
%% @end
%%======================================================================
-module(leo_backend_db_ets).

-include_lib("eunit/include/eunit.hrl").

-export([open/1, open/2, close/1, status/1]).
-export([get/2, put/3, delete/2, prefix_search/4, first/1]).

%% @doc Open a new ets datastore
%%
-spec(open(Table) ->
             ok when Table::atom() | string()).
open(Table) ->
    open(Table, [named_table, public, {read_concurrency, true}]).

-spec(open(Table, Option) ->
             ok when Table::atom() | string(),
                     Option::[atom()]).
open(Table, Option) when is_atom(Table) ->
    Table = ets:new(Table, Option),
    ok;
open(Table, Option) ->
    open(list_to_atom(Table), Option).


%% @doc Close a ets data store
%%
-spec(close(Table) ->
             ok when Table::atom()).
close(_Table) ->
    ok.


%% @doc Get the status information for this ets.
-spec(status(Table) ->
             [any()] | undefined when Table::atom()).
status(Table) ->
    ets:info(Table).


%% @doc Retrieve an object from the ets.
%%
-spec(get(Table, Key) ->
             not_found |
             {ok, binary()} |
             {error, any()} when Table::atom(),
                                 Key::binary()).
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
-spec(put(Table, Key, Value) ->
             ok | {error, any()} when Table::pid(),
                                      Key::binary(),
                                      Value::binary()).
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


%% @doc Delete an object from the ets
%%
-spec(delete(Table, Key) ->
             ok | not_found | {error, any()} when Table::atom(),
                                                  Key::binary()).
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
-spec(prefix_search(Table, Key, Fun, MaxKeys) ->
             {ok, list()} |
             not_found |
             {error, any()} when Table::pid(),
                                 Key::binary(),
                                 Fun::function(),
                                 MaxKeys::pos_integer()).
prefix_search(Table, _Key, Fun, MaxKeys) ->
    fold1(catch ets:foldl(Fun, [], Table), MaxKeys).

fold1([], _)                      -> not_found;
fold1(List, MaxKeys) when is_list(List) ->
    {ok, lists:sublist(lists:reverse(List), MaxKeys)};
fold1({'EXIT', Cause}, _)         -> {error, Cause};
fold1({error, Cause}, _)          -> {error, Cause};
fold1(_, _)                       -> {error, 'badarg'}.


%% @doc Retrieve a first record from the ets
%%
-spec(first(Table) ->
             {ok, any()} |
             not_found |
             {error, any()} when Table::pid()).
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
