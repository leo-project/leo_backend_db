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
%% Leo Backend DB - API
%% @doc
%% @end
%%======================================================================
-module(leo_backend_db_api).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-include("leo_backend_db.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([new/4, put/3, get/2, delete/2, fetch/3, first/1,
         status/1,
         compact_start/1, compact_put/3, compact_end/2,
         get_db_raw_filepath/1,
         stop/1
        ]).

-define(SERVER_MODULE, 'leo_backend_db_server').

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc create storage-processes.
%%
-spec(new(atom(), integer(), backend_db(), string()) ->
             ok | {error, any()}).
new(InstanceName, NumOfDBProcs, BackendDB, DBRootPath) ->
    case start_app() of
        ok ->
            leo_backend_db_sup:start_child(
              InstanceName, NumOfDBProcs, BackendDB, DBRootPath);
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Stop the instance
%%
-spec(stop(atom()) ->
             ok | {error, any()}).
stop(InstanceName) ->
    case ets:lookup(?ETS_TABLE_NAME, InstanceName) of
        [] ->
            {error, not_found};
        [{_, List}|_] ->
            true = ets:delete(?ETS_TABLE_NAME, InstanceName),
            lists:foreach(
              fun(Id) ->
                      catch leo_backend_db_server:stop(Id)
              end, List),
            ok
    end.


%% @doc Insert an object into backend-db.
%%
-spec(put(atom(), binary(), binary()) ->
             ok | {error, any()}).
put(InstanceName, KeyBin, ValueBin) ->
    do_request(put, [InstanceName, KeyBin, ValueBin]).


%% @doc Retrieve an object from backend-db.
%%
-spec(get(atom(), binary()) ->
             {ok, list()} | not_found | {error, any()}).
get(InstanceName, KeyBin) ->
    do_request(get, [InstanceName, KeyBin]).


%% @doc Delete an object from backend-db.
%%
-spec(delete(atom(), binary()) ->
             ok | {error, any()}).
delete(InstanceName, KeyBin) ->
    do_request(delete, [InstanceName, KeyBin]).


%% @doc Fetch objects from backend-db by key with function.
%%
-spec(fetch(atom(), binary(), function()) ->
             {ok, list()} | not_found | {error, any()}).
fetch(InstanceName, KeyBin, Fun) ->
    case ets:lookup(?ETS_TABLE_NAME, InstanceName) of
        [] ->
            not_found;
        [{InstanceName, List}] ->
            Res = lists:foldl(fun(Id, Acc) ->
                                      case ?SERVER_MODULE:fetch(Id, KeyBin, Fun) of
                                          {ok, Ret} -> [Acc|Ret];
                                          _Other    -> Acc
                                      end
                              end, [], List),
            fetch(lists:reverse(lists:flatten(Res)))
    end.
fetch([])  -> not_found;
fetch(Res) -> {ok, Res}.


%% @doc Retrieve a first record from backend-db.
%%
-spec(first(atom()) ->
             {ok, list()} | {error, any()}).
first(InstanceName) ->
    case ets:lookup(?ETS_TABLE_NAME, InstanceName) of
        [] ->
            not_found;
        [{InstanceName, List}] ->
            Res = lists:foldl(fun(Id, Acc) ->
                                      case ?SERVER_MODULE:first(Id) of
                                          {ok, K, V} -> [{K,V}|Acc];
                                          _Other     -> Acc
                                      end
                              end, [], List),
            first1(lists:reverse(Res))
    end.
first1([]) ->
    not_found;
first1(List) ->
    Index = erlang:phash2(List) rem length(List),
    {ok, lists:nth(Index+1, List)}.


%% @doc Retrieve status from backend-db.
%%
-spec(status(atom()) ->
             list()).
status(InstanceName) ->
    case ets:lookup(?ETS_TABLE_NAME, InstanceName) of
        [] ->
            not_found;
        [{InstanceName, List}] ->
            lists:foldl(fun(Id, Acc) ->
                                Res = ?SERVER_MODULE:status(Id),
                                [Res|Acc]
                        end, [], List)
    end.


%% @doc Retrieve a process status. running represents doing compaction, idle is not.
-spec(get_pid_status(atom()) -> running | idle ).
get_pid_status(Pid) ->
    case leo_misc:get_env(?APP_NAME, Pid) of
        undefined ->
            idle;
        {ok, Status} ->
            Status
    end.


%% @doc Direct to start a compaction. assume InstanceName has only one instance.
-spec(compact_start(atom()) ->
             ok | {error, any()}).
compact_start(InstanceName) ->
    Pid = get_object_storage_pid(InstanceName, none),
    case get_pid_status(Pid) of
        idle ->
            %% invoke server method
            ok = leo_misc:set_env(?APP_NAME, Pid, running),
            ?SERVER_MODULE:compact_start(Pid);
        running ->
            {error, invalid_compaction_status}
    end.


%% @doc Direct to end a compaction. assume InstanceName has only one instance.
-spec(compact_end(atom(), boolean()) ->
             ok | {error, any()}).
compact_end(InstanceName, Commit) ->
    Pid = get_object_storage_pid(InstanceName, none),
    case get_pid_status(Pid) of
        idle ->
            {error, invalid_compaction_status};
        running ->
            %% invoke server method
            ok = leo_misc:set_env(?APP_NAME, Pid, idle),
            ?SERVER_MODULE:compact_end(Pid, Commit)
    end.


%% @doc Direct to put a record to a temporary new data file. assume InstanceName has only one instance.
-spec(compact_put(atom(), KeyBin::binary(), ValueBin::binary()) ->
             ok | {error, any()}).
compact_put(InstanceName, KeyBin, ValueBin) ->
    Id = get_object_storage_pid(InstanceName, none),
    case get_pid_status(Id) of
        idle ->
            {error, invalid_compaction_status};
        running ->
            %% invoke server method
            ?SERVER_MODULE:compact_put(Id, KeyBin, ValueBin)
    end.


%% @doc get database file path for calculating disk size. assume InstanceName has only one instance.
-spec(get_db_raw_filepath(atom()) ->
             ok | {error, any()}).
get_db_raw_filepath(InstanceName) ->
    %% invoke server method
    Id = get_object_storage_pid(InstanceName, none),
    ?SERVER_MODULE:get_db_raw_filepath(Id).


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc start object storage application.
%%
-spec(start_app() ->
             ok | {error, any()}).
start_app() ->
    Module = leo_backend_db,
    case application:start(Module) of
        ok ->
            ok = leo_misc:init_env(),
            catch ets:new(?ETS_TABLE_NAME, [named_table, public, {read_concurrency, true}]),
            ok;
        {error, {already_started, Module}} ->
            ok;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "start_app/0"},
                                    {line, ?LINE}, {body, Cause}]),
            {exit, Cause}
    end.


%% @doc get an object storage process-id.
%%
-spec(get_object_storage_pid(atom(), any()) ->
             atom()).
get_object_storage_pid(InstanceName, Arg) ->
    case ets:lookup(?ETS_TABLE_NAME, InstanceName) of
        [] ->
            undefined;
        [{InstanceName, List}] when Arg == all ->
            lists:map(fun({Id, _, _}) ->
                              Id
                      end, List);
        [{InstanceName, [H|_] = List}] ->
            case erlang:length(List) of
                1 ->
                    H;
                _ ->
                    Index = (erlang:crc32(Arg) rem erlang:length(List)) + 1,
                    Id = lists:nth(Index, List),
                    Id
            end
    end.


%% @doc request an operation.
%%
-spec(do_request(type_of_methods(), list()) ->
             ok | {ok, list()} | {error, any()}).
do_request(get, [InstanceName, KeyBin]) ->
    Id = get_object_storage_pid(InstanceName, KeyBin),
    ?SERVER_MODULE:get(Id, KeyBin);

do_request(put, [InstanceName, KeyBin, ValueBin]) ->
    Id = get_object_storage_pid(InstanceName, KeyBin),
    case get_pid_status(Id) of
        idle ->
            ?SERVER_MODULE:put(Id, KeyBin, ValueBin);
        running ->
            {error, doing_compaction}
    end;

do_request(delete, [InstanceName, KeyBin]) ->
    Id = get_object_storage_pid(InstanceName, KeyBin),
    case get_pid_status(Id) of
        idle ->
            ?SERVER_MODULE:delete(Id, KeyBin);
        running ->
            {error, doing_compaction}
    end.

