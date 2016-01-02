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
%% Leo Backend DB - API
%% @doc leo_backend_db's API
%% @reference https://github.com/leo-project/leo_backend_db/blob/master/src/leo_backend_db_api.erl
%% @end
%%======================================================================
-module(leo_backend_db_api).

-include("leo_backend_db.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([new/4, put/3, get/2, delete/2, fetch/3, fetch/4, first/1,
         status/1,
         run_compaction/1, finish_compaction/2,
         put_value_to_new_db/3,
         get_db_raw_filepath/1,
         has_instance/1,
         stop/1
        ]).

-define(SERVER_MODULE, 'leo_backend_db_server').

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc create storage-processes.
%%
-spec(new(InstanceName, NumOfDBProcs, BackendDB, DBRootPath) ->
             ok | {error, any()} when InstanceName::atom(),
                                      NumOfDBProcs::pos_integer(),
                                      BackendDB::backend_db(),
                                      DBRootPath::string()).
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
-spec(stop(InstanceName) ->
             ok | {error, any()} when InstanceName::atom()).
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
-spec(put(InstanceName, KeyBin, ValueBin) ->
             ok | {error, any()} when InstanceName::atom(),
                                      KeyBin::binary(),
                                      ValueBin::binary()).
put(InstanceName, KeyBin, ValueBin) ->
    do_request(put, [InstanceName, KeyBin, ValueBin]).


%% @doc Retrieve an object from backend-db.
%%
-spec(get(InstanceName, KeyBin) ->
             {ok, binary()} |
             not_found |
             {error, any()} when InstanceName::atom(),
                                 KeyBin::binary()).
get(InstanceName, KeyBin) ->
    do_request(get, [InstanceName, KeyBin]).


%% @doc Delete an object from backend-db.
%%
-spec(delete(InstanceName, KeyBin) ->
             ok |
             {error, any()} when InstanceName::atom(),
                                 KeyBin::binary()).
delete(InstanceName, KeyBin) ->
    do_request(delete, [InstanceName, KeyBin]).


%% @doc Fetch objects from backend-db by key with function.
%%
-spec(fetch(InstanceName, KeyBin, Fun) ->
             {ok, list()} |
             not_found |
             {error, any()} when InstanceName::atom(),
                                 KeyBin::binary(),
                                 Fun::function()).
fetch(InstanceName, KeyBin, Fun) ->
    fetch(InstanceName, KeyBin, Fun, round(math:pow(2, 32))).

-spec(fetch(InstanceName, KeyBin, Fun, MaxKeys) ->
             {ok, list()} |
             not_found |
             {error, any()} when InstanceName::atom(),
                                 KeyBin::binary(),
                                 Fun::function(),
                                 MaxKeys::pos_integer()).
fetch(InstanceName, KeyBin, Fun, MaxKeys) ->
    case ets:lookup(?ETS_TABLE_NAME, InstanceName) of
        [] ->
            not_found;
        [{InstanceName, List}] ->
            case catch lists:foldl(
                         fun(Id, Acc) ->
                                 case ?SERVER_MODULE:fetch(Id, KeyBin, Fun, MaxKeys) of
                                     {ok, Ret} ->
                                         [Acc|Ret];
                                     not_found ->
                                         Acc;
                                     {error, Cause} ->
                                         erlang:error(Cause)
                                 end
                         end, [], List) of
                {'EXIT', Cause} ->
                    {error, Cause};
                RetL ->
                    fetch(lists:sublist(
                            lists:reverse(
                              lists:flatten(RetL)), MaxKeys))
            end
    end.
fetch([]) ->
    not_found;
fetch(Res) ->
    {ok, Res}.


%% @doc Retrieve a first record from backend-db.
%%
-spec(first(InstanceName) ->
             {ok, list()} | {error, any()} when InstanceName::atom()).
first(InstanceName) ->
    case ets:lookup(?ETS_TABLE_NAME, InstanceName) of
        [] ->
            not_found;
        [{InstanceName, RetL}] ->
            first_1(RetL, [])
    end.

%% @private
first_1([], []) ->
    not_found;
first_1([], Acc) ->
    Ret = erlang:hd(lists:sort(Acc)),
    {ok, Ret};
first_1([H|T], Acc) ->
    case ?SERVER_MODULE:first(H) of
        {ok, K, V} ->
            first_1(T, [{K, V}|Acc]);
        _Other ->
            first_1(T, Acc)
    end.


%% @doc Retrieve status from backend-db.
%%
-spec(status(InstanceName) ->
             [{atom(), term()}] when InstanceName::atom()).
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


%% @doc Start the data-compaction
-spec(run_compaction(InstanceName) ->
             ok | {error, any()} when InstanceName::atom()).
run_compaction(InstanceName) ->
    Pid = get_object_storage_pid(InstanceName),
    ?SERVER_MODULE:run_compaction(Pid).


%% @doc End the data-compaction
-spec(finish_compaction(InstanceName, Commit) ->
             ok | {error, any()} when InstanceName::atom(),
                                      Commit::boolean()).
finish_compaction(InstanceName, Commit) ->
    Pid = get_object_storage_pid(InstanceName),
    ?SERVER_MODULE:finish_compaction(Pid, Commit).


%% @doc Put a record to a new db
-spec(put_value_to_new_db(InstanceName, KeyBin, ValueBin) ->
             ok | {error, any()} when InstanceName::atom(),
                                      KeyBin::binary(),
                                      ValueBin::binary()).
put_value_to_new_db(InstanceName, KeyBin, ValueBin) ->
    Id = get_object_storage_pid(InstanceName),
    ?SERVER_MODULE:put_value_to_new_db(Id, KeyBin, ValueBin).


%% @doc get the database filepath for calculating disk size
-spec(get_db_raw_filepath(InstanceName) ->
             {ok, string()} | {error, any()} when InstanceName::atom()).
get_db_raw_filepath(InstanceName) ->
    %% invoke server method
    Id = get_object_storage_pid(InstanceName),
    ?SERVER_MODULE:get_db_raw_filepath(Id).


%% @doc has the instance into the db
-spec(has_instance(InstanceName) ->
             boolean() when InstanceName::atom()).
has_instance(InstanceName) ->
    case catch ets:lookup(?ETS_TABLE_NAME, InstanceName) of
        {'EXIT',_Cause} ->
            false;
        [] ->
            false;
        [{InstanceName,_List}] ->
            true
    end.


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc start object storage application.
%% @private
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
            {error, Cause}
    end.


%% @doc get an object storage process-id.
%% @private
-spec(get_object_storage_pid(atom()) ->
             atom()).
get_object_storage_pid(InstanceName) ->
    get_object_storage_pid(InstanceName, undefined).

-spec(get_object_storage_pid(atom(), undefined|binary()) ->
             atom()).
get_object_storage_pid(InstanceName, Arg) ->
    case ets:lookup(?ETS_TABLE_NAME, InstanceName) of
        [] ->
            undefined;
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
%% @private
-spec(do_request(type_of_methods(), list()) ->
             ok | {ok, binary()} | not_found | {error, any()}).
do_request(get, [InstanceName, KeyBin]) ->
    Id = get_object_storage_pid(InstanceName, KeyBin),
    ?SERVER_MODULE:get(Id, KeyBin);

do_request(put, [InstanceName, KeyBin, ValueBin]) ->
    Id = get_object_storage_pid(InstanceName, KeyBin),
    ?SERVER_MODULE:put(Id, KeyBin, ValueBin);

do_request(delete, [InstanceName, KeyBin]) ->
    Id = get_object_storage_pid(InstanceName, KeyBin),
    ?SERVER_MODULE:delete(Id, KeyBin).
