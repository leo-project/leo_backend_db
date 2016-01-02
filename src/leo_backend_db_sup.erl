%%======================================================================
%%
%% Leo Bakcend DB
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
%% Leo Backend DB - Supervisor.
%% @doc The supervisor of leo_backend_db
%% @reference https://github.com/leo-project/leo_backend_db/blob/master/src/leo_backend_db_sup.erl
%% @end
%%======================================================================
-module(leo_backend_db_sup).

-behaviour(supervisor).

-include("leo_backend_db.hrl").
-include_lib("eunit/include/eunit.hrl").

%% External API
-export([start_link/0,
         stop/0,
         start_child/4, start_child/5]).

%% Callbacks
-export([init/1]).

%%-----------------------------------------------------------------------
%% API-1
%%-----------------------------------------------------------------------
%% @doc Creates a supervisor process as part of a supervision tree
%% @end
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%% @doc Stop process
stop() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) ->
            List = supervisor:which_children(Pid),
            ok = close_db(List),
            ok;
        _ ->
            not_started
    end.


%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
%% @doc supervisor callback - Module:init(Args) -> Result
%% @end
init([]) ->
    {ok, {{one_for_one, 5, 60}, []}}.


%%-----------------------------------------------------------------------
%% API-2
%%-----------------------------------------------------------------------
%% @doc Creates the gen_server process as part of a supervision tree
%% @end
-spec(start_child(atom(), pos_integer(), backend_db(), string()) ->
             ok | no_return()).
start_child(InstanceName, NumOfDBProcs, BackendDB, DBRootPath) ->
    start_child(?MODULE, InstanceName, NumOfDBProcs, BackendDB, DBRootPath).

-spec(start_child(atom()|pid(), atom(), integer(), backend_db(), string()) ->
             ok | true).
start_child(SupRef, InstanceName, NumOfDBProcs, BackendDB, DBRootPath) ->
    ok = leo_misc:init_env(),
    catch ets:new(?ETS_TABLE_NAME, [named_table, public, {read_concurrency, true}]),
    start_child_1(SupRef, InstanceName, NumOfDBProcs - 1, (NumOfDBProcs == 1),
                  BackendDB, DBRootPath, []).

%% @private
start_child_1(_, InstanceName, -1,_,_,_, Acc) ->
    case ets:lookup(?ETS_TABLE_NAME, InstanceName) of
        [] ->
            true = ets:insert(?ETS_TABLE_NAME, {InstanceName, Acc});
        [{InstanceName, _List}|_] ->
            true = ets:delete(?ETS_TABLE_NAME, InstanceName),
            true = ets:insert(?ETS_TABLE_NAME, {InstanceName, Acc})
    end,
    ok;
start_child_1(SupRef, InstanceName, NumOfDBProcs, IsOneDevice, BackendDB, DBRootPath, Acc) ->
    BackendMod = backend_mod(BackendDB),
    {Id, StrDBNumber} =
        case IsOneDevice of
            true ->
                {InstanceName, []};
            false ->
                NewDBNumber =  integer_to_list(NumOfDBProcs),
                {list_to_atom(atom_to_list(InstanceName)
                              ++ "_"
                              ++  NewDBNumber), NewDBNumber}
        end,

    Path = DBRootPath ++ StrDBNumber,
    Args = [Id, BackendMod, Path],
    ChildSpec = {Id,
                 {leo_backend_db_server, start_link, Args},
                 permanent, 2000, worker, [leo_backend_db_server]},

    case supervisor:start_child(SupRef, ChildSpec) of
        {ok, _Pid} when BackendDB == bitcask ->
            ok = bitcask:merge(Path),
            start_child_1(SupRef, InstanceName, NumOfDBProcs - 1,
                          IsOneDevice, BackendDB, DBRootPath, [Id|Acc]);
        {ok, _Pid} ->
            start_child_1(SupRef, InstanceName, NumOfDBProcs - 1,
                          IsOneDevice, BackendDB, DBRootPath, [Id|Acc]);
        Cause ->
            io:format("~w:~w - ~w ~p~n", [?MODULE, ?LINE, Id, Cause]),
            case ?MODULE:stop() of
                ok ->
                    exit(invalid_launch);
                not_started ->
                    exit(noproc)
            end
    end.


%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
%% @doc Close databases
%% @private
-spec(close_db(list(tuple())) ->
             ok).
close_db([]) ->
    ok;
close_db([{Id,_Pid, worker, ['leo_backend_db_server' = Mod|_]}|T]) ->
    _ = Mod:close(Id),
    close_db(T);
close_db([_|T]) ->
    close_db(T).


%% @doc Retrieve a backend module name.
%% @private
-spec(backend_mod(backend_db()) ->
             atom()).
backend_mod(bitcask) ->
    leo_backend_db_bitcask;
backend_mod(leveldb) ->
    leo_backend_db_eleveldb;
backend_mod(rocksdb) ->
    leo_backend_db_erocksdb;
backend_mod(ets) ->
    leo_backend_db_ets;
backend_mod(_) ->
    leo_backend_db_ets.
