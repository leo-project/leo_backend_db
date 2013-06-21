%%======================================================================
%%
%% Leo Bakcend DB
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
%% Leo Backend DB - Supervisor.
%% @doc
%% @end
%%======================================================================
-module(leo_backend_db_sup).

-author('Yosuke Hara').

-behaviour(supervisor).

-include("leo_backend_db.hrl").
-include_lib("eunit/include/eunit.hrl").

%% External API
-export([start_link/0,
         stop/0, stop/1,
         start_child/4, start_child/5]).

%% Callbacks
-export([init/1]).

%%-----------------------------------------------------------------------
%% API-1
%%-----------------------------------------------------------------------
%% @spec () -> ok
%% @doc start link.
%% @end
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%% @spec () -> ok |
%%             not_started
%% @doc stop process.
%% @end
stop() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) == true ->
            stop(Pid);
        _ ->
            not_started
    end.

stop(Pid) ->
    List = supervisor:which_children(Pid),
    Len  = length(List),

    ok = terminate_children(List),
    timer:sleep(Len * 100),
    exit(Pid, shutdown),
    ok.


%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
%% @spec (Params) -> ok
%% @doc stop process.
%% @end
%% @private
init([]) ->
    {ok, {{one_for_one, 5, 60}, []}}.


%%-----------------------------------------------------------------------
%% API-2
%%-----------------------------------------------------------------------
%%
%%
-spec(start_child(atom(), pos_integer(), atom(), string()) ->
             ok | true).
start_child(InstanceName, NumOfDBProcs, BackendDB, DBRootPath) ->
    start_child(?MODULE, InstanceName, NumOfDBProcs, BackendDB, DBRootPath).

-spec(start_child(supervisro:sup_ref(), atom(), pos_integer(), atom(), string()) ->
             ok | true).
start_child(SupRef0, InstanceName, NumOfDBProcs, BackendDB, DBRootPath) ->
    ok = leo_misc:init_env(),
    catch ets:new(?ETS_TABLE_NAME, [named_table, public, {read_concurrency, true}]),

    BackendMod = backend_mod(BackendDB),
    Fun = fun(DBNumber) ->
                  {Id, StrDBNumber} =
                      case (NumOfDBProcs == 1) of
                          true ->
                              {InstanceName, []};
                          false ->
                              NewDBNumber =  integer_to_list(DBNumber),
                              {list_to_atom(atom_to_list(InstanceName)
                                            ++ "_"
                                            ++  NewDBNumber), NewDBNumber}
                      end,

                  Path = DBRootPath ++ StrDBNumber,
                  Args = [Id, BackendMod, Path],
                  ChildSpec = {Id,
                               {leo_backend_db_server, start_link, Args},
                               permanent, 2000, worker, [leo_backend_db_server]},
                  case supervisor:start_child(SupRef0, ChildSpec) of
                      {ok, _Pid} when BackendDB == bitcask ->
                          ok = bitcask:merge(Path),
                          Id;
                      {ok, _Pid} ->
                          Id;
                      Cause ->
                          io:format("~w:~w - ~w ~p~n", [?MODULE, ?LINE, Id, Cause]),
                          []
                  end
          end,
    Ret = lists:map(Fun, lists:seq(0, NumOfDBProcs-1)),

    SupRef1 = case is_atom(SupRef0) of
                  true  -> whereis(SupRef0);
                  false -> SupRef0
              end,

    case supervisor:count_children(SupRef1) of
        [{specs,_},{active,Active},{supervisors,_},{workers,Workers}] when Active == Workers ->
            case ets:lookup(?ETS_TABLE_NAME, InstanceName) of
                [] ->
                    true = ets:insert(?ETS_TABLE_NAME, {InstanceName, Ret});
                [{InstanceName, List}|_] ->
                    true = ets:delete(?ETS_TABLE_NAME, InstanceName),
                    true = ets:insert(?ETS_TABLE_NAME, {InstanceName, List ++ Ret})
            end,
            ok;
        _ ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "new/4"},
                                    {line, ?LINE},
                                    {body, "Could NOT start worker processes"}]),
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
terminate_children([]) ->
    ok;
terminate_children([{Id,_Pid, worker, [Mod|_]}|T]) ->
    Mod:stop(Id),
    terminate_children(T);
terminate_children([_|T]) ->
    terminate_children(T).


%% @doc Retrieve a backend module name.
%% @private
-spec(backend_mod(backend_db()) ->
             atom()).
backend_mod(bitcask) ->
    leo_backend_db_bitcask;
%% backend_mod(leveldb) ->
%%     leo_backend_db_eleveldb;
backend_mod(ets) ->
    leo_backend_db_ets;
backend_mod(_) ->
    leo_backend_db_ets.
