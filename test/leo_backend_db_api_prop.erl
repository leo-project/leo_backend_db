%%====================================================================
%%
%% Leo Backend DB
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
%% -------------------------------------------------------------------
%% Leo Backend DB - Property TEST
%% @doc
%% @end
%%====================================================================
-module(leo_backend_db_api_prop).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour('proper_statem').

-export([initial_state/0,
         command/1,
         precondition/2,
         next_state/3,
         postcondition/3]).


-type db_type() :: 'bitcask' | 'leveldb'.

-record(state, {stored = []  :: [string()],  %% list of objects stored in DB
                type         :: db_type()
               }).


-define(INSTANCE_NAME, 'backend_db').
-define(DB_PATH, "./db/temp").
-define(NUM_OF_PROCS, 3).
-define(KEYS, lists:map(fun(N) ->
                                list_to_binary(
                                  integer_to_list(N))
                        end, lists:seq(0, 9))).


%% @doc Utils
%%
key() ->
    frequency([{1, elements(?KEYS)}]).

value() ->
    frequency([{1, elements([<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>])}]).


%% @doc Property TEST
%%
prop_db() ->
    ?FORALL(Type, noshrink(db_type()),
            ?FORALL(Cmds, commands(?MODULE, initial_state(Type)),
                    begin
                        leo_backend_db_api:new(?INSTANCE_NAME, ?NUM_OF_PROCS, Type, ?DB_PATH),
                        {H,S,Res} = run_commands(?MODULE, Cmds),
                        application:stop(leo_backend_db),
                        os:cmd("rm -rf ./db"),

                        ?WHENFAIL(
                           io:format("History: ~p\nState: ~p\nRes: ~p\n", [H,S,Res]),
                           collect(Type, Res =:= ok))
                    end)).


%% @doc Initialize state
%%
initial_state() ->
    #state{}.
initial_state(Type) ->
    ?debugVal(Type),
    #state{type = Type}.


%% @doc Command
%%
command(_S) ->
    Cmd0 = [{call, leo_backend_db_api, put,    [?INSTANCE_NAME, key(), value()]},
            {call, leo_backend_db_api, get,    [?INSTANCE_NAME, key()]},
            {call, leo_backend_db_api, delete, [?INSTANCE_NAME, key()]},
            {call, leo_backend_db_api, first,  [?INSTANCE_NAME]}
           ],
    oneof(Cmd0).


%% @doc Pre-Condition
%%
precondition(_S, {call,_,_,_}) ->
    true.


%% @doc Next-State
%%
next_state(S, _V, {call,_,put,[_Instance, Key, _Object]}) ->
    case proplists:is_defined(Key, S#state.stored) of
        false ->
            S#state{stored = S#state.stored ++ [Key]};
        true ->
            S
    end;

next_state(S, _V, {call,_,delete,[_Instance, Key]}) ->
    S#state{stored=proplists:delete(Key, S#state.stored)};
next_state(S, _V, {call,_,_,_}) ->
    S.


%% @doc Post-Condition
%%
postcondition(_S,_V,_) ->
    true.
