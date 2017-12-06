%%======================================================================
%%
%% Leo Backend-DB
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
%% ---------------------------------------------------------------------
%% Leo Object Storage
%%
%%======================================================================

-define(ETS_TABLE_NAME, 'leo_backend_db_pd').
-define(APP_NAME, 'leo_backend_db').

-type(type_of_methods() :: put | get | delete | fetch).
-type(backend_db() :: bitcask | leveldb | ets).

-define(DEF_ELEVELDB_FADVISE_WILLNEED, false).
-define(DEF_ELEVELDB_WRITE_BUF_SIZE, 62914560).
-define(DEF_ELEVELDB_MAX_OPEN_FILES, 1000).
-define(DEF_ELEVELDB_SST_BLOCK_SIZE, 4096).

-define(env_eleveldb_fadvise_willneed(),
        case application:get_env(?APP_NAME,
                                 eleveldb_fadvise_willneed) of
            {ok, Fadvise} ->
                Fadvise;
            _ ->
                ?DEF_ELEVELDB_FADVISE_WILLNEED
        end).

-define(env_eleveldb_write_buf_size(),
        case application:get_env(?APP_NAME,
                                 eleveldb_write_buf_size) of
            {ok, WriteBufferSize} when is_integer(WriteBufferSize) ->
                WriteBufferSize;
            _ ->
                ?DEF_ELEVELDB_WRITE_BUF_SIZE
        end).

-define(env_eleveldb_max_open_files(),
        case application:get_env(?APP_NAME,
                                 eleveldb_max_open_files) of
            {ok, MaxOpenFiles} when is_integer(MaxOpenFiles) ->
                MaxOpenFiles;
            _ ->
                ?DEF_ELEVELDB_MAX_OPEN_FILES
        end).

-define(env_eleveldb_sst_block_size(),
        case application:get_env(?APP_NAME,
                                 eleveldb_sst_block_size) of
            {ok, SSTBlockSize} when is_integer(SSTBlockSize) ->
                SSTBlockSize;
            _ ->
                ?DEF_ELEVELDB_SST_BLOCK_SIZE
        end).

-define(get_new_count(_DBMod,_Handler,_Method,_Key,_Cnt,_IsStrictCheck),
        case _DBMod of
            'ets' ->
                _Cnt;
            _ when _IsStrictCheck == true ->
                case erlang:apply(_DBMod, get, [_Handler,_Key]) of
                    not_found ->
                        case _Method of
                            'put' ->
                                _Cnt + 1;
                            _ ->
                                _Cnt
                        end;
                    {ok,_} ->
                        case _Method of
                            'put' ->
                                _Cnt;
                            _ ->
                                _Cnt - 1
                        end;
                    _ ->
                        _Cnt
                end;
            _ ->
                _Cnt + 1
        end).
