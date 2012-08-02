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
%% Leo Bakcend DB - Server
%% @doc
%% @end
%%======================================================================
-module(leo_backend_db_server).
-author('Yosuke Hara').

-behaviour(gen_server).

-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/3,
         stop/1]).

%% data operations.
-export([put/3,
         get/2,
         delete/2,
         fetch/3,
         first/1,
         status/1
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         compact_start/1, compact_put/3, compact_end/2,
         get_db_raw_filepath/1
        ]).

-record(state, {db           :: atom(),
                path         :: string(),
                raw_path     :: string(),
                tmp_raw_path :: string(),
                tmp_handler  :: pid(),
                handler      :: pid()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link(Id, DBModule, Path) ->
    gen_server:start_link({local, Id}, ?MODULE, [DBModule, Path], []).

stop(Id) ->
    gen_server:call(Id, stop).


%%--------------------------------------------------------------------
%% Data Operation related.
%%--------------------------------------------------------------------
%% @doc Insert an object into backend-db.
%%
-spec(put(Id::atom(), KeyBin::binary(), ValueBin::binary()) ->
             ok | {error, any()}).
put(Id, KeyBin, ValueBin) ->
    gen_server:call(Id, {put, KeyBin, ValueBin}).


%% @doc Retrieve an object from backend-db.
%%
-spec(get(Id::atom(), KeyBin::binary()) ->
             {ok, any()} | {error, any()}).
get(Id, KeyBin) ->
    gen_server:call(Id, {get, KeyBin}).


%% @doc Delete an object from backend-db.
%%
-spec(delete(Id::atom(), KeyBin::binary()) ->
             ok | {error, any()}).
delete(Id, KeyBin) ->
    gen_server:call(Id, {delete, KeyBin}).


%% @doc Fetch records from backend-db.
%%
-spec(fetch(Id::atom(), KeyBin::binary(), Fun::function()) ->
             {ok, list()} | not_found | {error, any()}).
fetch(Id, KeyBin, Fun) ->
    gen_server:call(Id, {fetch, KeyBin, Fun}).


%% @doc Retrieve a first record from backend-db.
%%
-spec(first(atom()) ->
             {ok, list()} | {error, any()}).
first(Id) ->
    gen_server:call(Id, {first}).


%% Retrieve status from backend-db.
%%
-spec(status(atom()) ->
             list()).
status(Id) ->
    gen_server:call(Id, {status}).


%% @doc Direct to start a compaction.
%%
-spec(compact_start(atom()) ->
             ok | {error, any()}).
compact_start(Id) ->
    gen_server:call(Id, {compact_start}).


%% @doc Direct to end a compaction.
%%
-spec(compact_end(atom(), boolean()) ->
             ok | {error, any()}).
compact_end(Id, Commit) ->
    gen_server:call(Id, {compact_end, Commit}).


%% @doc Direct to put a record to a temporary new data file.
%%
-spec(compact_put(atom(), KeyBin::binary(), ValueBin::binary()) ->
             ok | {error, any()}).
compact_put(Id, KeyBin, ValueBin) ->
    gen_server:call(Id, {compact_put, KeyBin, ValueBin}).


%% @doc get database file path for calculating disk size.
%%
-spec(get_db_raw_filepath(atom()) ->
             ok | {error, any()}).
get_db_raw_filepath(Id) ->
    gen_server:call(Id, {get_db_raw_filepath}).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([leo_backend_db_ets = DBModule, Table]) ->
    ok = DBModule:open(Table),
    {ok, #state{db       = DBModule,
                handler  = list_to_atom(Table)}};

init([DBModule, Path0]) ->
    {ok, Curr} = file:get_cwd(),
    Path1 = case Path0 of
                "/"   ++ _Rest -> Path0;
                "../" ++ _Rest -> Path0;
                "./"  ++  Rest -> Curr ++ "/" ++ Rest;
                _              -> Curr ++ "/" ++ Path0
            end,

    case get_raw_path(Path1) of
        {ok, RawPath} ->
            case DBModule:open(Path0) of
                {ok, Handler} ->
                    {ok, #state{db       = DBModule,
                                path     = Path1,
                                raw_path = RawPath,
                                handler  = Handler}};
                {error, Cause} ->
                    {stop, Cause}
            end;
        {error, Cause} ->
            {stop, Cause}
    end.

handle_call(stop, _From, #state{db = DBModule,
                                handler = Handler} = State) ->
    erlang:apply(DBModule, close, [Handler]),
    {stop, normal, ok, State};


%%--------------------------------------------------------------------
%% Data Operation related.
%%--------------------------------------------------------------------
handle_call({put, KeyBin, ValueBin}, _From, #state{db      = DBModule,
                                                   handler = Handler} = State) ->
    Reply = erlang:apply(DBModule, put, [Handler, KeyBin, ValueBin]),
    {reply, Reply, State};


handle_call({get, KeyBin}, _From, #state{db      = DBModule,
                                         handler = Handler} = State) ->
    Reply = erlang:apply(DBModule, get, [Handler, KeyBin]),
    {reply, Reply, State};


handle_call({delete, KeyBin}, _From, #state{db      = DBModule,
                                            handler = Handler} = State) ->
    Reply = erlang:apply(DBModule, delete, [Handler, KeyBin]),
    {reply, Reply, State};


handle_call({fetch, KeyBin, Fun}, _From, #state{db      = DBModule,
                                                handler = Handler} = State) ->
    Reply = erlang:apply(DBModule, prefix_search, [Handler, KeyBin, Fun]),
    {reply, Reply, State};


handle_call({first}, _From, #state{db      = DBModule,
                                   handler = Handler} = State) ->
    Reply = erlang:apply(DBModule, first, [Handler]),
    {reply, Reply, State};


handle_call({status}, _From, #state{db      = DBModule,
                                    handler = Handler} = State) ->
    Reply = erlang:apply(DBModule, status, [Handler]),
    {reply, Reply, State};


handle_call({compact_start}, _From, #state{db   = DBModule,
                                           path = Path} = State) ->
    NewPath = gen_file_raw_path(Path),
    case filelib:ensure_dir(NewPath) of
        ok ->
            case DBModule:open(NewPath) of
                {ok, NewHandler} ->
                    NewState = State#state{
                                 tmp_raw_path     = NewPath,
                                 tmp_handler      = NewHandler},
                    {reply, ok, NewState};
                {error, Reason} ->
                    {stop, Reason, State}
            end;
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_call({compact_put, KeyBin, ValueBin}, _From, #state{db           = DBModule,
                                                           tmp_handler  = TmpHandler} = State) ->
    Reply = erlang:apply(DBModule, put, [TmpHandler, KeyBin, ValueBin]),
    {reply, Reply, State};

handle_call({compact_end, Commit}, _From, #state{db           = DBModule,
                                                 path         = Path,
                                                 raw_path     = RawPath,
                                                 handler      = Handler,
                                                 tmp_raw_path = TmpPath,
                                                 tmp_handler  = TmpHandler} = State) ->
    erlang:apply(DBModule, close, [TmpHandler]),
    case Commit of
        true ->
            erlang:apply(DBModule, close, [Handler]),
            leo_utils:file_delete_all(RawPath),
            file:delete(Path),
            case file:make_symlink(TmpPath, Path) of
                ok ->
                    case DBModule:open(Path) of
                        {ok, NewHandler} ->
                            NewState = State#state{
                                         raw_path = TmpPath,
                                         handler  = NewHandler},
                            {reply, ok, NewState};
                        {error, Cause} ->
                            {stop, Cause, State}
                    end;
                {error, Cause} ->
                    {stop, Cause, State}
            end;
        _ ->
            leo_utils:file_delete_all(TmpPath),
            {reply, ok, State}
    end;

handle_call({get_db_raw_filepath}, _From, #state{path = Path} = State) ->
    {reply, {ok, Path}, State}.

%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
handle_cast(_Msg, State) ->
    {noreply, State}.

%% Function: handle_info(Info, State) -> {noreply, State}          |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
handle_info(_Info, State) ->
    {noreply, State}.

%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, _State) ->
    ok.

%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
gen_file_raw_path(FilePath) ->
    FilePath ++ "_" ++ integer_to_list(leo_utils:now()) ++ "/".

get_raw_path(SymLinkPath) ->
    case file:read_link(SymLinkPath) of
        {ok, FileName} ->
            {ok, FileName};
        {error, enoent} ->
            RawPath = gen_file_raw_path(SymLinkPath),
            case filelib:ensure_dir(RawPath) of
                ok ->
                    case file:make_symlink(RawPath, SymLinkPath) of
                        ok ->
                            {ok, RawPath};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.
