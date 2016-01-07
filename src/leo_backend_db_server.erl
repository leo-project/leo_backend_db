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
%% Leo Bakcend DB - Server
%% @doc The gen_server process for the process of database as part of a supervision tree
%% @reference https://github.com/leo-project/leo_backend_db/blob/master/src/leo_backend_db_server.erl
%% @end
%%======================================================================
-module(leo_backend_db_server).

-behaviour(gen_server).

-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/3,
         stop/1]).

%% data operations.
-export([put/3,
         get/2,
         delete/2,
         fetch/4,
         first/1,
         status/1,
         close/1,
         run_compaction/1, finish_compaction/2,
         put_value_to_new_db/3,
         get_db_raw_filepath/1
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-record(state, {id :: atom(),
                db :: atom(),
                path = []         :: string(),
                raw_path = []     :: string(),
                tmp_raw_path = [] :: string(),
                tmp_handler :: pid(),
                handler     :: pid()}).

-define(DEF_TIMEOUT, 30000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Creates the gen_server process as part of a supervision tree
-spec(start_link(Id, DBModule, Path) ->
             {ok,pid()} | ignore | {error, any()} when Id::atom(),
                                                       DBModule::atom(),
                                                       Path::string()).
start_link(Id, DBModule, Path) ->
    gen_server:start_link({local, Id}, ?MODULE, [Id, DBModule, Path], []).

%% @doc Close the process
stop(Id) ->
    gen_server:call(Id, stop).


%%--------------------------------------------------------------------
%% Data Operation related.
%%--------------------------------------------------------------------
%% @doc Insert an object into backend-db.
%%
-spec(put(Id, KeyBin, ValueBin) ->
             ok | {error, any()} when Id::atom(),
                                      KeyBin::binary(),
                                      ValueBin::binary()).
put(Id, KeyBin, ValueBin) ->
    gen_server:call(Id, {put, KeyBin, ValueBin}, ?DEF_TIMEOUT).


%% @doc Retrieve an object from backend-db.
%%
-spec(get(Id, KeyBin) ->
             {ok, binary()} |
             not_found |
             {error, any()} when Id::atom(),
                                 KeyBin::binary()).
get(Id, KeyBin) ->
    gen_server:call(Id, {get, KeyBin}, ?DEF_TIMEOUT).


%% @doc Delete an object from backend-db.
%%
-spec(delete(Id::atom(), KeyBin::binary()) ->
             ok | {error, any()}).
delete(Id, KeyBin) ->
    gen_server:call(Id, {delete, KeyBin}, ?DEF_TIMEOUT).


%% @doc Fetch records from backend-db.
%%
-spec(fetch(Id, KeyBin, Fun, MaxKeys) ->
             {ok, list()} |
             not_found |
             {error, any()} when Id::atom(),
                                 KeyBin::binary(),
                                 Fun::function(),
                                 MaxKeys::integer()).
fetch(Id, KeyBin, Fun, MaxKeys) ->
    gen_server:call(Id, {fetch, KeyBin, Fun, MaxKeys}, ?DEF_TIMEOUT).


%% @doc Retrieve a first record from backend-db.
%%
-spec(first(Id) ->
             {ok, any(), any()} | {error, any()} when Id::atom()).
first(Id) ->
    gen_server:call(Id, first, ?DEF_TIMEOUT).


%% @doc Retrieve the current status from the database
%%
-spec(status(Id) ->
             [{atom(), term()}] when Id::atom()).
status(Id) ->
    gen_server:call(Id, status, ?DEF_TIMEOUT).


%% @doc Close the database
%%
-spec(close(Id) ->
             ok when Id::atom()).
close(Id) ->
    gen_server:call(Id, close, ?DEF_TIMEOUT).


%% @doc Direct to start a compaction.
%%
-spec(run_compaction(Id) ->
             ok | {error, any()} when Id::atom()).
run_compaction(Id) ->
    gen_server:call(Id, run_compaction, ?DEF_TIMEOUT).


%% @doc Direct to end a compaction.
%%
-spec(finish_compaction(Id, Commit) ->
             ok | {error, any()} when Id::atom(),
                                      Commit::boolean()).
finish_compaction(Id, Commit) ->
    gen_server:call(Id, {finish_compaction, Commit}, ?DEF_TIMEOUT).


%% @doc Direct to put a record to a temporary new data file.
%%
-spec(put_value_to_new_db(Id, KeyBin, ValueBin) ->
             ok | {error, any()} when Id::atom(),
                                      KeyBin::binary(),
                                      ValueBin::binary()).
put_value_to_new_db(Id, KeyBin, ValueBin) ->
    gen_server:call(Id, {put_value_to_new_db, KeyBin, ValueBin}, ?DEF_TIMEOUT).


%% @doc get database file path for calculating disk size.
%%
-spec(get_db_raw_filepath(Id) ->
             {ok, string()} | {error, any()} when Id::atom()).
get_db_raw_filepath(Id) ->
    gen_server:call(Id, get_db_raw_filepath, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc gen_server callback - Module:init(Args) -> Result
init([Id, leo_backend_db_ets = DBModule, Table]) ->
    ok = DBModule:open(Table),
    {ok, #state{id = Id,
                db = DBModule,
                handler = list_to_atom(Table)}};

init([Id, DBModule, Path]) ->
    {ok, Curr} = file:get_cwd(),
    Path1 = case Path of
                "/"   ++ _Rest -> Path;
                "../" ++ _Rest -> Path;
                "./"  ++  Rest -> Curr ++ "/" ++ Rest;
                _              -> Curr ++ "/" ++ Path
            end,

    case get_raw_path(Path1) of
        {ok, RawPath} ->
            case DBModule:open(Path) of
                {ok, Handler} ->
                    {ok, #state{id = Id,
                                db = DBModule,
                                path = Path1,
                                raw_path = RawPath,
                                handler = Handler}};
                {error, Cause} ->
                    {stop, Cause}
            end;
        {error, Cause} ->
            {stop, Cause}
    end.

%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop, _From, #state{id = Id,
                                db = DBModule,
                                handler = Handler} = State) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING},
                           {function, "handle_call/3 - stop"},
                           {line, ?LINE}, {body, Id}]),
    catch erlang:apply(DBModule, close, [Handler]),
    {stop, normal, stopped, State};


%%--------------------------------------------------------------------
%% Data Operation related.
%%--------------------------------------------------------------------
handle_call({put, KeyBin, ValueBin}, _From, #state{db = DBModule,
                                                   handler = Handler} = State) ->
    Reply = erlang:apply(DBModule, put, [Handler, KeyBin, ValueBin]),
    {reply, Reply, State};


handle_call({get, KeyBin}, _From, #state{db = DBModule,
                                         handler = Handler} = State) ->
    Reply = erlang:apply(DBModule, get, [Handler, KeyBin]),
    {reply, Reply, State};


handle_call({delete, KeyBin}, _From, #state{db = DBModule,
                                            handler = Handler} = State) ->
    Reply = erlang:apply(DBModule, delete, [Handler, KeyBin]),
    {reply, Reply, State};


handle_call({fetch, KeyBin, Fun, MaxKeys}, _From, #state{db = DBModule,
                                                         handler = Handler} = State) ->
    Reply = case catch erlang:apply(DBModule, prefix_search,
                                    [Handler, KeyBin, Fun, MaxKeys]) of
                {'EXIT', Cause} ->
                    {error, Cause};
                Ret ->
                    Ret
            end,
    {reply, Reply, State};

handle_call(first, _From, #state{db = DBModule,
                                 handler = Handler} = State) ->
    Reply = erlang:apply(DBModule, first, [Handler]),
    {reply, Reply, State};


handle_call(status, _From, #state{db = DBModule,
                                  handler = Handler} = State) ->
    Reply = erlang:apply(DBModule, status, [Handler]),
    {reply, Reply, State};


handle_call(close, _From, #state{db = DBModule,
                                 handler = Handler} = State) ->
    catch erlang:apply(DBModule, close, [Handler]),
    {reply, ok, State};


handle_call(run_compaction, _From, #state{db = DBModule,
                                          path = Path} = State) ->
    NewPath = gen_file_raw_path(Path),
    case filelib:ensure_dir(NewPath) of
        ok ->
            case DBModule:open(NewPath) of
                {ok, NewHandler} ->
                    NewState = State#state{
                                 tmp_raw_path = NewPath,
                                 tmp_handler = NewHandler},
                    {reply, ok, NewState};
                {error, Reason} ->
                    {stop, Reason, State}
            end;
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_call({put_value_to_new_db, KeyBin, ValueBin}, _From,
            #state{db          = DBModule,
                   tmp_handler = TmpHandler} = State) ->
    Reply = erlang:apply(DBModule, put, [TmpHandler, KeyBin, ValueBin]),
    {reply, Reply, State};

handle_call({finish_compaction, Commit}, _From, #state{db = DBModule,
                                                       path = Path,
                                                       raw_path = RawPath,
                                                       handler = Handler,
                                                       tmp_raw_path = TmpPath,
                                                       tmp_handler = TmpHandler} = State) ->
    _ = erlang:apply(DBModule, close, [TmpHandler]),

    case Commit of
        true ->
            _ = erlang:apply(DBModule, close, [Handler]),
            leo_file:file_delete_all(RawPath),
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
            leo_file:file_delete_all(TmpPath),
            {reply, ok, State}
    end;

handle_call(get_db_raw_filepath, _From, #state{path = Path} = State) ->
    {reply, {ok, Path}, State}.


%% @doc gen_server callback - Module:handle_cast(Request, State) -> Result
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc gen_server callback - Module:handle_info(Info, State) -> Result
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% <p>
%% gen_server callback - Module:terminate(Reason, State)
%% </p>
terminate(_Reason, #state{id = Id,
                          db = DBModule,
                          handler = Handler}) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING},
                           {function, "terminate/2"},
                           {line, ?LINE}, {body, Id}]),
    catch erlang:apply(DBModule, close, [Handler]),
    ok.

%% @doc Convert process state when code is changed
%% <p>
%% gen_server callback - Module:code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason}.
%% </p>
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
%% @private
gen_file_raw_path(FilePath) ->
    FilePath ++ "_" ++ integer_to_list(leo_date:now()) ++ "/".

%% @private
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
