%%% @type address() = string() | atom() | ip_address().
%%% @type option() = {password, str()} | {database, str()}.
%%% @type str() = iodata() | atom().
%%% @type capability() = found_rows | long_flag | connect_with_db | no_schema
%%%                    | compress | local_files | ignore_space | protocol_v41
%%%                    | interactive | ssl | transactions | secure_connection.
%%% @type stmt_id() = integer().
%%% @type row_value() = binary() | string() | integer() | float() | datetime()
%%%                   | date() | time().
%%% @type datetime() = {date(), time()}.
%%% @type date() = {Year::integer(), Month::integer(), Day::integer()}.
%%% @type time() = {Hour::integer(), Minute::integer(), Second::integer()}.

-module(ex_mysql).
-behaviour(gen_server).
-include("ex_mysql.hrl").
-include("ex_mysql_com.hrl").

-export([start/1, start/2, start/3, start/4,
         use/2,
         q/2,
         fields/2,
         stats/1,
         processes/1,
         kill/2,
         debug/1,
         ping/1,
         prepare/2,
         stmt_info/2,
         quote/2,
         supports/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(connect_opts, {passwd = <<>>, db}).
-record(state, {socket, escape, supports, table}).

%% @equiv start(User, [])
start(User) ->
  start(User, []).

%% @equiv start(localhost, User, Options)
start(User, Options) ->
  start(localhost, User, Options).

%% @equiv start(Address, 3306, User, Options)
start(Address, User, Options) ->
  start(Address, 3306, User, Options).

%% @spec start(adress(), integer(), str(), [option()]) -> {ok, pid()} | {error, term()}
%% @doc Attempt to establish a connection with a given MySQL server.
start(Address, Port, User, Options) ->
  gen_server:start(?MODULE, {Address, Port, User, Options}, [{timeout, infinity}]).

%% @spec use(pid(), str()) -> ok | {error, term()}
%% @doc Tell MySQL to use a given database as the default for subsequent actions.
use(Pid, Database) ->
  gen_server:call(Pid, {use, Database}, infinity).

%% @spec q(pid(), str()) -> {ok, status()} | {res, pid()} | {error, term()}
%% @doc Execute a given statement.
q(Pid, Statement) ->
  gen_server:call(Pid, {q, Statement}, infinity).

%% @spec fields(pid(), str()) -> [#ex_mysql_field{}] | {error, term()}
%% @doc List fields of a given table.
fields(Pid, Table) ->
  gen_server:call(Pid, {fields, Table}, infinity).

%% @spec stats(pid()) -> string() | {error, term()}
%% @doc Return a string containing information about the server.
%%      This includes uptime in seconds and the number of running threads,
%%      questions, reloads, and open tables.
stats(Pid) ->
  gen_server:call(Pid, stats, infinity).

%% @spec processes(pid()) -> {res, pid()} | {error, term()}
%% @doc Return a result set describing the current server threads.
%%      This is the same kind of information as that reported by a
%%      "SHOW PROCESSLIST" query.
processes(Pid) ->
  gen_server:call(Pid, processes, infinity).

%% @spec kill(pid(), pos_integer()) -> ok | {error, term()}
%% @doc Kill a given process on the MySQL server.
kill(Pid, ProcessId) ->
  gen_server:call(Pid, {kill, ProcessId}, infinity).

%% @spec debug(pid()) -> string() | {error, term()}
%% @doc Ask the MySQL server to dump some debug information.
debug(Pid) ->
  gen_server:call(Pid, debug, infinity).

%% @spec ping(pid()) -> ok | {error, term()}
%% @doc Check whether or not the connection to the server is working.
ping(Pid) ->
  gen_server:call(Pid, ping, infinity).

%% @spec prepare(pid(), iodata()) -> {ok, stmt_id()} | {error, term()}
%% @doc Prepare a given statement.
prepare(Pid, Statement) ->
  gen_server:call(Pid, {prepare, Statement}, infinity).

%% @spec stmt_info(pid(), stmt_id()) -> {ok, #ex_mysql_stmt{}} | {error, invalid_stmt_id}
%% @doc Return informations about a given statement id.
stmt_info(Pid, StmtId) ->
  gen_server:call(Pid, {stmt_info, StmtId}, infinity).

%% @spec quote(pid(), term()) -> binary() | {error, term()}
%% @doc Escape and quote a value for safe concatenation into an SQL statement.
quote(Pid, Value) ->
  gen_server:call(Pid, {quote, Value}, infinity).

%% @spec supports(Pid) -> [capability()]
%% @doc Return the capabilities of the MySQL server.
supports(Pid) ->
  gen_server:call(Pid, supports, infinity).

%% @private
init({Address, Port, User, Options}) ->
  {ok, Socket} = gen_tcp:connect(Address, Port, [{active, false}, binary]),
  {ok, SocketServ} = ex_mysql_tcp:start_link(Socket),
  do_handshake(SocketServ, User, connect_opts(Options)).

%% @private
handle_call({use, Database}, _From, State) ->
  case do_use(Database, State) of
    Error = {error, _Reason} -> {reply, Error, State};
    ok -> {reply, ok, State} end;
handle_call({q, Statement}, From, State = #state{socket = Socket}) ->
  {ok, {_Number, Bytes = <<First, _Rest/binary>>}} = ex_mysql_tcp:send_recv(Socket, <<?COM_QUERY, Statement/binary>>),
  erlang:display(First),
  case First of
    0 ->
      {Ok, Status} = ex_mysql_util:read_ok(Bytes),
      EscapeMode = ex_mysql_util:status_to_escape_mode(Status),
      {reply, Ok, State#state{escape = EscapeMode}};
    255 -> {reply, ex_mysql_util:error(Bytes), State};
    _ -> handle_result(Bytes, From, State) end;
handle_call({fields, Table}, _From, State = #state{socket = Socket}) ->
  {ok, {_Number, Bytes}} = ex_mysql_tcp:send_recv(Socket, <<?COM_FIELD_LIST, Table/binary>>),
  case Bytes of
    <<255, _Rest/binary>> -> {reply, ex_mysql_util:error(Bytes), State};
    _ ->
      Fields = read_fields_list(Socket, [ex_mysql_util:read_field(Bytes)]),
      {reply, Fields, State} end;
handle_call(stats, _From, State) ->
  cmd(State, <<?COM_STATISTICS>>, fun binary_to_list/1);
handle_call(processes, From, State = #state{socket = Socket}) ->
  {ok, {_Number, Bytes}} = ex_mysql_tcp:send_recv(Socket, <<?COM_PROCESS_INFO>>),
  handle_result(Bytes, From, State);
handle_call({kill, ProcessId}, _From, State) ->
  cmd(State, <<?COM_PROCESS_KILL, ProcessId:64/little>>, fun ex_mysql_util:ok/1);
handle_call(debug, _From, State) ->
  cmd(State, <<?COM_DEBUG>>, fun binary_to_list/1);
handle_call(ping, _From, State) ->
  cmd(State, <<?COM_PING>>, fun ex_mysql_util:ok/1);
handle_call({prepare, Statement}, From, State = #state{socket = Socket}) ->
  {ok, {_Number, Bytes}} = ex_mysql_tcp:send_recv(Socket, <<?COM_STMT_PREPARE, Statement/binary>>),
  case Bytes of
    <<255, _Rest/binary>> -> {reply, ex_mysql_util:error(Bytes), State};
    <<0, StmtId:32/little, ColumnsCount:16/little, ParamsCount:16/little, _Rest2/binary>> ->
      gen_server:reply(From, {ok, StmtId}),
      Parameters = read_params(Socket, ParamsCount),
      Columns = case ColumnsCount of
                  0 -> [];
                  _ -> read_fields(Socket, ColumnsCount) end,
      erlang:display(Columns),
      Stmt = #ex_mysql_stmt{params = {ParamsCount, Parameters}, columns = {ColumnsCount, Columns}},
      {noreply, insert(State, {{stmt, StmtId}, Stmt})} end;
handle_call({execute, _StmtId}, _From, State) ->
  {reply, ok, State};
handle_call({stmt_info, StmtId}, _From, State = #state{table = Table}) ->
  Key = {stmt, StmtId},
  case ets:lookup(Table, Key) of
    [{Key, Stmt}] -> {reply, {ok, Stmt}, State};
    [] -> {reply, {error, invalid_stmt_id}, State} end;
handle_call({quote, Value}, _From, State = #state{escape = EscapeMode}) ->
  {reply, ex_mysql_util:quote(Value, EscapeMode), State};
handle_call(supports, _From, State = #state{supports = Caps}) ->
  {reply, ex_mysql_util:caps_flags(Caps), State};
handle_call(Request, _From, State) ->
  {reply, {error, {badreq, Request}}, State}.

%% @private
handle_cast(_Msg, State) ->
  {noreply, State}.

%% @private
handle_info(_Event, State) ->
  {noreply, State}.

%% @private
terminate(_Reason, _State) ->
  ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


connect_opts(Args) ->
  lists:foldl(fun ({password, Password}, Opts) ->
                    Opts#connect_opts{passwd = Password};
                  ({database, Database}, Opts) ->
                    Opts#connect_opts{db = Database} end,
              #connect_opts{}, Args).

do_handshake(Socket, User, #connect_opts{passwd = Passwd, db = Db}) ->
  {ok, {Number, Bytes}} = ex_mysql_tcp:recv(Socket),
  {Caps, Status, Message} = ex_mysql_util:read_init(Bytes),
  ok = send_auth(Socket, Number + 1, User, Message, Passwd),
  EscapeMode = ex_mysql_util:status_to_escape_mode(Status),
  State = #state{socket = Socket, escape = EscapeMode, supports = Caps},
  case Db of
    undefined -> {ok, State};
    _ ->
      case do_use(Db, State) of
        {error, Reason} -> {stop, Reason};
        ok -> {ok, State} end end.

send_auth(Socket, Number, User, Message, Passwd) ->
  Bytes = <<?CLIENT_LONG_PASSWORD:16/little, (1 bsl 24):24/little, User/binary, 0,
            (ex_mysql_util:scramble(Passwd, Message, false))/binary, 0>>,
  {ok, {_Number2, Bytes2}} = ex_mysql_tcp:send_recv(Socket, {Number, Bytes}),
  case Bytes2 of
    <<0:24>> -> ok;
    _ -> ex_mysql_util:error(Bytes2) end.

do_use(Database, #state{socket = Socket}) ->
  {ok, {_Number, Bytes}} = ex_mysql_tcp:send_recv(Socket, <<?COM_INIT_DB, Database/binary>>),
  case Bytes of
    <<0:24>> -> ok;
    _ -> ex_mysql_util:error(Bytes) end.

cmd(State = #state{socket = Socket}, Packet, ReplyFun) ->
  {ok, {_Number, Bytes}} = ex_mysql_tcp:send_recv(Socket, Packet),
  {reply, ReplyFun(Bytes), State}.

handle_result(Header, From, State = #state{socket = Socket}) ->
  {ok, Pid} = gen_fsm:start_link(ex_mysql_res, self(), [{timeout, infinity}]),
  gen_server:reply(From, {res, Pid}),
  FieldCount = ex_mysql_util:read_result_set_header(Header),
  Fields = read_fields(Socket, FieldCount),
  Pid ! {fields, Fields},
  handle_result_rows(Pid, Fields, State).

handle_result_rows(Pid, Fields, State = #state{socket = Socket}) ->
  case ex_mysql_tcp:recv(Socket) of
    {ok, {_Number, <<254, Rest/binary>>}} when byte_size(Rest) < 8 ->
      Pid ! {row, false},
      {noreply, State};
    {ok, {_Number, Bytes}} ->
      Pid ! {row, ex_mysql_util:read_row(Bytes, Fields)},
      handle_result_rows(Pid, Fields, State) end.

read_fields_list(Socket, Fields) ->
  case ex_mysql_tcp:recv(Socket) of
    {ok, {_Number, <<254, Rest/binary>>}} when byte_size(Rest) < 8 -> lists:reverse(Fields);
    {ok, {_Number, Bytes}} -> read_fields_list(Socket, [ex_mysql_util:read_field(Bytes) | Fields]) end.

read_fields(Socket, Count) ->
  read_list(Socket, Count, fun ex_mysql_util:read_field/1).

read_params(_Socket, 0) ->
  [];
read_params(Socket, Count) ->
  read_list(Socket, Count, fun ex_mysql_util:read_field/1).

read_list(Socket, Count, ReadFun) ->
  read_list(Socket, Count, ReadFun, []).

read_list(Socket, 0, _ReadFun, Values) ->
  {ok, {_Number, <<254, _Rest/binary>>}} = ex_mysql_tcp:recv(Socket),
  lists:reverse(Values);
read_list(Socket, Count, ReadFun, Values) ->
  {ok, {_Number, Bytes}} = ex_mysql_tcp:recv(Socket),
  read_list(Socket, Count - 1, ReadFun, [ReadFun(Bytes) | Values]).

insert(State = #state{table = undefined}, Object) ->
  insert(State#state{table = ets:new(ex_mysql, [private])}, Object);
insert(State = #state{table = T}, Object) ->
  ets:insert(T, Object),
  State.
