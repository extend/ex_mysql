-module(ex_mysql_tcp).
-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).
-export([recv/1, send_recv/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Socket) ->
  gen_server:start_link(?MODULE, Socket, [{timeout, infinity}]).

recv(Server) ->
  gen_server:call(Server, recv, infinity).

send_recv(Server, Packet) ->
  gen_server:call(Server, {send, Packet}, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Socket) ->
  {ok, Socket}.

handle_call(recv, _From, Socket) ->
  handle_recv(Socket);
handle_call({send, Packet}, _From, Socket) ->
  ok = send_packet(Socket, Packet),
  handle_recv(Socket);
handle_call(Request, _From, State) ->
  {reply, {error, {badreq, Request}}, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Event, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

handle_recv(Socket) ->
  Ok = {ok, _Packet} = recv_packet(Socket),
  {reply, Ok, Socket}.

recv_packet(Socket) ->
  {ok, <<Size:24/little, Number:8/little>>} = gen_tcp:recv(Socket, 4),
  {ok, Bytes} = gen_tcp:recv(Socket, Size),
  {ok, {Number, Bytes}}.

send_packet(Socket, {Number, Bytes}) ->
  Packet = <<(byte_size(Bytes)):24/little, Number:8/little, Bytes/binary>>,
  gen_tcp:send(Socket, Packet);
send_packet(Socket, Bytes) when is_binary(Bytes) ->
  send_packet(Socket, {0, Bytes}).
