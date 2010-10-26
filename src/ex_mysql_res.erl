-module(ex_mysql_res).
-behaviour(gen_fsm).

-record(state, {conn, fields, count = 0, rows = queue:new()}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([fields/1, fetch/1, count/1, close/1]).

%% ------------------------------------------------------------------
%% gen_fsm Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
-export([wait_fields/3, wait_row/3, finished/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

fields(Server) ->
  gen_fsm:sync_send_event(Server, fields, infinity).

fetch(Server) ->
  gen_fsm:sync_send_event(Server, fetch, infinity).

count(Server) ->
  gen_fsm:sync_send_event(Server, count, infinity).

close(Server) ->
  gen_fsm:send_all_state_event(Server, close).

%% ------------------------------------------------------------------
%% gen_fsm Function Definitions
%% ------------------------------------------------------------------

init(Conn) ->
  {ok, wait_fields, #state{conn = Conn}}.

wait_fields(Event, From, State) ->
  Fields = recv_fields(),
  wait_row(Event, From, State#state{fields = Fields}).

wait_row(fetch, _From, State = #state{conn = Conn, count = Count, rows = Q}) ->
  case queue:out(Q) of
    {{value, Row}, NewQ} -> {reply, Row, wait_row, State#state{rows = NewQ}};
    {empty, _} ->
      case recv_row() of
        false ->
          unlink(Conn),
          {reply, false, finished, State#state{conn = undefined, rows = []}};
        Row -> {reply, Row, wait_row, State#state{count = Count + 1}} end end;
wait_row(fields, _From, State = #state{fields = Fields}) ->
  {reply, Fields, wait_row, State};
wait_row(Event, From, State) ->
  NewState = recv_rows(State),
  finished(Event, From, NewState).

finished(fetch, _From, State = #state{rows = [Row | Rows]}) ->
  {reply, Row, finished, State#state{rows = Rows}};
finished(fetch, _From, State = #state{rows = []}) ->
  {reply, false, finished, State};
finished(count, _From, State = #state{count = Count}) ->
  {reply, Count, finished, State};
finished(fields, _From, State = #state{fields = Fields}) ->
  {reply, Fields, finished, State};
finished(Event, _From, State) ->
  {reply, {error, {badevent, Event}}, finished, State}.

handle_event(close, _StateName, State) ->
  {stop, normal, State};
handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.

handle_sync_event(Event, _From, StateName, State) ->
  {reply, {error, {badevent, Event}}, StateName, State}.

handle_info({fields, Fields}, wait_fields, State) ->
  {next_state, wait_row, State#state{fields = Fields}};
handle_info({row, false}, wait_row, State = #state{conn = Conn, rows = Q}) ->
  unlink(Conn),
  {next_state, finished, State#state{rows = queue:to_list(Q)}};
handle_info({row, Row}, wait_row, State) ->
  {next_state, wait_row, enqueue(Row, State)};
handle_info(_Msg, StateName, State) ->
  {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
  ok.

code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

recv_fields() ->
  receive {fields, Fields} -> Fields end.

recv_rows(State = #state{rows = Q}) ->
  case recv_row() of
    false -> State#state{rows = queue:to_list(Q)};
    Row -> recv_rows(enqueue(Row, State)) end.

recv_row() ->
  receive {row, Row} -> Row end.

enqueue(Row, State = #state{count = Count, rows = Q}) ->
  State#state{count = Count + 1, rows = queue:in(Row, Q)}.
