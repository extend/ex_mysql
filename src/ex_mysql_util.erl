-module(ex_mysql_util).
-include("ex_mysql.hrl").
-include("ex_mysql_com.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([quote/2]).
-export([read_init/1, read_result_set_header/1, read_field/1, read_param/1]).
-export([read_row/2, read_value/2]).
-export([scramble/3]).
-export([ok/1, error/1]).
-export([read_null_terminated_string/1, read_length/1, read_length_coded_binary/1, read_length_coded_string/1]).

-export([caps_flags/1, status_to_escape_mode/1]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

quote(Value, no_backslash) ->
  quote_val(Value, fun escape/1);
quote(Value, backslash) ->
  quote_val(Value, fun escape_bs/1).

read_init(<<10/little, Rest/binary>>) ->
  {_ServVersion, Rest2} = read_null_terminated_string(Rest),
  <<_ThreadId:32/little, ScrambleBegin:8/binary, 0, Caps:16/little,
    _Lang:8/little, Status:16/little, 0:104, _Rest/binary>> = Rest2,
  {Caps, Status, ScrambleBegin}.

scramble(Password = <<>>, _Message, _Secure) ->
  Password;
scramble(Password, Message, false) ->
  {PassNr, PassNr2} = hash(Password),
  {MessageNr, MessageNr2} = hash(Message),
  Seed1 = PassNr bxor MessageNr,
  Seed2 = PassNr2 bxor MessageNr2,
  Ctx = ex_mysql_rnd:new(Seed1, Seed2),
  {List, Ctx2} = ex_mysql_rnd:next(Ctx, 8),
  {Value, _Ctx3} = ex_mysql_rnd:next(Ctx2),
  Extra = trunc(Value * 31),
  list_to_binary([ (trunc(X * 31) + 64) bxor Extra || X <- List ]);
scramble(Password, Message, true) ->
  Stage1Hash = crypto:sha(Password),
  Ctx = crypto:sha_init(),
  Ctx2 = crypto:sha_update(Ctx, Message),
  Ctx3 = crypto:sha_update(Ctx2, crypto:sha(Stage1Hash)),
  crypto:exor(crypto:sha_final(Ctx3), Stage1Hash).

ok(<<0, Rest/binary>>) ->
  {_Rows, Rest2} = read_length(Rest),
  {_InsertId, Rest3} = read_length(Rest2),
  case Rest3 of
    <<_ServerStatus, Message/binary>> -> {ok, binary_to_list(Message)};
    <<>> -> ok end;
ok(Bytes) ->
  error(Bytes).

error(<<255, Errno:16/little, Rest/binary>>) ->
  Message = case binary:last(Rest) == $\0 of
    true -> binary_part(Rest, {0, byte_size(Rest) - 1});
    false -> Rest end,
  {error, {mysql, Errno, binary_to_list(Message)}}.

read_result_set_header(Bytes) ->
  {FieldCount, _Rest} = read_length(Bytes),
  FieldCount.

read_field(Bytes) ->
  {Table, Rest} = read_length_coded_string(Bytes),
  {Name, Rest2} = read_length_coded_string(Rest),
  {Length, Rest3} = read_length_coded_integer(Rest2),
  {TypeInt, Rest4} = read_length_coded_integer(Rest3),
  {Flags, _Rest5} = read_length_coded_integer(Rest4),
  Type = case field_flavor(Flags) of
           basic -> integer_to_field_type(TypeInt);
           Flavor -> Flavor end,
  #ex_mysql_field{table = Table, name = Name, type = Type, length = Length}.

read_param(Bytes) ->
  {TypeInt, Rest} = read_length_coded_integer(Bytes),
  {_Flags, _Rest2} = read_length_coded_integer(Rest),
  integer_to_field_type(TypeInt).

read_row(Row, Fields) ->
  read_row(Row, Fields, []).

read_row(<<>>, [], Values) ->
  lists:reverse(Values);
read_row(Bytes, [#ex_mysql_field{type = Type} | Fields], Values) ->
  {Value, NewRest} = read_column_value(Bytes),
  read_row(NewRest, Fields, [read_value(Value, Type) | Values]).

read_value(Value, _Field) when Value =:= null ->
  null;
read_value(Value, T) when T =:= tiny; T =:= short; T =:= long ->
  binary_to_integer(Value);
read_value(Value, {decimal, _Scale}) ->
  case binary:split(Value, <<$.>>) of
    [E, D] -> {binary_to_signed(E), binary_to_integer(D)};
    [E] -> {binary_to_signed(E), 0} end;
read_value(Value, T) when T =:= float; T =:= double ->
  Value2 = binary_to_list(Value),
  try list_to_float(Value2)
  catch
    error:badarg -> float(list_to_integer(Value2)) end;
read_value(Value, string) ->
  binary_to_list(Value);
read_value(Value, set) ->
  [ binary_to_list(Item) || Item <- binary:split(Value, <<$,>>) ];
read_value(<<Date:10/binary, $\s, Time:8/binary>>, datetime) ->
  {read_date(Date), read_time(Time)};
read_value(Value, date) ->
  {date, read_date(Value)};
read_value(Value, time) ->
  {time, read_time(Value)};
read_value(Value, year) ->
  binary_to_integer(Value);
read_value(Value, geometry) ->
  read_geometry(Value);
read_value(Value, _Type) ->
  Value.

read_null_terminated_string(Bin) ->
    [String, Rest] = binary:split(Bin, <<0>>),
    {binary_to_list(String), Rest}.

read_length(<<Length, Rest/binary>>) when Length =< 250 ->
  {Length, Rest};
read_length(<<252, Length:16/little, Rest/binary>>) ->
  {Length, Rest};
read_length(<<253, Length:24/little, Rest/binary>>) ->
  {Length, Rest};
read_length(<<254, Length:64/little, Rest/binary>>) ->
  {Length, Rest}.

read_length_coded_binary(Bytes) ->
  {Length, Rest} = read_length(Bytes),
  <<Bin:Length/binary, NewRest/binary>> = Rest,
  {Bin, NewRest}.

read_length_coded_integer(Bytes) ->
  {Length, Rest} = read_length(Bytes),
  <<Int:Length/little-unit:8, NewRest/binary>> = Rest,
  {Int, NewRest}.

read_length_coded_string(Bytes) ->
    {Bin, Rest} = read_length_coded_binary(Bytes),
    {binary_to_list(Bin), Rest}.

caps_flags(Set) ->
  set_to_flags(Set, [{found_rows, ?CLIENT_FOUND_ROWS},
                     {long_flag, ?CLIENT_LONG_FLAG},
                     {connect_with_db, ?CLIENT_CONNECT_WITH_DB},
                     {no_schema, ?CLIENT_NO_SCHEMA},
                     {compress, ?CLIENT_COMPRESS},
                     {local_files, ?CLIENT_LOCAL_FILES},
                     {ignore_space, ?CLIENT_IGNORE_SPACE},
                     {protocol_41, ?CLIENT_PROTOCOL_41 bor ?CLIENT_RESERVED},
                     {interactive, ?CLIENT_INTERACTIVE},
                     {ssl, ?CLIENT_SSL},
                     {transactions, ?CLIENT_TRANSACTIONS},
                     {secure_connection, ?CLIENT_SECURE_CONNECTION}]).

status_to_escape_mode(Int) when Int band ?SERVER_STATUS_NO_BACKSLASH_ESCAPES =/= 0 ->
  no_backslash;
status_to_escape_mode(_Int) ->
  backslash.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

quote_val(Int, _EscapeFun) when is_integer(Int), Int >= -9223372036854775808, Int =< 18446744073709551615 ->
  list_to_binary(integer_to_list(Int));
quote_val({Integral, Decimal}, _EscapeFun) when is_integer(Integral), is_integer(Decimal), Decimal >= 0 ->
  IntBin = list_to_binary(integer_to_list(Integral)),
  DecBin = list_to_binary(integer_to_list(Decimal)),
  <<$', IntBin/binary, $., DecBin/binary, $'>>;
quote_val(Float, _EscapeFun) when is_float(Float) ->
  list_to_binary(float_to_list(Float));
quote_val(Bin, EscapeFun) when is_binary(Bin) ->
  quote_binary(Bin, EscapeFun);
quote_val(Atom, EscapeFun) when is_atom(Atom) ->
  quote_atom(Atom, EscapeFun);
quote_val(List, EscapeFun) when is_list(List) ->
  quote_binary(iolist_to_binary(List), EscapeFun);
quote_val(Tuple, _EscapeFun) when is_tuple(Tuple) ->
  quote_tuple(Tuple).

quote_atom(null, _EscapeFun) ->
  <<"NULL">>;
quote_atom(true, _EscapeFun) ->
  <<"TRUE">>;
quote_atom(false, _EscapeFun) ->
  <<"FALSE">>;
quote_atom(Atom, EscapeFun) ->
  quote_binary(atom_to_binary(Atom, latin1), EscapeFun).

quote_binary(Bin, EscapeFun) ->
  <<$', (EscapeFun(Bin))/binary, $'>>.

escape(Bin) ->
  binary:replace(Bin, <<$'>>, <<"''">>).

escape_bs(Bin) ->
  Bin2 = binary:replace(Bin, [<<$\\>>, <<$'>>], <<$\\>>, [{insert_replaced, 1}]),
  binary:replace(Bin2, <<0>>, <<"\\0">>).

quote_tuple({date, Date}) ->
  case date_to_binary(Date) of
    Error = {error, _Reason} -> Error;
    DateBin -> <<$', DateBin/binary, $'>> end;
quote_tuple({time, Time}) ->
  case time_to_binary(Time) of
    Error = {error, _Reason} -> Error;
    TimeBin -> <<$', TimeBin/binary, $'>> end;
quote_tuple({Date, Time}) ->
  case date_to_binary(Date) of
    Error = {error, _Reason} -> Error;
    DateBin ->
      case time_to_binary(Time) of
        Error = {error, _Reason} -> Error;
        TimeBin -> <<$', DateBin/binary, $\s, TimeBin/binary, $'>> end end;
quote_tuple(Tuple) ->
  {error, {bad_tuple, Tuple}}.

date_to_binary(Date) ->
  case calendar:valid_date(Date) of
    true ->
      {Year, Month, Day} = Date,
      case Year >= 1000 andalso Year =< 9999 of
        true ->
          YearBin = list_to_binary(integer_to_list(Year)),
          MonthBin = integer_to_2digits(Month),
          DayBin = integer_to_2digits(Day),
          <<YearBin/binary, $-, MonthBin/binary, $-, DayBin/binary>>;
        false -> {error, {year_out_of_range, Year}} end;
    false -> {error, {bad_date, Date}} end.

time_to_binary(Time) ->
  case valid_time(Time) of
    true ->
      {Hour, Minute, Second} = Time,
      HourBin = integer_to_2digits(Hour),
      MinBin = integer_to_2digits(Minute),
      SecBin = integer_to_2digits(Second),
      <<HourBin/binary, $:, MinBin/binary, $:, SecBin/binary>>;
    false -> {error, {bad_time, Time}} end.

valid_time({Hour, Minute, Second}) when Hour >= 0, Hour =< 23, Minute >= 0, Minute =< 59, Second >= 0, Second =< 59 ->
  true;
valid_time(_Tuple) ->
  false.

integer_to_2digits(Int) when Int < 10 ->
  <<$0, (Int + $0)>>;
integer_to_2digits(Int) ->
  <<(Int / 10 + $0), ((Int rem 10) + $0)>>.

hash(Password) ->
  hash(Password, 1345345333, 305419889, 7).
hash(<<Char, Rest/binary>>, Nr, Nr2, Add) when Char =:= $\s; Char =:= $\t ->
  hash(Rest, Nr, Nr2, Add);
hash(<<Char, Rest/binary>>, Nr, Nr2, Add) ->
  NewNr = Nr bxor (((Nr band 63) + Add) * Char + (Nr bsl 8)),
  hash(Rest, NewNr, Nr2 + ((Nr2 bsl 8) bxor NewNr), Add + Char);
hash(<<>>, Nr, Nr2, _Add) ->
  Mask = (1 bsl 31) - 1,
  {Nr band Mask, Nr2 band Mask}.

set_to_flags(Set, Spec) ->
  lists:reverse(
    lists:foldl(fun ({Flag, Mask}, List) ->
                  case Set band Mask =/= 0 of
                    true -> [Flag | List];
                    false -> List end end,
                [], Spec)).

field_flavor(FieldFlags) when FieldFlags band ?SET_FLAG =/= 0 ->
  set;
field_flavor(_FieldFlags) ->
  basic.

read_column_value(<<251, Rest/binary>>) ->
  {null, Rest};
read_column_value(Bytes) ->
  read_length_coded_binary(Bytes).

read_date(<<Year:4/binary, $-, Month:2/binary, $-, Day:2/binary>>) ->
  {binary_to_integer(Year), binary_to_integer(Month), binary_to_integer(Day)}.

read_time(<<Hour:2/binary, $:, Minute:2/binary, $:, Second:2/binary>>) ->
  {binary_to_integer(Hour), binary_to_integer(Minute), binary_to_integer(Second)}.

read_geometry(Bytes = <<1, _Type, 0:24, _Rest/binary>>) ->
  {Object, <<>>} = read_wkb(Bytes),
  {geometry, {0, Object}};
read_geometry(<<SRID:32/little, Rest/binary>>) ->
  {Object, <<>>} = read_wkb(Rest),
  {geometry, {SRID, Object}}.

read_wkb(Bytes = <<1, 1, 0:24, _Rest/binary>>) ->
  read_wkb_point(Bytes);
read_wkb(Bytes = <<1, 2, 0:24, _Rest/binary>>) ->
  read_wkb_line_string(Bytes);
read_wkb(Bytes = <<1, 3, 0:24, _Rest/binary>>) ->
  read_wkb_polygon(Bytes);
read_wkb(<<1, 4, 0:24, Rest/binary>>) ->
  {MultiPoint, NewRest} = read_wkb_objects(Rest, fun read_wkb_point/1),
  {{multi_point, MultiPoint}, NewRest};
read_wkb(<<1, 5, 0:24, Rest/binary>>) ->
  {MultiLineString, NewRest} = read_wkb_objects(Rest, fun read_wkb_line_string/1),
  {{multi_line_string, MultiLineString}, NewRest};
read_wkb(<<1, 6, 0:24, Rest/binary>>) ->
  {MultiPolygon, NewRest} = read_wkb_objects(Rest, fun read_wkb_polygon/1),
  {{multi_polygon, MultiPolygon}, NewRest};
read_wkb(<<1, 7, 0:24, Rest/binary>>) ->
  {Collection, NewRest} = read_wkb_objects(Rest, fun read_wkb/1),
  {{collection, Collection}, NewRest}.

read_wkb_point(<<1, 1, 0:24, Rest/binary>>) ->
  read_point(Rest).

read_wkb_line_string(<<1, 2, 0:24, Rest/binary>>) ->
  {LineString, NewRest} = read_wkb_objects(Rest, fun read_point/1),
  {{line_string, LineString}, NewRest}.

read_wkb_polygon(<<1, 3, 0:24, Rest/binary>>) ->
  {Value, NewRest} = read_wkb_objects(Rest, fun read_linear_ring/1),
  {{polygon, Value}, NewRest}.

read_linear_ring(Bin) ->
  {Value, Rest} = read_wkb_objects(Bin, fun read_point/1),
  {{linear_ring, Value}, Rest}.

read_point(<<X/little-float, Y/little-float, Rest/binary>>) ->
  {{point, {X, Y}}, Rest}.

read_wkb_objects(<<Count:32/little, Rest/binary>>, ReadFun) ->
  {Objects, NewRest} = read_multiple(Rest, Count, ReadFun),
  {{Count, Objects}, NewRest}.

read_multiple(Bin, Count, ReadFun) ->
  read_multiple(Bin, Count, ReadFun, []).

read_multiple(Rest, 0, _ReadFun, Values) ->
  {lists:reverse(Values), Rest};
read_multiple(Bin, Count, ReadFun, Values) ->
  {Value, Rest} = ReadFun(Bin),
  read_multiple(Rest, Count - 1, ReadFun, [Value | Values]).

binary_to_signed(<<$-, Rest/binary>>) ->
  -binary_to_signed(Rest);
binary_to_signed(Bin) ->
  binary_to_integer(Bin).

binary_to_integer(Bin) ->
  binary_to_integer(Bin, 0).
binary_to_integer(<<Char, Rest/binary>>, Int) when Char >= $0, Char =< $9 ->
  binary_to_integer(Rest, Int * 10 + Char - $0);
binary_to_integer(<<>>, Int) ->
  Int.

integer_to_field_type(?FIELD_TYPE_DECIMAL) ->
  decimal;
integer_to_field_type(?FIELD_TYPE_TINY) ->
  tiny;
integer_to_field_type(?FIELD_TYPE_SHORT) ->
  short;
integer_to_field_type(?FIELD_TYPE_LONG) ->
  long;
integer_to_field_type(?FIELD_TYPE_FLOAT) ->
  float;
integer_to_field_type(?FIELD_TYPE_DOUBLE) ->
  double;
integer_to_field_type(?FIELD_TYPE_NULL) ->
  null;
integer_to_field_type(?FIELD_TYPE_DATE) ->
  date;
integer_to_field_type(?FIELD_TYPE_TIME) ->
  time;
integer_to_field_type(?FIELD_TYPE_DATETIME) ->
  datetime;
integer_to_field_type(?FIELD_TYPE_YEAR) ->
  year;
integer_to_field_type(?FIELD_TYPE_BLOB) ->
  blob;
integer_to_field_type(?FIELD_TYPE_VAR_STRING) ->
  string;
integer_to_field_type(?FIELD_TYPE_STRING) ->
  string;
integer_to_field_type(?FIELD_TYPE_GEOMETRY) ->
  geometry.
