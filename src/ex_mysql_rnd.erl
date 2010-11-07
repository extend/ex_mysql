-module(ex_mysql_rnd).

-export([new/2,
         next/1, next/2]).

%%% @type context() = {integer(), integer()}.

%% @spec new(integer(), integer()) -> context()
%% @doc Return a new pseudo-random number generator context.
new(Seed1, Seed2) ->
  Mod = (1 bsl 30) - 1,
  {Seed1 rem Mod, Seed2 rem Mod}.

%% @spec next(Context::context()) -> {float(), context()}
%% @doc Return the next pseudo-random number from a given generator.
next({Seed1, Seed2}) ->
  Mod = (1 bsl 30) - 1,
  NewSeed1 = (Seed1 * 3 + Seed2) rem Mod,
  NewSeed2 = (NewSeed1 + Seed2 + 33) rem Mod,
  {NewSeed1 / float(Mod), {NewSeed1, NewSeed2}}.

%% @spec next(context(), integer()) -> {[float()], context()}
%% @doc Return the nth next pseudo-random numbers from a given generator.
next(Context, N) when N >= 0 ->
  next(Context, N, []).

next(Context, 0, List) ->
  {lists:reverse(List), Context};
next(Context, N, Acc) ->
  {Value, NewContext} = next(Context),
  next(NewContext, N - 1, [Value | Acc]).
