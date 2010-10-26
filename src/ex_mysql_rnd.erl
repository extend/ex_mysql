-module(ex_mysql_rnd).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([new/2]).
-export([next/1, next/2]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

new(Seed1, Seed2) ->
  Mod = (1 bsl 30) - 1,
  {Seed1 rem Mod, Seed2 rem Mod}.

next({Seed1, Seed2}) ->
  Mod = (1 bsl 30) - 1,
  NewSeed1 = (Seed1 * 3 + Seed2) rem Mod,
  NewSeed2 = (NewSeed1 + Seed2 + 33) rem Mod,
  {NewSeed1 / float(Mod), {NewSeed1, NewSeed2}}.

next(Context, N) when N >= 0 ->
  next(Context, N, []).

next(Context, 0, List) ->
  {lists:reverse(List), Context};
next(Context, N, Acc) ->
  {Value, NewContext} = next(Context),
  next(NewContext, N - 1, [Value | Acc]).
