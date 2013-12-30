%%%_* Module declaration ===============================================
-module(yamq_test).

%%%_* Exports ==========================================================
-export([run/1]).
-export([run/2]).

run(F)   -> do_run(F, def_args()).
run(F,A) -> do_run(F, lists:ukeysort(1, A++def_args())).

do_run(F,A) ->
  {ok, _} = yamq_dets_store:start_link("foo.dets"),
  {ok, _} = yamq:start_link(A),
  F(),
  catch yamq:stop(),
  catch yamq_dets_store:stop().


def_args() ->
  Daddy = self(),
  [{'store',   'yamq_dets_store'},
   {'workers', 4},
   {'func',    fun(X) -> Daddy ! X, ok end}].


%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
