-module(yamq_test).

-export([run/2]).
-export([run/3]).

run(F0,F1) ->
    do_run(F1,def(F0, [])).

run(F0,F1,Args) ->
    do_run(F1,def(F0,Args)).

do_run(F,Args) ->
  {ok, _} = yamq_dets_store:start_link("foo.dets"),
  {ok, _} = yamq:start_link(Args),
  F(),
  catch yamq:stop(),
  catch yamq_dets_store:stop().

def(F,A) ->
    lists:ukeysort(1, A ++ [{'store',   'yamq_dets_store'},
                            {'workers', 4},
                            {'fun',     F}]).
