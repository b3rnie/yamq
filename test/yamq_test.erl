-module(yamq_test).

-export([run/2]).

run(F0,F1) ->
  {ok, _} = yamq_dets_store:start_link("foo.dets"),
  {ok, _} = yamq:start_link([{'store',   'yamq_dets_store'},
                             {'workers', 4},
                             {'fun',     F0}]),
  F1(),
  yamq_dets_store:stop().

