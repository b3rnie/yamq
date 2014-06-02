-module(gen_lb_test).
-behaviour(gen_lb).

-export([exec/2]).

exec(crash, _Args)   -> exit(crash);
exec(_Node, [ok])    -> {ok, ok};
exec(_Node, [error]) -> {error, error}.
