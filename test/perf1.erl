%% Very crude performance test to get a rough idea how many things we
%% can keep track of and handle / second

-module(perf1).

-export([run/0]).

run() ->
    {ok, _} = yamq_dets_store:start_link("blah.dets"),
    io:format("Generating 4M keys due within next 6 months spread out on all priorities~n"),
    lists:foreach(fun(N) ->
                          ok = yamq_dets_store:put({s2_time:stamp(), (N rem 8)+1,
                                                    random:uniform(86400 * 30 * 60 * 1000)},
                                                   crypto:rand_bytes(20))
                  end, lists:seq(1, 4000000)),
    io:format("Generating 1M keys due now spread out on all priorities~n"),
    lists:foreach(fun(N) ->
                          ok = yamq_dets_store:put({s2_time:stamp(),(N rem 8)+1,0},crypto:rand_bytes(20))
                  end, lists:seq(1, 1000000)),
    io:format("Starting yamq~n"),
    Daddy = self(),
    {Time0, {ok, _}} = timer:tc(yamq, start_link, [[{store, yamq_dets_store}, {workers, 8}, {'fun', fun(_) -> Daddy ! ok, ok end}]]),
    io:format("Startup took ~pms~n", [Time0 div 1000]),
    {Time1, _} = timer:tc(fun() -> receive_n(1000000) end),

    io:format("Due now processed in ~pms~n", [Time1 div 1000]),
    yamq:stop(),
    yamq_dets_store:stop(),
    ok.

receive_n(0) -> ok;
receive_n(N) ->
    receive _ -> receive_n(N-1) end.

%% Hardware:
%% Thinkpad x230
%% 4xcore i5 2.5GHz
%% 16GB ram
%% Spinning disk
%%
%%  mem usage < 7%
%%
%% 37> perf1:run().
%% Generating 4M keys due within next 6 months spread out on all priorities
%% Generating 1M keys due now spread out on all priorities
%% Starting yamq
%% Startup took 14739ms
%% Due now processed in 124108ms
%% ok
%% 38> 
