%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%%   A dirt simple priority queue built on top of ETS.
%%%
%%%   Usage:
%%%     yamq:start_link(
%%%       [{func,    Fun},            %function that handles enqueued tasks
%%%        {workers, 8},              %number of workers
%%%        {store,   yamq_dets_store} %storage
%%%       ]),
%%%
%%%     yamq:enqueue(foo, [{serialize, Z}, %serialize task foo on Z
%%%                        {due,       5000}, %task is due in 5000ms
%%%                       ]),
%%%
%%% @copyright Bjorn Jensen-Urstad 2013
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(yamq).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% api
-export([ start_link/1
        , stop/0
        , enqueue/1
        , enqueue/2
        , size/0
        ]).

%% gen_server
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { store   = throw(store) %user supplied - store to use
           , func    = throw(func)  %user supplied - function to call
           , spids   = []           %store requests
           , wpids   = []           %workers busy
           , wfree   = throw(wfree) %workers free
           , heads   = throw(heads) %mirror head of each queue
           , blocked = dict:new()   %blocked (serialized) tasks
           }).

%%%_ * API -------------------------------------------------------------
start_link(Args) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

stop() ->
  gen_server:call(?MODULE, stop, infinity).

enqueue(Task) ->
  enqueue(Task, []).

enqueue(Task, Options) ->
  gen_server:call(?MODULE, {enqueue, Task, opt_parse(Options)}, infinity).

size() ->
  gen_server:call(?MODULE, size, infinity).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  process_flag(trap_exit, true),
  {ok, Func}  = s2_lists:assoc(Args, func),
  {ok, Store} = s2_lists:assoc(Args, store),
  {ok, N}     = s2_lists:assoc(Args, workers),
  _     = q_init(),
  Heads = q_load(Store),
  ?info("~p items in queue", [q_size(dict:new())]),
  {ok, #s{ store = Store
         , func  = Func
         , wfree = N
         , heads = Heads
         }, wait(N, Heads)}.

terminate(_Rsn, S) ->
  ?info("waiting for ~p workers to finish", [length(S#s.wpids)]),
  ?info("waiting for ~p enqueue requests to finish", [length(S#s.spids)]),
  lists:foreach(fun(Pid) ->
                    receive {'EXIT', Pid, _Rsn} -> ok end
                end,
                S#s.wpids ++ S#s.spids).

handle_call({enqueue, Task, Options}, From, #s{store=Store} = S) ->
  Pid = spawn_store(Store, Task, Options, From),
  {noreply, S#s{spids=[Pid|S#s.spids]}, wait(S#s.wfree, S#s.heads)};
handle_call(size, _From, S) ->
  {reply, q_size(S#s.blocked), S, wait(S#s.wfree, S#s.heads)};
handle_call(stop, _From, S) ->
  {stop, normal, ok, S}.

handle_cast({enqueued, {Pid, Info}}, S) ->
  ?hence(lists:member(Pid, S#s.spids)),
  Heads = q_insert(Info, S#s.heads),
  {noreply, S#s{heads=Heads}, wait(S#s.wfree, Heads)};
handle_cast({done, {Pid, Info}}, S) ->
  ?hence(lists:member(Pid, S#s.wpids)),
  {Heads0, Blocked0} = q_unblock(Info, S#s.heads, S#s.blocked),
  case q_next(Heads0, Blocked0) of
    {ok, {NInfo, Heads, Blocked}} ->
      NPid = spawn_worker(S#s.store, S#s.func, NInfo),
      {noreply, S#s{wpids   = [NPid|S#s.wpids],
                    heads   = Heads,
                    blocked = Blocked}, wait(S#s.wfree, Heads)};
    {empty, {Heads, Blocked}} ->
      {noreply, S#s{wfree   = S#s.wfree + 1,
                    heads   = Heads,
                    blocked = Blocked
                   }, wait(S#s.wfree + 1, Heads)}
  end;
handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

handle_info({'EXIT', Pid, normal}, S) ->
  case {lists:member(Pid, S#s.wpids), lists:member(Pid, S#s.spids)} of
    {true, false} ->
      {noreply, S#s{wpids=S#s.wpids -- [Pid]}, wait(S#s.wfree, S#s.heads)};
    {false, true} ->
      {noreply, S#s{spids=S#s.spids -- [Pid]}, wait(S#s.wfree, S#s.heads)}
  end;
handle_info({'EXIT', Pid, Rsn}, S) ->
  case {lists:member(Pid, S#s.spids), lists:member(Pid, S#s.wpids)} of
    {true, false} ->
      ?warning("writer died: ~p", [Rsn]),
      {stop, {writer_failed, Rsn}, S#s{spids=S#s.spids -- [Pid]}};
    {false, true} ->
      ?warning("worker died: ~p", [Rsn]),
      {stop, {worker_failed, Rsn}, S#s{wpids=S#s.wpids -- [Pid]}}
  end;
handle_info(timeout, S) ->
  ?hence(S#s.wfree > 0),
  case q_next(S#s.heads, S#s.blocked) of
    {ok, {Info, Heads, Blocked}} ->
      Pid   = spawn_worker(S#s.store, S#s.func, Info),
      N     = S#s.wfree - 1,
      {noreply, S#s{wfree   = N,
                    wpids   = [Pid|S#s.wpids],
                    heads   = Heads,
                    blocked = Blocked}, wait(N, Heads)};
    {empty, {Heads, Blocked}} ->
      {noreply, S#s{heads   = Heads,
                    blocked = Blocked}, wait(S#s.wfree, Heads)}
  end;
handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S, wait(S#s.wfree, S#s.heads)}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals queue -------------------------------------------------
-define(QS, [yamq_q1, yamq_q2, yamq_q3, yamq_q4,
             yamq_q5, yamq_q6, yamq_q7, yamq_q8]).

wait(0,      _Heads       ) -> infinity;
wait(_WFree, []           ) -> infinity;
wait(_WFree, [{_QS,DS}|Hs]) ->
  case lists:foldl(fun({_Q, D},Acc) when D < Acc -> D;
                      ({_Q,_D},Acc)              -> Acc
                   end, DS, Hs) of
    0   -> 0;
    Min -> lists:max([0, (Min - s2_time:stamp() div 1000)+1])
  end.

q_init() ->
  lists:foreach(fun(Q) ->
                    Q = ets:new(Q, [ordered_set, private, named_table])
                end, ?QS).

q_load(Store) ->
  {ok, Keys} = Store:list(),
  lists:foldl(fun(Info, Heads) -> q_insert(Info, Heads) end, [], Keys).

q_insert({K,P,D,S}, Heads0) ->
  Queue = q_p2q(P),
  true  = ets:insert_new(Queue, {{D,K},S}),
  case lists:keytake(Queue, 1, Heads0) of
    {value, {Queue,DP}, _Heads} when DP =< D -> Heads0;
    {value, {Queue,DP},  Heads} when DP >= D -> [{Queue,D}|Heads];
    false                                    -> [{Queue,D}|Heads0]
  end.

q_next(Heads0, Blocked0) ->
  S = s2_time:stamp() div 1000,
  case lists:filter(fun({_Q,D}) -> D =< S end, Heads0) of
    []    -> {empty, {Heads0, Blocked0}};
    Ready ->
      {Info, Heads} = q_take_next(q_pick(Ready), Heads0),
      case q_next_is_blocked(Info, Blocked0) of
        {true, Blocked}  ->
          q_next(Heads, Blocked);
        {false, Blocked} ->
          {ok, {Info, Heads, Blocked}}
      end
  end.

q_pick(Ready) ->
  [{Q,_}|_] = lists:sort(Ready), %pick highest priority
  Q.

q_take_next(Q, Heads0) ->
  {D,K}       = ets:first(Q),
  [{{D,K},S}] = ets:lookup(Q, {D,K}),
  true        = ets:delete(Q, {D,K}),
  Heads       = lists:keydelete(Q, 1, Heads0),
  case ets:first(Q) of
    '$end_of_table' -> {{K,q_q2p(Q),D,S}, Heads};
    {ND,_NK}        -> {{K,q_q2p(Q),D,S}, [{Q,ND}|Heads]}
  end.

%% NOTE: This feature is not meant to be heavily used as moving
%% tasks back and forth is quite expensive.
q_next_is_blocked({_,_,_,[]},      Blocked) -> {false, Blocked};
q_next_is_blocked({_,_,_,S }=Info, Blocked) ->
  case dict:is_key(S, Blocked) of
    true  -> {true,  dict:update(S, fun(L) -> [Info|L] end, Blocked)};
    false -> {false, dict:store(S, [], Blocked)}
  end.

q_unblock({_,_,_,[]}, Heads,  Blocked) -> {Heads, Blocked};
q_unblock({_,_,_,S }, Heads0, Blocked) ->
  {lists:foldl(fun(Info, Heads) ->
                   q_insert(Info, Heads)
               end, Heads0, dict:fetch(S, Blocked)),
   dict:erase(S, Blocked)}.

q_q2p(yamq_q1) -> 1;
q_q2p(yamq_q2) -> 2;
q_q2p(yamq_q3) -> 3;
q_q2p(yamq_q4) -> 4;
q_q2p(yamq_q5) -> 5;
q_q2p(yamq_q6) -> 6;
q_q2p(yamq_q7) -> 7;
q_q2p(yamq_q8) -> 8.

q_p2q(1) -> yamq_q1;
q_p2q(2) -> yamq_q2;
q_p2q(3) -> yamq_q3;
q_p2q(4) -> yamq_q4;
q_p2q(5) -> yamq_q5;
q_p2q(6) -> yamq_q6;
q_p2q(7) -> yamq_q7;
q_p2q(8) -> yamq_q8.

q_size(Blocked) ->
  dict:fold(fun(_S,L,N) -> N + length(L) end, 0, Blocked) +
    lists:foldl(fun(Q,N) -> N + ets:info(Q, size) end, 0, ?QS).

%%%_ * Internals options -----------------------------------------------
opt_parse(Options) ->
  lists:ukeysort(1, lists:map(fun opt_validate/1, Options) ++ opt_def()).

opt_validate({priority, P})
  when P >=1, P=<8           -> {priority, P};
opt_validate({due, 0})       -> {due, 0};
opt_validate({due, D})
  when is_integer(D), D>0    -> {due, D + (s2_time:stamp() div 1000)};
opt_validate({serialize, S})
  when S =/= []              -> {serialize, S}.

opt_def() ->
  [{priority,  8},
   {due,       0},
   {serialize, []}
  ].

%%%_ * Internals workers/storage ---------------------------------------
spawn_worker(Store, Fun, Info) ->
  Daddy = erlang:self(),
  erlang:spawn_link(
    fun() ->
        {ok, Task} = Store:get(Info),
        ok         = Fun(Task),
        ok         = Store:delete(Info),
        ok         = gen_server:cast(Daddy, {done, {erlang:self(), Info}})
    end).

spawn_store(Store, Task, Options, From) ->
  Daddy = erlang:self(),
  erlang:spawn_link(
    fun() ->
        {ok, Priority}  = s2_lists:assoc(Options, priority),
        {ok, Due}       = s2_lists:assoc(Options, due),
        {ok, Serialize} = s2_lists:assoc(Options, serialize),
        Info            = {s2_time:stamp(), Priority, Due, Serialize},
        ok   = Store:put(Info, Task),
        ok   = gen_server:cast(Daddy, {enqueued, {erlang:self(), Info}}),
        _    = gen_server:reply(From, ok) %tell caller we are done
    end).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
  yamq_test:run(
    fun() ->
        lists:foreach(fun(N) ->
                          ok = yamq:enqueue({basic, N}, [{priority, N}])
                      end, lists:seq(1, 8)),
        'receive'([{basic, N} || N <- lists:seq(1,8)])
    end).

size_test() ->
  yamq_test:run(
    fun() ->
        0  = yamq:size(),
        ok = yamq:enqueue({size, 1}, [{priority, 1}, {due, 500}]),
        1  = yamq:size(),
        ok = yamq:enqueue({size, 2}, [{priority, 4}, {due, 500}]),
        2  = yamq:size()
    end).

delay1_test() ->
  yamq_test:run(
    fun() ->
        ok = yamq:enqueue({delay, 1}, [{priority, 1}, {due, 2000}]),
        ok = yamq:enqueue({delay, 2}, [{priority, 1}, {due, 1000}]),
        ok = yamq:enqueue({delay, 3}, [{priority, 1}, {due, 3000}]),
        ok = yamq:enqueue({delay, 4}, [{priority, 2}, {due, 500}]),
        ok = yamq:enqueue({delay, 5}, [{priority, 2}, {due, 4000}]),
        receive_in_order([{delay, N} || N <- [4,2,1,3,5]])
    end).

delay2_test() ->
  yamq_test:run(
    fun() ->
        ok = yamq:enqueue({delay2, 1}, [{priority, 1}, {due, 3000}]),
        receive _ -> exit(fail) after 2500 -> ok end,
        receive {delay2,1} -> ok after 1000 -> exit(fail) end
    end).

priority_test() ->
  Daddy = self(),
  yamq_test:run(
    fun() ->
        yamq:enqueue({priority_test, 1}, [{priority, 1}]),
        timer:sleep(100),
        lists:foreach(fun(P) ->
                          yamq:enqueue({priority_test,P}, [{priority, P}])
                      end, lists:reverse(lists:seq(2,8))),
        receive_in_order([{priority_test, N} || N <- lists:seq(1,8)])
    end, [{func, fun(X) -> timer:sleep(400), Daddy ! X, ok end},
          {workers, 1}]).

stop_wait_for_workers_test() ->
  Daddy = self(),
  yamq_test:run(
    fun() ->
        yamq:enqueue({stop_wait_for_workers_test, 1}, [{priority, 1}]),
        timer:sleep(100),
        yamq:stop(),
        receive_in_order([{stop_wait_for_workers_test, 1}])
    end,
    [{func, fun(Msg) -> timer:sleep(1000), Daddy ! Msg, ok end}]).

cover_test() ->
  yamq_test:run(
    fun() ->
        yamq ! foo,
        yamq ! timeout,
        ok = sys:suspend(yamq),
        ok = sys:change_code(yamq, yamq, 0, []),
        ok = sys:resume(yamq),
        timer:sleep(100)
    end, [{func, fun(X) -> exit(X) end}]).

queue_test_() ->
  F = fun() ->
          yamq_test:run(
            fun() ->
                L = lists:foldl(
                      fun(N, Acc) ->
                          P  = random:uniform(8),
                          D  = random:uniform(5000),
                          ok = yamq:enqueue({queue_test, N}, [{priority,P},
                                                              {due,D}]),
                          [{queue_test, N}|Acc]
                      end, [], lists:seq(1, 1000)),
                'receive'(L)
            end, [{'workers', 8}])
      end,
  {timeout, 30, F}.

init_test() ->
  {ok, _} = yamq_dets_store:start_link("x.dets"),
  lists:foreach(fun(N) ->
                    K  = s2_time:stamp(),
                    P  = random:uniform(8),
                    D  = random:uniform(1000),
                    ok = yamq_dets_store:put({K,P,D,[]}, N)
                end, lists:seq(1, 100)),
  {ok, _} = yamq:start_link([{workers, 1},
                             {func, fun(_) -> ok end},
                             {store, yamq_dets_store}]),
  ok = yamq:stop(),
  ok = yamq_dets_store:stop().

crash_test() ->
  erlang:process_flag(trap_exit, true),
  {ok, Pid1} = yamq_dets_store:start_link("x.dets"),
  {ok, Pid2} = yamq:start_link([{workers, 4},
                                {func, fun(Msg) -> exit(Msg) end},
                                {store, yamq_dets_store}]),
  ok = yamq:enqueue(oh_no, [{priority, 1}]),
  receive {'EXIT', Pid2, {worker_failed, oh_no}} -> ok end,
  ok = yamq_dets_store:stop(),
  receive {'EXIT', Pid1, normal} -> ok end,
  erlang:process_flag(trap_exit, false),
  ok.

store_fail_test() ->
  erlang:process_flag(trap_exit, true),
  {ok, _}     = yamq_dets_store:start_link("x.dets"),
  {ok, Pid}   = yamq:start_link([{workers, 4},
                                 {func, fun(_) -> ok end},
                                 {store, yamq_dets_store}]),
  ok          = yamq_dets_store:stop(),
  {'EXIT', _} = (catch yamq:enqueue(store_fail_test, [{priority, 1}])),
  receive {'EXIT', Pid, {writer_failed, _}} -> ok end,
  erlang:process_flag(trap_exit, false),
  ok.

serialize_test() ->
  yamq_test:run(
    fun() ->
        yamq:enqueue(serialize_test1, [{serialize, foo}, {priority, 3}]),
        yamq:enqueue(serialize_test2, [{serialize, foo}, {priority, 2}]),
        yamq:enqueue(serialize_test3, [{serialize, foo}, {priority, 1}]),
        timer:sleep(1000)
    end, [{func, fun(_X) ->
                     erlang:register(serialize_test, self()),
                     timer:sleep(200),
                     erlang:unregister(serialize_test),
                     timer:sleep(100),
                     ok
                 end},
          {workers, 4}
         ]).

bad_options_test() ->
  yamq_test:run(
    fun() ->
        {'EXIT', _} = (catch yamq:enqueue(foo, [{due, -1}])),
        {'EXIT', _} = (catch yamq:enqueue(bar, [{serialize, []}])),
        {'EXIT', _} = (catch yamq:enqueue(baz, [{priority, 0}])),
        {'EXIT', _} = (catch yamq:enqueue(buz, [{priority, 9}]))
    end).

'receive'([]) -> ok;
'receive'(L) ->
  receive X ->
      ?hence(lists:member(X, L)),
      'receive'(L -- [X])
  end.

receive_in_order(L) ->
  lists:foreach(fun(X) -> X = receive Y -> Y end end, L).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
