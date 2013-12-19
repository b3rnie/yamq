%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(yamq).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% API
-export([ start_link/1
        , stop/0
        , enqueue/3
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

%%%_* Macros ===========================================================
-define(workers, 10).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { store = throw('store')
           , spids = throw('spids')
           , wpids = throw('wpids')
           , fpids = throw('fpids')
           , heads = throw('heads')
           }).

%%%_ * API -------------------------------------------------------------
start_link(Args) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

stop() ->
  gen_server:call(?MODULE, stop, infinity).

enqueue(X, P, D)
  when (P>=1 andalso P=<8),
       (is_integer(D) andalso D>=0) ->
  gen_server:call(?MODULE, {enqueue, X, P, D}, infinity).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  process_flag(trap_exit, true),
  {ok, Fun}   = s2_lists:assoc(Args, 'fun'),
  {ok, Store} = s2_lists:assoc(Args, 'store'),
  _     = q_init(),
  Heads = q_load(Store),
  Pids  = w_init(Store, Fun),
  {ok, #s{ store = Store
         , wpids = Pids
         , fpids = []
         , spids = []
         , heads = Heads
         }, wait([], Heads)}.

terminate(_Rsn, _S) ->
  ok.

handle_call({enqueue, X, P, D}, From, S) ->
  %% yes, we rely on time always moving forward..
  K     = s2_time:stamp(),
  Store = S#s.store,
  Pid = spawn_link(
          fun() ->
              case Store:put({K,P,D}, X) of
                ok           -> gen_server:reply(From, ok);
                {error, Rsn} -> gen_server:reply(From, {error, Rsn}),
                                %% Riak:
                                %% might still be stored and might
                                %% show up on later reads/index scans.
                                exit(store_failed)
              end
          end),
  {noreply, S#s{spids=[{Pid,{K,P,D}}|S#s.spids]}, wait(S#s.fpids, S#s.heads)};
handle_call(dequeue, From, #s{fpids=[]} = S) ->
  case q_next(S#s.heads) of
    {ok, {{K,P,D}, Heads}} ->
      {reply, {K,P,D}, S#s{heads=Heads}, wait(S#s.fpids, Heads)};
    {error, empty} ->
      {noreply, S#s{fpids=[From]}, wait([From], S#s.heads)}
  end;
handle_call(dequeue, From, S) ->
  FPids = [From|S#s.fpids],
  {noreply, S#s{fpids=FPids}, wait(FPids, S#s.heads)};

handle_call(stop, _From, S) ->
  %% wait for workers
  {stop, normal, ok, S}.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

handle_info({'EXIT', Pid, normal}, S) ->
  {value, {Pid, {K,P,D}}, SPids} = lists:keytake(Pid, 1, S#s.spids),
  %% TODO:
  %% is it due?
  %% do we have a free worker?
  %% if so, hand it directly to worker and skip ets.
  Heads = q_insert(K,P,D,S#s.heads),
  {noreply, S#s{spids=SPids, heads=Heads}, wait(S#s.fpids, Heads)};
handle_info({'EXIT', _Pid, Rsn}, S) ->
  {stop, Rsn, S};
handle_info(timeout, #s{fpids=[F|Fs]} = S) ->
  {ok, {{K,P,D},Heads}} = q_next(S#s.heads),
  gen_server:reply(F, {K,P,D}),
  {noreply, S#s{fpids=Fs, heads=Heads}, wait(Fs, Heads)};
handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S, wait(S#s.fpids, S#s.heads)}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S, wait(S#s.fpids, S#s.heads)}.

%%%_ * Internals gen_server util ---------------------------------------
wait([], _           ) -> infinity;
wait(_,  []          ) -> infinity;
wait(_,  [{_Q,DS}|Hs]) ->
  Min = lists:foldl(fun({_Q, D},Acc) when D < Acc -> D;
                       ({_Q,_D},Acc)              -> Acc
                    end, DS, Hs),
  lists:max([0, ((Min - s2_time:stamp()) div 1000)+1]).

%%%_ * Internals queue -------------------------------------------------
-define(QS, ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8']).

q_init() ->
  lists:foreach(fun(Q) ->
                    Q = ets:new(Q, [ordered_set, private, named_table])
                end, ?QS).

q_load(Store) ->
  {ok, Keys} = Store:list(),
  lists:foldl(fun({K,P,D}, Heads) -> q_insert(K,P,D,Heads) end, [], Keys).

q_insert(K,P,D,Heads0) ->
  Q = q_p2q(P),
  Q = ets:insert_new(Q, {{D, K}, []}),
  case lists:keytake(Q, 1, Heads0) of
    {value, {Q,DP}, Heads} -> [{Q,lists:min([DP|D])}|Heads];
    false                  -> [{Q,D}|Heads0]
  end.

q_next(Heads) ->
  S = s2_time:stamp(),
  case lists:filter(fun({_Q,D}) -> D =< S end, Heads) of
    []    -> {error, empty};
    Ready -> q_take_next(q_pick(Ready), Heads)
  end.

q_pick(Ready) ->
  %% TODO: make this more even more complex to avoid starvation
  %% and pick 1% from q8, 2% from q7 etc.
  [{Q,_}|_] = lists:sort(Ready),
  Q.

q_take_next(Q, Heads) ->
  {D,K} = ets:first(Q),
  case ets:first(Q) of
    '$end_of_table' -> {ok, {{K,q_q2p(Q),D}, lists:keydelete(Q, 1, Heads)}};
    {ND,_NK}        -> {ok, {{K,q_q2p(Q),D}, lists:keyreplace(Q, 1, Heads, {Q,ND})}}
  end.
  

q_q2p('q1') -> 1;
q_q2p('q2') -> 2;
q_q2p('q3') -> 3;
q_q2p('q4') -> 4;
q_q2p('q5') -> 5;
q_q2p('q6') -> 6;
q_q2p('q7') -> 7;
q_q2p('q8') -> 8.

q_p2q(1)    -> 'q1';
q_p2q(2)    -> 'q2';
q_p2q(3)    -> 'q3';
q_p2q(4)    -> 'q4';
q_p2q(5)    -> 'q5';
q_p2q(6)    -> 'q6';
q_p2q(7)    -> 'q7';
q_p2q(8)    -> 'q8'.

%%%_ * Internals workers -----------------------------------------------
w_init(Store, Fun) ->
  lists:map(fun(_) ->
                erlang:spawn_link(fun() -> w_loop(Store, Fun) end)
            end, lists:seq(1, ?workers)).

w_loop(Store, Fun) ->
  {K,P,D} = gen_server:call(?MODULE, dequeue, infinity),
  {ok, X} = Store:get({K,P,D}),
  ok      = Fun(X),
  ok      = Store:delete({K,P,D}),
  w_loop(Store, Fun).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

start_stop_test() ->
  yamq:start_link(),
  yamq:stop().

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
