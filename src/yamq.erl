%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(yamq).

%%%_* Exports ==========================================================
%% API
-export([ start_link/1
        , stop/0
        , enqueue/3
        ]).

%%%_* Includes =========================================================
-record(s, { store = throw('store')
           , spids = throw('spids')
           , wpids = throw('wpids')
           }).

%%%_* Macros ===========================================================
-define(queues,  ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8']).
-define(workers, 10).
%%%_* Code =============================================================
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
  _    = queues_init(),
  _    = queues_load(Store),
  Pids = workers_init(Store, Fun),
  {ok, #s{ store = Store
         , wpids = Pids
         , spids = []
         }}.

terminate(_Rsn, S) ->
  ok.

handle_call({enqueue, X, P, D}, From, S) ->
  Pid = spawn_link(
          fun() ->
              case Store:put() of
                ok           -> gen_server:reply(From, ok);
                {error, Rsn} -> gen_server:reply(From, {error, Rsn}),
                                %% Riak:
                                %% might still be stored and might
                                %% show up on later reads/index scans.
                                exit(store_failed)
              end),
  {noreply, S#s{spids=[{Pid,{X,P,D}}|S#s.spids]}};
handle_call(dequeue, From, S) ->
  %% block until there is something to do
  {reply, X, S};
handle_call(stop, _From, S) ->
  %% wait for workers
  {stop, normal, ok, S}.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

handle_info({'EXIT', Pid, normal}, S) ->
  {value, {Pid, {X,P,D}}, SPids} = lists:keytake(Pid, 1, S#s.spids),
  _ = queues_insert(K,P,D),
  {noreply, S#s{spids=SPids}};
handle_info({'EXIT', Pid, Rsn}, S) ->
  {stop, Rsn, S};
handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

%%%_ * Internals queue -------------------------------------------------
%%  Bin = hipe_bifs:bytearray(8, 0),
%%  hipe_bifs:bytearray_update(Bin, 0, 10),
queues_init() ->
  lists:foreach(fun(Q) ->
                    Q = ets:new(Q, [ordered_set, private, named_table])
                end, ?queues).

queues_load(Store) ->
  {ok, Keys} = Store:list(),
  lists:foreach(fun({K,P,D}) ->
                    queues_insert(K,P,D)
                end, Store:list()).

queues_insert(K,P,D) ->
  Q = which_queue(P),
  Q = ets:insert_new(Q, {{D, K}, []}).

queues_next([Q|Qs]) ->
  case ets:first(Q) of
    '$end_of_table' -> queues_next(Qs);
    {D,K} ->
      %% take first due thing, start with prio 1
      %% TODO: make this more even more complex to avoid starvation
      %% and pick 1% from q8, 2% from q7 etc.
      ok
  end;
queues_next([]) ->
  {error, empty}.

q2p('q1') -> 1;
q2p('q2') -> 2;
q2p('q3') -> 3;
q2p('q4') -> 4;
q2p('q5') -> 5;
q2p('q6') -> 6;
q2p('q7') -> 7;
q2p('q8') -> 8.

p2q(1) -> 'q1';
p2q(2) -> 'q2';
p2q(3) -> 'q3';
p2q(4) -> 'q4';
p2q(5) -> 'q5';
p2q(6) -> 'q6';
p2q(7) -> 'q7';
p2q(8) -> 'q8'.

%%%_ * Internals workers -----------------------------------------------
workers_init(Store, Fun) ->
  lists:map(fun(_) ->
                erlang:spawn_link(fun() -> worker_loop(Store, Fun) end)
            end, lists:seq(1, ?workers)],


worker_loop(Store, Fun) ->
  {X = gen_server:call(?MODULE, dequeue, infinity),
  Fun(X),
  worker_loop(Fun).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
