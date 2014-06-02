%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%%   loadbalancer/circuitbreaker
%%%
%%% @copyright Bjorn Jensen-Urstad 2014
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(gen_lb).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% api
-export([ start_link/2
        , stop/1
        , call/2
        , block/2
        , unblock/2
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

%%%_* Behaviour ========================================================
-callback exec(any(), list()) -> {ok, _} | {error, _}.

%%%_* Macros ===========================================================
%% Number of attepts done, will always hit atleast two different servers
-define(CALL_ATTEMPTS, 2).

%% Interval on which failures are calculated (in seconds)
-define(AUTOBLOCK_INTERVAL, 10).

%% Number of errors before node will be taken out of cluster
-define(AUTOBLOCK_ERRORS, 2).

%% Number of crashes before node will be taken out of cluster
-define(AUTOBLOCK_CRASHES,  1).

%% Seconds a blocked node will be kept out of cluster
-define(AUTOBLOCK_REINSERT, 15).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(r, { attempts     = 1
           , args         = throw(args)
           , from         = throw(from)
           , node         = throw(node)
           }).

-record(s, { cluster_up   = throw(cluster_up)
           , cluster_down = throw(cluster_down)
           , cb_mod       = throw(cb_mod)
           , tref         = throw(tref)
           , reqs         = dict:new()
           }).

%%%_ * API -------------------------------------------------------------
start_link(Name, Args) ->
  gen_server:start_link({local, Name}, ?MODULE, Args, []).

stop(Ref) ->
  gen_server:call(Ref, stop, infinity).

call(Ref, Args) ->
  gen_server:call(Ref, {call, Args}, infinity).

block(Ref, Node) ->
  gen_server:call(Ref, {block, Node}, infinity).

unblock(Ref, Node) ->
  gen_server:call(Ref, {unblock, Node}, infinity).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  process_flag(trap_exit, true),
  {ok, Cluster} = s2_lists:assoc(Args, cluster),
  {ok, CbMod}   = s2_lists:assoc(Args, cb_mod),
  {ok, TRef}    = timer:send_interval(1000, maybe_unblock),
  {ok, #s{ cluster_up   = [{Node,[]} || Node <- Cluster]
         , cluster_down = []
         , cb_mod       = CbMod
         , tref         = TRef
         }}.

terminate(_Rsn, S) ->
  {ok, cancel} = timer:cancel(S#s.tref),
  lists:foreach(fun(Pid) ->
                    receive {'EXIT', Pid, _Rsn} -> ok end
                end, dict:fetch_keys(S#s.reqs)).

handle_call({call, _Args}, _From, #s{cluster_up=[]} = S) ->
  {reply, {error, cluster_down}, S};
handle_call({call, Args}, From, #s{cluster_up=[{Node,Info}|Up]} = S) ->
  Pid = do_call(S#s.cb_mod, Args, From, Node),
  {noreply, S#s{ cluster_up = Up ++ [{Node,Info}] %round robin lb
               , reqs       = dict:store(Pid, #r{ from = From
                                                , args = Args
                                                , node = Node},
                                         S#s.reqs)
               }};
handle_call({block, Node}, _From, S) ->
  case lists:keytake(Node, 1, S#s.cluster_up) of
    {value, {Node, _Info}, Up} ->
      {reply, ok, S#s{ cluster_up   = Up
                     , cluster_down = [{Node,inf}|S#s.cluster_down]}};
    false ->
      case lists:keytake(Node, 1, S#s.cluster_down) of
        {value, {Node, _Time}, Down} ->
          {reply, ok, S#s{cluster_down = [{Node,inf}|Down]}};
        false ->
          {reply, {error, not_member}, S}
      end
  end;
handle_call({unblock, Node}, _From, S) ->
  case lists:keytake(Node, 1, S#s.cluster_down) of
    {value, {Node, _Time}, Down} ->
      {reply, ok, S#s{ cluster_up   = [{Node,[]}|S#s.cluster_up]
                     , cluster_down = Down}};
    false ->
      case lists:keymember(Node, 1, S#s.cluster_up) of
        true  -> {reply, {error, not_blocked}, S};
        false -> {reply, {error, not_member}, S}
      end
  end;
handle_call(stop, _From, S) ->
  {stop, normal, ok, S}.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

handle_info({'EXIT', Pid, normal}, S) ->
  {noreply, S#s{reqs=dict:erase(Pid, S#s.reqs)}};
handle_info({'EXIT', Pid, {Type, Rsn}}, S)
  when Type =:= call_error;
       Type =:= call_crash ->
  Req0  = dict:fetch(Pid, S#s.reqs),
  Reqs  = dict:erase(Pid, S#s.reqs),
  Up0   = cluster_add_failure(S#s.cluster_up, Req0#r.node, Type),
  Up1   = cluster_prune_failures(Up0),
  {Up2, Down} = cluster_maybe_block(Up1, S#s.cluster_down),
  case Req0#r.attempts < ?CALL_ATTEMPTS of
    true ->
      %% make sure we dont hit the same node twice in a row
      Up3 = case Up2 of
              [{Node1,Info1},{Node2,Info2}|Nodes]
                when Node1 =:= Req0#r.node ->
                [{Node2,Info2},{Node1,Info1}|Nodes];
              Up2 -> Up2
            end,
      case Up3 of
        [] ->
          %% everything is down
          gen_server:reply(Req0#r.from, {error, Rsn}),
          {noreply, S#s{ cluster_up   = Up2 %no need to use reordered
                       , cluster_down = Down
                       , reqs         = Reqs}};
        [{Node,Info}|Up] ->
          NewPid = do_call(S#s.cb_mod, Req0#r.args, Req0#r.from, Node),
          Req = Req0#r{ attempts = Req0#r.attempts + 1
                      , node     = Node
                      },
          {noreply, S#s{ cluster_up   = Up ++ [{Node,Info}] %round robin
                       , cluster_down = Down
                       , reqs         = dict:store(NewPid, Req, Reqs)
                       }}
      end;
    false ->
      gen_server:reply(Req0#r.from, {error, Rsn}),
      {noreply, S#s{ cluster_up   = Up2
                   , cluster_down = Down
                   , reqs         = Reqs
                   }}
  end;
handle_info({'EXIT', Pid, Rsn}, S) ->
  {stop, Rsn, S#s{reqs=dict:erase(Pid, S#s.reqs)}};
handle_info(maybe_unblock, S) ->
  {Up, Down} = cluster_maybe_unblock(S#s.cluster_up, S#s.cluster_down),
  {noreply, S#s{ cluster_up   = Up
               , cluster_down = Down}};
handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
do_call(CbMod, Args, From, Node) ->
  erlang:spawn_link(
    fun() ->
        Daddy = erlang:self(),
        Ref   = erlang:make_ref(),
        {Pid, MRef} =
          erlang:spawn_monitor(
            fun() ->
                case CbMod:exec(Node, Args) of
                  {ok, Res}    -> Daddy ! {Ref, {ok, Res}};
                  {error, Rsn} -> Daddy ! {Ref, {error, Rsn}}
                end
            end),
        receive
          {Ref, {ok, Res}} ->
            gen_server:reply(From, {ok, Res});
          {Ref, {error, Rsn}} ->
            exit({call_error, Rsn});
          {'DOWN', MRef, process, Pid, Rsn} ->
            exit({call_crash, Rsn})
        end
    end).

cluster_add_failure(Up, Node, Type) ->
  case lists:keyfind(Node, 1, Up) of
    {Node, Info} ->
      Now = s2_time:stamp() div 1000,
      lists:keyreplace(Node, 1, Up, {Node, [{Type, Now}|Info]});
    false ->
      %% already blocked
      Up
  end.

cluster_prune_failures(Up0) ->
  Then = (s2_time:stamp() div 1000) - ?AUTOBLOCK_INTERVAL * 1000,
  lists:map(fun({Node, Info}) ->
                {Node, lists:filter(
                         fun({_Type,  Time}) when Time < Then -> false;
                            ({_Type, _Time})                  -> true
                         end, Info)}
            end, Up0).

cluster_maybe_block(Up0, Down) ->
  {Up, NewDown} =
    lists:partition(
      fun({Node, Info}) ->
          case lists:foldl(fun({call_error, _}, {E,C}) -> {E+1,C};
                              ({call_crash, _}, {E,C}) -> {E,  C+1}
                           end, {0, 0}, Info) of
            {E, _C} when E >= ?AUTOBLOCK_ERRORS ->
              ?warning("blocking ~p (too many errors)", [Node]),
              false;
            {_E, C} when C >= ?AUTOBLOCK_CRASHES ->
              ?warning("blocking ~p (too many crasches)", [Node]),
              false;
            {_, _} ->
              true
          end
      end, Up0),
  Now = s2_time:stamp() div 1000,
  {Up, Down ++ [{Node, Now} || {Node, _Info} <- NewDown]}.

cluster_maybe_unblock(Up, Down) ->
  Timeout = (s2_time:stamp() div 1000) - ?AUTOBLOCK_REINSERT * 1000,
  {NewUp, NewDown} =
    lists:partition(
      fun({_Node, inf  })      -> false;
         ({ Node, Time })
          when Time < Timeout  -> ?warning("unblocking ~p", [Node]),
                                  true;
         ({_Node, _Time})      -> false
      end, Down),
  {Up ++ [{Node, []} || {Node, _Time} <- NewUp], NewDown}.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

manual_block_test() ->
  {ok, Ref} = start_link(manual_block,
                         [{cb_mod, gen_lb_test},
                          {cluster, [foo, bar]}]),
  ok                    = block(Ref, foo),
  ok                    = block(Ref, bar),
  ok                    = block(Ref, bar),
  {error, not_member}   = block(Ref, baz),
  {error, cluster_down} = call(Ref, [ok]),
  ok                    = unblock(Ref, foo),
  {error, not_blocked}  = unblock(Ref, foo),
  {error, not_member}   = unblock(Ref, buz),
  {ok, ok}              = call(Ref, [ok]),
  ok                    = stop(Ref).

automatic_block_test() ->
  {ok, Ref} = start_link(automatic_block,
                         [{cb_mod, gen_lb_test},
                          {cluster, [foo, bar]}]),
  {error, error}        = call(Ref, [error]),
  {error, error}        = call(Ref, [error]),
  {error, cluster_down} = call(Ref, [ok]),
  ok                    = stop(Ref).

automatic_unblock_test_() ->
  {timeout, 660,
   fun() ->
       {ok, Ref} = start_link(automatic_unblock,
                              [{cb_mod, gen_lb_test},
                               {cluster, [baz, buz]}]),
       ok                    = block(Ref, baz),
       {error, error}        = call(Ref, [error]),
       {error, cluster_down} = call(Ref, [ok]),
       timer:sleep(20000),
       {ok, ok}              = call(Ref, [ok]),
       {error, error}        = call(Ref, [error]),
       {error, cluster_down} = call(Ref, [ok]),
       ok                    = stop(Ref)
   end}.

retry_test() ->
  {ok, Ref} = start_link(retry,
                         [{cb_mod, gen_lb_test},
                          {cluster, [crash, good]}]),
  {ok, ok}  = call(Ref, [ok]),
  ok        = stop(Ref).

failures_expire_test_() ->
  {timeout, 60,
   fun() ->
       {ok, Ref} = start_link(failures_expire,
                              [{cb_mod, gen_lb_test},
                               {cluster, [foo, bar]}]),
       {error, error} = call(Ref, [error]),
       timer:sleep(15000),
       {error, error} = call(Ref, [error]),
       {ok,ok}        = call(Ref, [ok]),
       ok = stop(Ref)
   end}.

bad_cast_test() ->
  process_flag(trap_exit, true),
  {ok, Ref} = start_link(bad_cast,
                         [{cb_mod, gen_lb_test}, {cluster, []}]),
  ok        = gen_server:cast(Ref, foo),
  receive {'EXIT', Ref, {bad_cast, foo}} -> ok end,
  process_flag(trap_exit, false),
  ok.

stray_msg_test() ->
  {ok, Ref} = start_link(stray_msg,
                         [{cb_mod, gen_lb_test}, {cluster, [foo]}]),
  Ref ! oops,
  {ok, ok} = call(Ref, [ok]),
  ok       = stop(Ref).

-else.
-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
