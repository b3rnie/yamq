%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(yamq_dets_store).
-behaviour(yamq_store).

%%%_* Exports ==========================================================
-export([ start_link/1
        , stop/0
        ]).

-export([ get/1
        , put/2
        , delete/1
        , list/0
        ]).

-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
start_link(File) -> gen_server:start_link({local, ?MODULE}, ?MODULE, [File], []).
stop()           -> gen_server:call(?MODULE, stop, infinity).
get(K)           -> gen_server:call(?MODULE, {get, K}, infinity).
put(K,V)         -> gen_server:call(?MODULE, {put, K,V}, infinity).
delete(K)        -> gen_server:call(?MODULE, {delete, K}, infinity).
list()           -> gen_server:call(?MODULE, list, infinity).

%%%_ * gen_server ------------------------------------------------------
init([File]) ->
  {ok, dets} = dets:open_file(dets, [{file, File}]),
  {ok, File}.

terminate(_Rsn, File) ->
  ok = dets:close(dets),
  ok = file:delete(File).

handle_call({get,K}, _From, S) ->
  case dets:lookup(dets, K) of
    [{K,V}] -> {reply, {ok, V}, S};
    []      -> {reply, {error, notfound}, S}
  end;
handle_call({put,K,V}, _From, S) ->
  true = dets:insert_new(dets,{K,V}),
  {reply, ok, S};
handle_call({delete, K}, _From, S) ->
  dets:delete(dets, K),
  {reply, ok, S};
handle_call(list, _From, S) ->
  L = dets:select(dets, [{{'$1','_'},[],['$1']}]),
  {reply, {ok, L}, S};
handle_call(stop, _From, S) ->
  {stop, normal, ok, S}.

handle_cast(Msg, S)             -> {stop, {bad_cast, Msg}, S}.
handle_info(_Msg, S)            -> {noreply, S}.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-else.
-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
