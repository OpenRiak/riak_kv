-module(riak_kv_http_cache).

-export([start_link/0,
	 get_stats/1]).

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-record(st,
    {
        ts :: undefined|erlang:timestamp(),
        stats = []
    }
).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_stats(Milliseconds :: pos_integer()) -> list({atom(), term()}).
get_stats(Timeout) ->
    gen_server:call(?MODULE, get_stats, Timeout).

init(_) ->
    {ok, #st{}}.

handle_call(get_stats, _From, #st{} = S) ->
    #st{stats = Stats} = S1 = check_cache(S),
    {reply, Stats, S1}.

handle_cast(_, S) ->
    {noreply, S}.

handle_info(_, S) ->
    {noreply, S}.

terminate(_, _) ->
    ok.

code_change(_, S, _) ->
    {ok, S}.

check_cache(#st{ts = undefined} = S) ->
    Stats = riak_kv_status:get_stats(web),
    S#st{ts = os:timestamp(), stats = Stats};
check_cache(#st{ts = Then} = S) ->
    CacheTime =
        application:get_env(riak_kv, http_stats_cache_milliseconds, 1000),
    Now = os:timestamp(),
    case timer:now_diff(Now, Then) < (CacheTime * 1000) of
        true ->
            S;
        false ->
            Stats = riak_kv_status:get_stats(web),
            S#st{ts = os:timestamp(), stats = Stats}
    end.