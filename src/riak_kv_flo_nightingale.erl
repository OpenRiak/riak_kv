%% -------------------------------------------------------------------
%%
%% riak_kv_flo_nightingale: gathering of stats with counters/atomics
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% Flo Nightingale gathers statistics.

%% Every statistic has a name (e.g. riak_kv_put_fsm_time), and a type.  There
%% are three types supported rate (like counter in exometer), count (like
%% spiral in exometer) and lossy_histogram (the equivalent to histogram).
%%
%% A set of stats is a set with a single type, but one or more names.  A set
%% will use a common array of atomics or counters.  It is assumed there may be
%% value in separating very busy stats of the same type on to different arrays.
%%
%% Every individual stat (i.e. {Type, Name} combination) will have a Lamp which
%% is used by Flo Nightingale to help gather the stats.  The Lamp will tick
%% every 1s, and find the current value of the stats and send the result back
%% to Flo Nightingale for collation.
%%
%% When stats are required, the last 60s of stats for each {Type, Name}
%% combination are summarised into the required outputs:
%% - _active and _60s and for rate (the current number of active processes,
%% and the average number over the past 60s).
%% - . and _total for count (the total seen in the last 60s and the total seen
%% over all time)
%% - _mean, _median, _95, _99, _100 for lossy_histogram (approximations of
%% mean median and percentiles based on sampling over the period)
%%
%% The tick, width and depth are configurable:
%% - `tick` is how long between a lamp taking a reading - and defaults to 1s.
%% - `width` is how long the period over which each non-total stat is produced,
%% and defaults to 60 slots of tick (i.e. one minute).
%% - `depth` is how many slots are used in each histogram, the bigger the
%% depth, the less lossy the histogram, and it defaults to 100.
%%
%% The histogram is lossy, in that for each tick every timing will be in the
%% final histogram if CountPerTick =< Depth.  Otherwise, every timing will
%% have an equal chance of Depth / CountPerTick of appearing in the final
%% histogram calculation.

-module(riak_kv_flo_nightingale).

-behaviour(gen_server).

%% gen_server callbacks
-export(
    [
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        handle_continue/2,
        terminate/2
    ]
).

-export(
    [
        start_link/0,
        stop/0,
        update_rate/2,
        update_count/1,
        update_lossyhistogram/2,
        update/3,
        get_stats/0,
        get_tick/1,
        get_width/1,
        get_depth/1
    ]
).

-define(DEFAULT_HISTOGRAM_SIZE, 100).
-define(DEFAULT_TIME_SLICES, 60).
-define(DEFAULT_TICK, 1000).

-define(SUFFIX_RATE_MEAN, <<"_60s">>).
-define(SUFFIX_COUNT_TOTAL, <<"_total">>).
-define(SUFFIX_HIST_100, <<"_100">>).
-define(SUFFIX_HIST_99, <<"_99">>).
-define(SUFFIX_HIST_95, <<"_95">>).
-define(SUFFIX_HIST_MEDIAN, <<"_median">>).
-define(SUFFIX_HIST_MEAN, <<"_mean">>).

-record(state, {
    time_slices = ?DEFAULT_TIME_SLICES ::
        pos_integer(),
    stat_result_map = maps:new() ::
        #{{stat_type(), atom()} => stat_results()},
    lamp_list = [] ::
        list(lamp_ref()),
    housekeep_tick = ?DEFAULT_TIME_SLICES * 1000 ::
        pos_integer()
}).

-type stat_type() :: rate | count | lossy_histogram.
-type stat_set() :: {stat_type(), list(atom())}.
-type lamp_ref() :: {stat_type(), atom(), pid()}.
-type rate_result() :: {rate, list(non_neg_integer())}.
-type count_result() :: {count, {list(non_neg_integer()), non_neg_integer()}}.
-type histo_result() :: {lossy_histogram, list(list(pos_integer()))}.
-type stat_results() ::
    rate_result() | count_result() | histo_result().
-type stat_output() :: list({atom(), non_neg_integer()}).

-type option() ::
    {width, pos_integer()}
    | {tick, pos_integer()}
    | {depth, pos_integer()}.

-export_type([stat_type/0, stat_set/0, option/0]).

%%%============================================================================
%%% External API
%%%============================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    {ok, Pid} =
        gen_server:start_link(
            {local, ?MODULE},
            ?MODULE,
            {riak_kv_stat:flo_stats(), []},
            []
        ),
    {ok, Pid}.

%% @doc Used by process to generate a stat change
update_rate(Name, starting) ->
    nobadarg(
        fun() ->
            {Ref, Idx} = persistent_term:get({?MODULE, rate, Name}),
            counters:add(Ref, Idx, 1)
        end
    );
update_rate(Name, stopping) ->
    nobadarg(
        fun() ->
            {Ref, Idx} = persistent_term:get({?MODULE, rate, Name}),
            counters:sub(Ref, Idx, 1)
        end
    ).

%% @doc Used by process to generate a stat change
update_count(Name) ->
    nobadarg(
        fun() ->
            {Ref, Idx} = persistent_term:get({?MODULE, count, Name}),
            counters:add(Ref, Idx, 1)
        end
    ).

%% @doc Used by process to generate a stat change
update_lossyhistogram(Name, Timing) ->
    nobadarg(
        fun() ->
            {Ref, Idx, Depth, Lamp} =
                persistent_term:get({?MODULE, lossy_histogram, Name}),
            case atomics:add_get(Ref, Idx, 1) of
                Count when Count =< Depth ->
                    riak_kv_flo_lamp:add_timing(Lamp, Timing, Count);
                Count ->
                    case rand:uniform(Count) of
                        Slice when Slice =< Depth ->
                            riak_kv_flo_lamp:add_timing(Lamp, Timing, Slice);
                        _Slice ->
                            ok
                    end
            end
        end
    ).

%% @doc Used by a Lamp to update Flo Nightingale
-spec update(stat_type(), atom(), list(pos_integer()) | pos_integer()) -> ok.
update(Type, Name, Update) ->
    gen_server:cast(?MODULE, {update, Type, Name, Update}).

%% @doc Calculate and output all stats
-spec get_stats() -> list({binary(), non_neg_integer()}).
get_stats() ->
    gen_server:call(?MODULE, get_stats, infinity).

-spec get_tick(list(option())) -> pos_integer().
get_tick(Options) ->
    get_option(Options, tick, flo_tick, ?DEFAULT_TICK).

-spec get_width(list(option())) -> pos_integer().
get_width(Options) ->
    get_option(Options, width, flo_width, ?DEFAULT_TIME_SLICES).

-spec get_depth(list(option())) -> pos_integer().
get_depth(Options) ->
    get_option(Options, depth, flo_depth, ?DEFAULT_HISTOGRAM_SIZE).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init({RequiredStatSets, Options}) ->
    UpdatedOptions = lists:ukeysort(1, [{tick, get_tick(Options)} | Options]),
    LampList = setup_stats(RequiredStatSets, UpdatedOptions),
    ResultMap =
        lists:foldl(
            fun({Type, Name, _Pid}, AccMap) ->
                InitValue =
                    case Type of
                        count ->
                            {count, {[], 0}};
                        rate ->
                            {rate, []};
                        lossy_histogram ->
                            {lossy_histogram, [[]]}
                    end,
                maps:put({Type, Name}, InitValue, AccMap)
            end,
            maps:new(),
            LampList
        ),
    {
        ok,
        #state{stat_result_map = ResultMap, lamp_list = LampList},
        {continue, {schedule_tick, UpdatedOptions}}
    }.

handle_continue({schedule_tick, Options}, State) ->
    TimeSlices = get_width(Options),
    Interval = get_tick(Options),
    HousekeepTick = TimeSlices * Interval,
    erlang:send_after(HousekeepTick, self(), house_keeping),
    {
        noreply,
        State#state{time_slices = TimeSlices, housekeep_tick = HousekeepTick}
    }.

handle_call(get_stats, _From, State) ->
    Results =
        lists:map(
            fun({{Type, Name}, Results}) ->
                produce_result({Type, Name}, Results, State#state.time_slices)
            end,
            maps:to_list(State#state.stat_result_map)
        ),
    {
        reply,
        lists:sort(lists:flatten(Results)),
        State
    }.

handle_cast({update, rate, Name, LatestRate}, State) ->
    {
        noreply,
        State#state{
            stat_result_map =
                maps:update_with(
                    {rate, Name},
                    fun({rate, RL}) -> {rate, [LatestRate | RL]} end,
                    State#state.stat_result_map
                )
        }
    };
handle_cast({update, count, Name, LatestCount}, State) ->
    {
        noreply,
        State#state{
            stat_result_map =
                maps:update_with(
                    {count, Name},
                    fun({count, {RL, T}}) ->
                        {count, {[LatestCount | RL], T + LatestCount}}
                    end,
                    State#state.stat_result_map
                )
        }
    };
handle_cast({update, lossy_histogram, Name, ResultList}, State) ->
    {
        noreply,
        State#state{
            stat_result_map =
                maps:update_with(
                    {lossy_histogram, Name},
                    fun({lossy_histogram, ListOfRL}) ->
                        {lossy_histogram, [ResultList | ListOfRL]}
                    end,
                    State#state.stat_result_map
                )
        }
    }.

handle_info(house_keeping, State) ->
    erlang:send_after(State#state.housekeep_tick, self(), house_keeping),
    TrimmedResults =
        maps:map(
            fun(_K, V) ->
                housekeep_results(V, State#state.time_slices)
            end,
            State#state.stat_result_map
        ),
    {noreply, State#state{stat_result_map = TrimmedResults}}.

terminate(_Reason, State) ->
    lists:foreach(
        fun(LampRef) -> gen_server:stop(element(3, LampRef)) end,
        State#state.lamp_list
    ),
    ok.

%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec setup_stats(list(stat_set()), list(option())) -> list(lamp_ref()).
setup_stats(StatNeeds, Options) ->
    setup_stats(StatNeeds, Options, []).

setup_stats([], _Options, Acc) ->
    Acc;
setup_stats([{lossy_histogram, Names} | Rest], Options, Acc) ->
    Ref = atomics:new(length(Names), [{signed, false}]),
    HistogramSlots = get_depth(Options),
    LampList =
        lists:map(
            fun({Name, I}) ->
                {ok, Pid} =
                    riak_kv_flo_lamp:start_link(
                        lossy_histogram,
                        Name,
                        {Ref, I},
                        Options
                    ),
                persistent_term:put(
                    {?MODULE, lossy_histogram, Name},
                    {Ref, I, HistogramSlots, Pid}
                ),
                {lossy_histogram, Name, Pid}
            end,
            lists:zip(Names, lists:seq(1, length(Names)))
        ),
    setup_stats(Rest, Options, LampList ++ Acc);
setup_stats([{Type, Names} | Rest], Options, Acc) ->
    Ref = counters:new(length(Names), [write_concurrency]),
    LampList =
        lists:map(
            fun({Name, I}) ->
                {ok, Pid} =
                    riak_kv_flo_lamp:start_link(Type, Name, {Ref, I}, Options),
                persistent_term:put({?MODULE, Type, Name}, {Ref, I}),
                {Type, Name, Pid}
            end,
            lists:zip(Names, lists:seq(1, length(Names)))
        ),
    setup_stats(Rest, Options, LampList ++ Acc).

-spec housekeep_results(stat_results(), pos_integer()) -> stat_results().
housekeep_results({rate, RecentResults}, TimeSlices) ->
    {rate, lists:sublist(RecentResults, TimeSlices)};
housekeep_results({count, {RecentResults, Total}}, TimeSlices) ->
    {count, {lists:sublist(RecentResults, TimeSlices), Total}};
housekeep_results({lossy_histogram, RecentResults}, TimeSlices) ->
    {lossy_histogram, lists:sublist(RecentResults, TimeSlices)}.

-spec produce_result(
    {stat_type(), atom()}, stat_results(), pos_integer()
) ->
    stat_output().
produce_result({rate, Name}, {rate, RecentResults}, TimeSlices) ->
    {CurrentRate, MeanRate} =
        case length(RecentResults) of
            L when L == 0 ->
                {0, 0};
            _L ->
                ResultsToUse = lists:sublist(RecentResults, TimeSlices),
                {
                    hd(RecentResults),
                    trunc(lists:sum(ResultsToUse) / length(ResultsToUse))
                }
        end,
    NameBin = atom_to_binary(Name),
    [
        {Name, CurrentRate},
        {get_name(NameBin, ?SUFFIX_RATE_MEAN), MeanRate}
    ];
produce_result({count, Name}, {count, {RecentResults, Total}}, TimeSlices) ->
    ResultsToUse = lists:sublist(RecentResults, TimeSlices),
    RecentCount = lists:sum(ResultsToUse),
    NameBin = atom_to_binary(Name),
    [
        {Name, RecentCount},
        {get_name(NameBin, ?SUFFIX_COUNT_TOTAL), Total}
    ];
produce_result({Type, Name}, {Type, ResultLists}, TimeSlices) when
    Type == lossy_histogram
->
    Timings = lists:umerge(lists:sublist(ResultLists, TimeSlices)),
    {Median, Mean, P95, P99, P100} =
        case length(Timings) of
            0 ->
                {0, 0, 0, 0, 0};
            Count ->
                {
                    lists:nth(max(1, Count div 2), Timings),
                    trunc(lists:sum(Timings) / Count),
                    lists:nth(max(1, Count - (Count div 20)), Timings),
                    lists:nth(max(1, Count - (Count div 100)), Timings),
                    lists:last(Timings)
                }
        end,
    NameBin = atom_to_binary(Name),
    [
        {get_name(NameBin, ?SUFFIX_HIST_100), P100},
        {get_name(NameBin, ?SUFFIX_HIST_95), P95},
        {get_name(NameBin, ?SUFFIX_HIST_99), P99},
        {get_name(NameBin, ?SUFFIX_HIST_MEAN), Mean},
        {get_name(NameBin, ?SUFFIX_HIST_MEDIAN), Median}
    ].

%%%============================================================================
%%% Helper functions
%%%============================================================================

-spec get_option(
    list(option()),
    tick | width | depth,
    flo_tick | flo_width | flo_depth,
    pos_integer()
) ->
    pos_integer().
get_option(Options, OptionName, AppKey, Default) ->
    case lists:keyfind(OptionName, 1, Options) of
        {OptionName, I} when is_integer(I), I > 0 ->
            I;
        _ ->
            application:get_env(
                riak_kv,
                AppKey,
                Default
            )
    end.

-spec nobadarg(fun(() -> ok)) -> ok.
nobadarg(Fun) ->
    try
        Fun()
    catch
        error:badarg -> ok
    end.

-spec get_name(binary(), binary()) -> atom().
get_name(NameBin, SuffixBin) ->
    binary_to_atom(<<NameBin/binary, SuffixBin/binary>>).

%%%============================================================================
%%% Eunit tests
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").

simple_rate_test_() ->
    {timeout, 60, fun simple_rate_tester/0}.

simple_count_test_() ->
    {timeout, 60, fun simple_count_tester/0}.

simple_histo_test_() ->
    {timeout, 60, fun simple_histo_tester/0}.

simple_rate_tester() ->
    StatDefinition =
        {
            rate, [node_put_fsm_active]
        },
    Args = {[StatDefinition], [{tick, 100}, {width, 60}]},
    {ok, _Flo} = gen_server:start({local, ?MODULE}, ?MODULE, Args, []),
    ?assertMatch(
        [
            {node_put_fsm_active, 0},
            {node_put_fsm_active_60s, 0}
        ],
        get_stats()
    ),

    update_rate(node_put_fsm_active, starting),
    loop_single_active_rate(150, 50),
    update_rate(node_put_fsm_active, stopping),

    Stats = get_stats(),
    ?assertMatch(
        [
            {node_put_fsm_active, 1},
            {node_put_fsm_active_60s, 1}
        ],
        Stats
    ),

    %% No crash if we update a stat not yet available
    ?assertMatch(ok, update_rate(node_get_fsm_active, starting)),

    gen_server:stop(?MODULE).

loop_single_active_rate(0, _Sleep) ->
    ok;
loop_single_active_rate(N, Sleep) ->
    update_rate(node_put_fsm_active, stopping),
    update_rate(node_put_fsm_active, starting),
    timer:sleep(Sleep),
    loop_single_active_rate(N - 1, Sleep).

simple_count_tester() ->
    Tick = 500,
    Width = 10,
    StatDefinition =
        {
            count, [node_puts, node_puts_set]
        },
    Args = {[StatDefinition], [{tick, Tick}, {width, Width}]},
    {ok, _Flo} = gen_server:start({local, ?MODULE}, ?MODULE, Args, []),
    ?assertMatch(
        [
            {node_puts, 0},
            {node_puts_set, 0},
            {node_puts_set_total, 0},
            {node_puts_total, 0}
        ],
        get_stats()
    ),

    {ok, TR1} =
        timer:apply_interval(20, ?MODULE, update_count, [node_puts]),
    {ok, TR2} =
        timer:apply_interval(25, ?MODULE, update_count, [node_puts_set]),
    timer:sleep((Tick * (Width + 2)) + 1),
    timer:cancel(TR1),
    timer:cancel(TR2),
    [
        {node_puts, PC},
        {node_puts_set, SC},
        {node_puts_set_total, _SCT},
        {node_puts_total, _PCT}
    ] = get_stats(),
    % Wait a tick
    timer:sleep(Tick),
    [
        {node_puts, _PC},
        {node_puts_set, _SC},
        {node_puts_set_total, SCT},
        {node_puts_total, PCT}
    ] = get_stats(),

    ?assertMatch(
        {300, 240},
        {PCT, SCT}
        % Totals must match exactly
    ),

    true = PC >= 250 andalso PC < 254,
    true = SC >= 200 andalso SC < 203,
    % The rate can be over-estimated slightly, but by less than 1%

    gen_server:stop(?MODULE).

simple_histo_tester() ->
    Tick = 500,
    Width = 10,
    Depth = 100,
    StatDefinition =
        {
            lossy_histogram, [node_put_fsm_time]
        },
    Args = {[StatDefinition], [{tick, Tick}, {width, Width, depth, Depth}]},
    {ok, _Flo} = gen_server:start({local, ?MODULE}, ?MODULE, Args, []),
    ?assertMatch(
        [
            {node_put_fsm_time_100, 0},
            {node_put_fsm_time_95, 0},
            {node_put_fsm_time_99, 0},
            {node_put_fsm_time_mean, 0},
            {node_put_fsm_time_median, 0}
        ],
        get_stats()
    ),

    timer:sleep(1000),
    lists:foreach(
        fun(I) ->
            update_lossyhistogram(node_put_fsm_time, I)
        end,
        lists:seq(1, 100)
    ),
    timer:sleep(Tick + 1),
    Stats0 = get_stats(),
    ?assertMatch(
        [
            {node_put_fsm_time_100, 100},
            {node_put_fsm_time_95, 95},
            {node_put_fsm_time_99, 99},
            {node_put_fsm_time_mean, 50},
            {node_put_fsm_time_median, 50}
        ],
        Stats0
    ),
    lists:foreach(
        fun(I) ->
            update_lossyhistogram(node_put_fsm_time, I)
        end,
        lists:seq(1, 100)
    ),
    timer:sleep(Tick + 1),
    lists:foreach(
        fun(I) ->
            update_lossyhistogram(node_put_fsm_time, I)
        end,
        lists:seq(1, 100)
    ),
    timer:sleep(Tick + 1),
    Stats1 = get_stats(),
    ?assertMatch(
        [
            {node_put_fsm_time_100, 100},
            {node_put_fsm_time_95, 95},
            {node_put_fsm_time_99, 99},
            {node_put_fsm_time_mean, 50},
            {node_put_fsm_time_median, 50}
        ],
        Stats1
    ),
    gen_server:stop(?MODULE).

-endif.
