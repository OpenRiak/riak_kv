%% -------------------------------------------------------------------
%%
%% riak_kv_flo_lamp: A helper server to assist flo nightingale in the
%% gathering of stats
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

%% Flo Nightingale is the Lady with the Lamp - and her Lamps are used to
%% illuminate stats to be gathered by her

-module(riak_kv_flo_lamp).

-behaviour(gen_server).

-export(
    [
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        handle_continue/2
    ]
).

-export(
    [
        start_link/4,
        fetch/1,
        add_timing/3
    ]
).

-define(
    NEW_SLICE(Width),
    array:new([{size, Width}, {fixed, true}, {default, unset}])
).

-record(state,
    {
        type :: riak_kv_flo_nightingale:stat_type(),
        name :: atom(),
        counter_ref :: atomics:atomics_ref()|counters:counters_ref(),
        counter_idx :: pos_integer(),
        tick :: pos_integer(), 
        depth :: pos_integer(), % only required in lossy_histogram
        current_slices :: undefined|array:array() % only in lossy_histogram
    }
).

-type lamp_state() :: #state{}.

%%%============================================================================
%%% External API
%%%============================================================================

-spec start_link(
    riak_kv_flo_nightingale:stat_type(),
    atom(),
    {atomics:atomics_ref()|counters:counters_ref(), pos_integer()},
    list(riak_kv_flo_nightingale:option())) ->
        {ok, pid()}.
start_link(Type, Name, Ref, Options) ->
    {ok, Pid} =
        gen_server:start_link(
            ?MODULE,
            {Type, Name, Ref, Options},
            []
        ),
    {ok, Pid}.

-spec fetch(pid()) -> lamp_state().
fetch(Pid) ->
    gen_server:call(Pid, fetch, infinity).

-spec add_timing(pid(), pos_integer(), pos_integer()) -> ok.
add_timing(Pid, Timing, Slice) ->
    gen_server:cast(Pid, {add_timing, Timing, Slice}).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init({Type, Name, {Ref, Idx}, Options}) ->
    Depth = riak_kv_flo_nightingale:get_depth(Options),
    Tick = riak_kv_flo_nightingale:get_tick(Options),
    CurrentSlices =
        case Type of
            lossy_histogram ->
                ?NEW_SLICE(Depth);
            _ ->
                undefined
        end,
    {
        ok,
        #state{
            type = Type,
            name = Name,
            counter_ref = Ref,
            counter_idx = Idx,
            tick = Tick,
            depth = Depth,
            current_slices = CurrentSlices
        },
        {continue, {schedule_tick, Options}}
    }.

handle_continue({schedule_tick, _Options}, State) ->
    erlang:send_after(rand:uniform(State#state.tick), self(), tick),
    {noreply, State}.

handle_call(fetch, _From, State) ->
    %% Expected to be used in tests/debugging only
    {reply, State, State}.

handle_cast(
    {add_timing, Timing, Slice},
    State = #state{type = T, current_slices = CS})
        when T == lossy_histogram, CS =/= undefined ->
    {
        noreply,
        State#state{
            current_slices =
                array:set(Slice - 1, Timing, State#state.current_slices)
        }
    }.

handle_info(tick, State = #state{type = rate}) ->
    erlang:send_after(State#state.tick, self(), tick),
    CurrentCounter =
        counters:get(
            State#state.counter_ref,
            State#state.counter_idx
        ),
    riak_kv_flo_nightingale:update(rate, State#state.name, CurrentCounter),
    {noreply, State};
handle_info(tick, State = #state{type = count}) ->
    erlang:send_after(State#state.tick, self(), tick),
    CurrentCounter =
        counters:get(
            State#state.counter_ref,
            State#state.counter_idx
        ),
    counters:sub(
        State#state.counter_ref,
        State#state.counter_idx,
        CurrentCounter
    ),
    riak_kv_flo_nightingale:update(count, State#state.name, CurrentCounter),
    {noreply, State};
handle_info(tick, State = #state{type = lossy_histogram}) ->
    erlang:send_after(State#state.tick, self(), tick),
    atomics:put(
        State#state.counter_ref,
        State#state.counter_idx,
        0
    ),
    Slice = lists:sort(array:sparse_to_list(State#state.current_slices)),
    riak_kv_flo_nightingale:update(lossy_histogram, State#state.name, Slice),
    {
        noreply,
        State#state{
            current_slices = ?NEW_SLICE(State#state.depth)
        }
    }.