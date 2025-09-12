%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
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

%% Generate a vnode ID unique to this node
%% As suggested by Ted Burghart - https://github.com/OpenRiak/riak_kv/issues/47 

-module(riak_kv_vnode_id).

-export([
    next_vnode_epoch/0
]).
-export_type([
    vnode_epoch/0
]).

-compile({inline, [vnode_epoch_instant/0]}).
-on_load(init_persistent/0).

-type vnode_epoch() :: 0..16#ffffffff.
%% An unsigned 32-bit value that's greater than or equal to the current time
%% in seconds since 2010-01-01 00:00:00 UTC.
%%
%% 2010 is chosen as the epoch, instead of the Posix-standard 1970, as:
%%
%% * It's the last decade that predates Riak.
%%
%% * It buys us 40 additional years (until past 2140) before the 32-bit value
%%   rolls over.
%%

-spec next_vnode_epoch() -> vnode_epoch().
%% @doc Returns the next unique epoch for vnode identity.
%%
%% The returned value is guaranteed to be:
%%
%% * Unique within the current VM.
%%
%% * Greater than any previously returned value.
%%
next_vnode_epoch() ->
    EpochAtomic = persistent_term:get({?MODULE, last_vnode_epoch}),
    next_vnode_epoch(EpochAtomic, atomics:get(EpochAtomic, 1)).

-spec next_vnode_epoch(
    Atomic :: atomics:atomics_ref(), Last :: integer())
        -> vnode_epoch().
%% @hidden
%% Handles contention between parallel calls to ensure the sequence and
%% uniqueness invariants hold.
%%
%% The following timing results were derived with OTP 24 on macOS 15 with a
%% 12-core Apple M2 Pro CPU, using an instrumented version of this function
%% to report iterations (total compare-and-exchange attempts before success).
%%
%% * Without contention, this function takes about 150-400 nanoseconds,
%%   with the predominant elapsed time usually around 250ns.
%%
%% * With contention (one-ten+ parallel invocations per core), this function
%%   averages around 400-500ns and is consistently sub-microsecond.
%%
%% * With contention, average iterations through this function tend to stay
%%   very close to two, consistent with the non-contention single-iteration
%%   elapsed time results.
%%
%% * With heavy contention, iterations do occasionally rise above two, but
%%   have never been seen above three.
%%
%% Despite the above, it's reasonable to expect higher contention and
%% increased iterations with more active CPU cores and schedulers, though
%% it's hard to envision it becoming problematic in any anticipated use case.
%%
next_vnode_epoch(Atomic, Last) ->
    Now = vnode_epoch_instant(),
    Next = if
        Last >= Now ->
            (Last + 1);
        true ->
            Now
    end,
    case atomics:compare_exchange(Atomic, 1, Last, Next) of
        ok ->
            Next;
        NewLast ->
            next_vnode_epoch(Atomic, NewLast)
    end.


%% Seconds between 1970-01-01 and 2010-01-01.
-define(EPOCH_OFFSET_Secs,  1262304000).

-spec vnode_epoch_instant() -> vnode_epoch().
%% @hidden
%% At time of writing the returned value is less than 30 bits.
%% This function will be inlined away, it's here only for code clarity.
vnode_epoch_instant() ->
    (erlang:system_time(second) - ?EPOCH_OFFSET_Secs).

-spec init_persistent() -> ok.
%% @hidden
%% Run at module load, initialization gets too difficult otherwise.
init_persistent() ->
    EpochAtomic = atomics:new(1, [{signed, false}]),
    ok = atomics:put(EpochAtomic, 1, vnode_epoch_instant()),
    ok = persistent_term:put({?MODULE, last_vnode_epoch}, EpochAtomic).