%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019-2022 Martin Sumner.
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
%%
%% @doc Queue any reap request originating from this node.  The process will
%% reap each tombstone one by one, waiting or the reap attempt to be
%% acknowledged from each vnode - so as to act as a natural throttle on reap
%% workloads.
%% Each node should have a singleton reaper initiated at startup.  Should
%% additional reap capacity be required, then reap jobs could start their own
%% reapers.
%%
-module(riak_kv_reaper).
-behaviour(riak_kv_queue_manager).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([start_link/1]).
-endif.

-export_type([reap_reference/0, job_id/0]).

-define(QUEUE_LIMIT, 100000).
-define(OVERFLOW_LIMIT, 10000000).
-define(REDO_TIMEOUT, 2000).
-define(TOMB_PAUSE, 2).
    % Used as flow control in the reaper, shared configuration with the delete
    % process where the pause has a dual-purpose, for both flow control and for
    % improving the probability that tombstone PUTs are propagated before a
    % reap attempt is prompted
-define(BUSY_FACTOR, 5).
    % Multiply the tombstone pause by this factor when there is a soft overload
    % state.

-export([start_link/0,
            start_job/1,
            request_reap/1,
            request_reap/2,
            bulk_request_reap/1,
            bulk_request_reap/2,
            direct_reap/1,
            reap_stats/0,
            reap_stats/1,
            clear_queue/0,
            clear_queue/1,
            stop_job/1]).

-export([action/2,
            get_limits/0,
            redo/0]).

-type index() :: chash:index_as_int().
-type reap_reference() :: full_reap_request()|partial_reap_request().

-type full_reap_request() ::
    {{riak_object:bucket(), riak_object:key()}, vclock:vclock(), boolean()}.
    %% the reap reference is {Bucket, Key, Clock (of tombstone), Forward}.  The
    %% Forward boolean() indicates if this reap should be replicated if
    %% riak_kv.repl_reap is true.  When a reap is received via replication
    %% Forward should be set to false, to prevent reaps from perpetually
    %% circulating
-type partial_reap_request() ::
    {{riak_object:bucket(), riak_object:key()}, non_neg_integer(), [index()]}.
    %% A request to reap from a specific index, eventually, when this index is
    %% up and available as a primary.
    %% Unlike a full reap request a partial reap request cannot be forwarded
    %% to another cluster.
    %% The DeleteHash is used rather than the tombstone clock to allow
    %% for the potential for downstream vnodes to prompt a partial reap request
    %% (the downstream vnode will only see the DeleteHash, not the clock in the
    %% reap request).
-type job_id() :: pos_integer().

%%%============================================================================
%%% API
%%%============================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link(app_helper:get_env(riak_kv, reaper_dataroot)).

start_link(FilePath) ->
    riak_kv_queue_manager:start_link(?MODULE, FilePath).

-spec start_job(job_id()) -> {ok, pid()}.
%% @doc
%% To be used when starting a reaper for a specific workload
start_job(JobID) ->
    start_job(JobID, app_helper:get_env(riak_kv, reaper_dataroot)).

start_job(JobID, FilePath) ->
   riak_kv_queue_manager:start_job(JobID, ?MODULE, FilePath).

-spec request_reap(reap_reference()) -> ok.
request_reap(ReapReference) ->
    request_reap(?MODULE, ReapReference).

-spec request_reap(pid()|module(), reap_reference()) -> ok.
request_reap(Pid, ReapReference) ->
    riak_kv_queue_manager:request(Pid, ReapReference).

-spec bulk_request_reap(list(reap_reference())) -> ok.
bulk_request_reap(RefList) ->
    bulk_request_reap(?MODULE, RefList).

-spec bulk_request_reap(pid()|module(), list(reap_reference())) -> ok.
bulk_request_reap(Pid, RefList) ->
    riak_kv_queue_manager:bulk_request(Pid, RefList).

-spec reap_stats() ->
    list({atom(), non_neg_integer()|riak_kv_overflow_queue:queue_stats()}).
reap_stats() -> reap_stats(?MODULE).

-spec reap_stats(pid()|module()) ->
    list({atom(), non_neg_integer()|riak_kv_overflow_queue:queue_stats()}).
reap_stats(Pid) ->
    riak_kv_queue_manager:stats(Pid).

-spec direct_reap(reap_reference()) -> boolean().
direct_reap(ReapReference) ->
    riak_kv_queue_manager:immediate_action(?MODULE, ReapReference).

-spec clear_queue() -> ok.
clear_queue() -> clear_queue(?MODULE).

-spec clear_queue(pid()|module()) -> ok.
clear_queue(Reaper) ->
   riak_kv_queue_manager:clear_queue(Reaper).

%% @doc
%% Stop the job once the queue is empty
-spec stop_job(pid()) -> ok.
stop_job(Pid) ->
    riak_kv_queue_manager:stop_job(Pid).

%%%============================================================================
%%% Callback functions
%%%============================================================================

-spec get_limits() -> {pos_integer(), pos_integer(), pos_integer()}.
get_limits() ->
    RedoTimeout =
        app_helper:get_env(riak_kv, reaper_redo_timeout, ?REDO_TIMEOUT),
    QueueLimit =
        app_helper:get_env(riak_kv, reaper_queue_limit, ?QUEUE_LIMIT),
    OverflowLimit =
        app_helper:get_env(riak_kv, reaper_overflow_limit, ?OVERFLOW_LIMIT),
    {RedoTimeout, QueueLimit, OverflowLimit}.

%% @doc
%% Try and reap from as many primaries as are available - and for those not
%% available defer, and retry until they are available.
%% The initial message to check all preflists must prompt the replication of
%% the reap unless that message is to be redone.  If there is some partial reap
%% then redo should not be used - instead a message with the outstanding
%% indices is generated instead
-spec action(reap_reference(), boolean()) -> boolean().
action({{_Bucket, _Key}, DeleteHash, []}, _Redo) when is_integer(DeleteHash) ->
    %% Nothing left to do
    true;
action({{Bucket, Key}, DeleteHash, Indices}, Redo)
        when is_integer(DeleteHash), is_list(Indices) ->
    {TombPause, AplAnn} = setup_reap(Bucket, Key),
    case find_available(AplAnn, Indices) of
        {[], Deferred} when Deferred == Indices ->
            %% There are no indices available, so redo this message
            %% But pause first - so that the process is not in a tight loop
            %% re-checking
            timer:sleep(TombPause),
            respond_asfail_ifredo(Redo);
        {Available, Deferred} when Available =/= [] ->
            case check_all_mailboxes(Available) of
                ok ->
                    riak_kv_vnode:reap(
                        Available,
                        {Bucket, Key},
                        DeleteHash
                    ),
                    timer:sleep(TombPause),
                    ok = 
                        riak_kv_queue_manager:redo_request(
                            ?MODULE,
                            {{Bucket, Key}, DeleteHash, Deferred}
                        ),
                    %% Some indices were available and the cluster is not
                    %% stressed - so some updates made, and the remaining are
                    %% deferred
                    %% As an updated request has been sent to redo - this
                    %% request must be marked as success (i.e. return `true`),
                    %% to end the cycle of redo for this partially completed
                    %% request.
                    true;
                soft_loaded ->
                     %% The cluster is busy - reaps need to slow down, so pause
                    %% then requeue this message as-is.
                    timer:sleep(TombPause * ?BUSY_FACTOR),
                    respond_asfail_ifredo(Redo)
                end;
        {[], []} ->
            %% This may occur during shutdown - as no preflist can be accessed
            %% Just pass at this stage without pausing
            respond_asfail_ifredo(Redo)
    end;
action({{Bucket, Key}, TombClock, ToRepl}, Redo)
        when is_boolean(ToRepl) ->
    {TombPause, AplAnn} = setup_reap(Bucket, Key),
    DeleteHash = riak_object:delete_hash(TombClock),
    case find_available(AplAnn, all) of
        {Available, []} ->
            case check_all_mailboxes(Available) of
                ok ->
                    riak_kv_vnode:reap(
                        Available,
                        {Bucket, Key},
                        DeleteHash
                    ),
                    maybe_repl_reap(Bucket, Key, TombClock, ToRepl),
                    timer:sleep(TombPause),
                    %% All primaries were up, and the reap is replicated so
                    %% indicate response as success (true)
                    true;
                soft_loaded ->
                    timer:sleep(TombPause * ?BUSY_FACTOR),
                    %% The cluster is busy - reaps need to slow down, so pause
                    %% then requeue this message as-is.  Reap is not replicated
                    %% yet as it has not been applied
                    respond_asfail_ifredo(Redo)
                end;
        _ ->
            maybe_repl_reap(Bucket, Key, TombClock, ToRepl),
            ok = 
                riak_kv_queue_manager:request(
                    ?MODULE,
                    {{Bucket, Key}, DeleteHash, indices_in_preflist(AplAnn)}
                ),
            %% Not all primaries were available, so this reap request is
            %% re-queued but with all indices indicated as being required.  The
            %% reap will now be replicated (as partial reaps never are) - so
            %% there may be a temporary discrepancy (hence why this is queued
            %% as an individual reap, which has higher priority than bulk reaps) 
            true
    end.

-spec redo() -> boolean().
redo() -> true.

%%%============================================================================
%%% Internal functions
%%%============================================================================

-type preflist_entry() :: {non_neg_integer(), node()}.

-spec setup_reap(
    riak_object:bucket(), riak_object:key()) ->
        {non_neg_integer(), riak_core_apl:preflist_ann()}.
setup_reap(Bucket, Key) ->
    TombPause = app_helper:get_env(riak_kv, tombstone_pause, ?TOMB_PAUSE),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    DocIdx = riak_core_util:chash_key({Bucket, Key}, BucketProps),
    {n_val, N} = lists:keyfind(n_val, 1, BucketProps),
    {
        TombPause,
        riak_core_apl:get_apl_ann(
            DocIdx,
            N,
            riak_core_node_watcher:nodes(riak_kv)
        )
    }.

%% @doc
%% Redo by indicating the original request was not successful (false), but only
%% if the request is marked as valid for redo (i.e. the Redo passed to the
%% request is true).  If the redo passed is false (redo not supported) - make a
%% false claim of success (true), so as not to trigger Redo.
-spec respond_asfail_ifredo(boolean()) -> boolean().
respond_asfail_ifredo(Redo) ->
    not Redo.

%% @doc
%% Find the available primaries (i.e. primaries on Up nodes) out of a sublist
%% of required primaries.  The `all` keyword is used instead of a sublist
%% should the availability of all primaries in a preflist be the concern.
%% Function returns a list of {Idx, Node} tuples for online primaries, and a
%% list of Index integers for primaries not currently available.
-spec find_available(
    riak_core_apl:preflist_ann(), all|list(index())) ->
        {list({index(), node()}), list(index())}.
find_available(AplAnn, all) ->
    find_available(AplAnn, indices_in_preflist(AplAnn), [], []);
find_available(AplAnn, Indices) ->
    find_available(AplAnn, Indices, [], []).

find_available([], _Reqd, Available, Deferred) ->
    {lists:reverse(Available), lists:reverse(Deferred)};
find_available([{{Idx, Node}, primary}|Rest], Reqd, Available, Deferred) ->
    case lists:member(Idx, Reqd) of
        true ->
            find_available(Rest, Reqd, [{Idx, Node}|Available], Deferred);
        false ->
            find_available(Rest, Reqd, Available, Deferred)
    end;
find_available([{{Idx, _Node}, fallback}|Rest], Reqd, Available, Deferred) ->
    case lists:member(Idx, Reqd) of
        true ->
            find_available(Rest, Reqd, Available, [Idx|Deferred]);
        false ->
            find_available(Rest, Reqd, Available, Deferred)
    end.

-spec indices_in_preflist(riak_core_apl:preflist_ann()) -> list(index()).
indices_in_preflist(ApplAnn) ->
    lists:map(fun({{Idx, _N}, _T}) -> Idx end, ApplAnn).

-spec maybe_repl_reap(
    riak_object:bucket(), riak_object:key(), vclock:vclock(), boolean()) -> ok.
maybe_repl_reap(Bucket, Key, TombClock, ToReap) ->
    case application:get_env(riak_kv, repl_reap, false) and ToReap of
        true ->
            riak_kv_replrtq_src:replrtq_reap(
                Bucket, Key, TombClock, os:timestamp());
        false ->
            ok
    end.

%% Protect against overloading the system when not reaping should any
%% mailbox be in soft overload state
-spec check_all_mailboxes(list(preflist_entry())) -> ok|soft_loaded.
check_all_mailboxes([]) ->
    ok;
check_all_mailboxes([H|Rest]) ->
    case check_mailbox(H) of
        ok ->
            check_all_mailboxes(Rest);
        soft_loaded ->
            riak_kv_stat:update(soft_loaded_vnode_mbox),
            soft_loaded
    end.

%% Call off to vnode proxy for mailbox status.
-spec check_mailbox(preflist_entry()) -> ok|soft_loaded.
check_mailbox({Idx, Node}) ->
    RegName = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx, Node),
    element(1, riak_core_vnode_proxy:call(RegName, mailbox_size)).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

test_1inNreapfun(N) ->
    fun(ReapRef, _Bool) ->
        case erlang:phash2({ReapRef, os:timestamp()}) rem N of
            0 -> false;
            _ -> true
        end
    end.

test_100reap(_ReapRef, _Bool) ->
    true.

find_available_test() ->
    N1 = 'node1@127.0.0.1',
    N2 = 'node2@127.0.0.1',
    N3 = 'node3@127.0.0.1',
    I1 = 0,
    I2 = 1 bsl 156,
    I3 = 2 bsl 156,
    AplAnn1 = [{{I1, N1}, primary}, {{I2, N2}, primary}, {{I3, N3}, primary}],
    ?assertMatch(
        {[{I1, N1}, {I2, N2}, {I3, N3}], []},
        find_available(AplAnn1, all)
    ),
    ?assertMatch(
        {[{I1, N1}, {I3, N3}], []},
        find_available(AplAnn1, [I1, I3])
    ),
    AplAnn2 = [{{I1, N1}, primary}, {{I2, N2}, primary}, {{I3, N3}, fallback}],
    ?assertMatch(
        {[{I1, N1}, {I2, N2}], [I3]},
        find_available(AplAnn2, all)
    ),
    ?assertMatch(
        {[{I1, N1}], [I3]},
        find_available(AplAnn2, [I1, I3])
    ).

standard_reaper_test_() ->
    {timeout, 30, fun standard_reaper_tester/0}.

failure_reaper_test_() ->
    {timeout, 60, fun somefail_reaper_tester/0}.

standard_reaper_tester() ->
    NumberOfRefs = 1000,
    {ok, P} = start_job(1, riak_core_test_util:get_test_dir("std_reaper")),
    ok = gen_server:call(P, {override_action, fun test_100reap/2}),
    B = {<<"type1">>, <<"B1">>},
    RefList =
        lists:map(fun(X) -> {{B, term_to_binary(X)}, erlang:phash2(X)} end,
                    lists:seq(1, NumberOfRefs)),
    spawn(fun() ->
                lists:foreach(fun(R) -> request_reap(P, R) end, RefList)
            end),
    WaitFun =
        fun(Sleep, Done) ->
            case Done of
                false ->
                    timer:sleep(Sleep),
                    [{mqueue_lengths,[{1,RedoQL},{2,ReapQL}]},
                        {overflow_lengths,[{2,0},{1,0}]},
                        {overflow_discards,[{2,0},{1,0}]},
                        {attempts,AT},
                        {aborts,AB}] = reap_stats(P),
                    case AT of
                        NumberOfRefs ->
                            ?assertMatch(0, AB),
                            ?assertMatch({0, 0}, {RedoQL, ReapQL}),
                            true;
                        _ ->
                            false
                    end;
                true ->
                    true
            end
        end,
    lists:foldl(WaitFun, false, lists:seq(101, 200)),
    ok = stop_job(P),
    timer:sleep(100),
    ?assertMatch(false, is_process_alive(P)).

somefail_reaper_tester() ->
    somefail_reaper_tester(4),
    somefail_reaper_tester(16),
    somefail_reaper_tester(64).


somefail_reaper_tester(N) ->
    NumberOfRefs = 1000,
    {ok, P} = start_job(1, riak_core_test_util:get_test_dir("err_reaper")),
    ok = gen_server:call(P, {override_action, test_1inNreapfun(N)}),
    B = {<<"type1">>, <<"B1">>},
    RefList =
        lists:map(fun(X) -> {{B, term_to_binary(X)}, erlang:phash2(X)} end,
                    lists:seq(1, NumberOfRefs)),
    spawn(fun() ->
                lists:foreach(fun(R) -> request_reap(P, R) end, RefList)
            end),
    WaitFun =
        fun(Sleep, Done) ->
            case Done of
                false ->
                    timer:sleep(Sleep),
                    [{mqueue_lengths,[{1,RedoQL},{2,ReapQL}]},
                        {overflow_lengths,[{2,0},{1,0}]},
                        {overflow_discards,[{2,0},{1,0}]},
                        {attempts,AT},
                        {aborts,AB}] = reap_stats(P),
                    case (AT + AB >= NumberOfRefs) of
                        true ->
                            ?assertMatch(true, AB > 0),
                            ?assertMatch(true, AT > 0),
                            ?assertMatch(true, AT > AB),
                            ?assertMatch(true, RedoQL >= 0),
                            ?assertMatch(NumberOfRefs, AT + RedoQL),
                            ?assertMatch(0, ReapQL),
                            true;
                        false ->
                            false
                    end;
                true ->
                    true
            end
        end,
    lists:foldl(WaitFun, false, lists:seq(101, 500)),
    ok = clear_queue(P),
    ok = stop_job(P),
    timer:sleep(100),
    ?assertMatch(false, is_process_alive(P)).


-endif.
