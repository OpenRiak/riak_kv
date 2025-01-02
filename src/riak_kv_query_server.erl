%% -------------------------------------------------------------------
%%
%% riak_query_worker: Manage complex secondary index query.
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc The query worker manages a single secondary index query.
%%
%% This is a gen_server to manage an individual query.
%% 
%% The server will initialise a coverage query, and then await vnodes
%% returning results.  The server should terminate when:
%% - all vnodes have returned all results
%% - sufficient results from all vnodes have been received such that any
%% further results received would exceed the maximum number of results
%% - the PID which requested the query dies
%% - an absolute timeout occurs
%% 
%% TODO:
%% This has not been implemented with the riak_core_coverage_fsm behaviour as
%% it was considered there was minimal value in that behaviour, and there is a
%% preference not to complicate future work to remove deprecated gen_fsm
%% from Riak by expanding its use.
%% In the future the riak_kv_clusteraae_fsm and riak_index_fsm should be
%% refactored in line with this - and this may involve re-introducing a
%% common behaviour.

-module(riak_kv_query_server).

-behaviour(gen_server).

-define(SLOW_TIME, application:get_env(riak_kv, index_fsm_slow_timems, 200)).
-define(FAST_TIME, application:get_env(riak_kv, index_fsm_fast_timems, 10)).

-export(
    [
        query_start/1,
        new_timeout/2,
        check_vnode_monitor/1
    ]
).

-export(
    [
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        format_status/1
    ]
).

-include_lib("kernel/include/logger.hrl").

-define(VERSION, [{version, 2}]). % to be used in sets
-define(ETS_THRESHOLD, 4096).
% -define(START_OPTS, [{spawn_opt, [{min_heap_size, ?MIN_HEAP_SIZE}]}]).
-define(START_OPTS, []).
-define(DEFAULT_BUFFER_SIZE, 320).
-define(DBTYPE_KEYS, ordered_set).

-record(timings, 
    {
        start_time = os:timestamp() :: erlang:timestamp(),
        max = 0 :: non_neg_integer(),
        min = infinity :: non_neg_integer()|infinity,
        count = 0 :: non_neg_integer(),
        sum = 0 :: non_neg_integer(),
        slow_count = 0 :: non_neg_integer(),
        fast_count = 0 :: non_neg_integer(),
        slow_time = ?SLOW_TIME,
        fast_time = ?FAST_TIME
    }
).
-record(state,
    {
        from :: from(),
        client_monitor :: reference(),
        timeout_reqid :: req_id(),
        req_id :: req_id(),
        timings = #timings{} :: timings(),
        bucket :: riak_object:bucket(),
        vnode_monitor :: vnode_monitor(),
        vnodes_ongoing :: sets:set(vnode_id()),
        acc :: result_record()|redacted,
        result_table = none :: none|ets:table(),
        result_encoding_fun :: riak_kv_query:encoding_fun()|raw
    }
).

-record(count_acc,
    {
        results = 0 :: non_neg_integer()
    }
).
-record(list_acc,
    {
        results = [] :: key_list()|term_list()
    }
).
-record(map_acc,
    {
        results = maps:new() :: count_map()
    }
).

-type key_list() :: list({riak_object:key()})|list(riak_object:key()).
-type term_list() :: list({{binary(), riak_object:key()}}).
-type count_map() :: #{binary() => non_neg_integer()}|#{}.

-type result_record() :: #count_acc{}|#list_acc{}|#map_acc{}.
-type results() :: key_list()|term_list()|count_map()|non_neg_integer().

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

-type timings() :: #timings{}.

-type vnode_id() :: non_neg_integer().
-type vnode_monitor() :: #{vnode_id() => non_neg_integer()}|#{}.

-export_type([results/0]).


%%%============================================================================
%%% API
%%%============================================================================

-spec query_start(
    riak_kv_query:complex_query_definition())
        -> {ok, pid(), non_neg_integer()}.
query_start(Query) ->
    {ok, Worker} = gen_server:start_link(?MODULE, Query, ?START_OPTS),
    {ok, Worker, riak_kv_query:get_reqid(Query)}.

-spec new_timeout(pid(), pos_integer()) -> ok.
new_timeout(Pid, SecondsToTimeout) ->
    gen_server:cast(Pid, {new_timeout, SecondsToTimeout}).

-spec check_vnode_monitor(pid()) -> {ok, vnode_monitor()}.
check_vnode_monitor(Pid) ->
    gen_server:call(Pid, {check_progress, vnode_monitor}, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init(Query) ->
    Bucket = riak_kv_query:get_bucket(Query),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),
    R = riak_kv_query:get_r(Query),
    AccType = riak_kv_query:get_accumulator(Query),
    EvaluatedQuery = riak_kv_query:get_query_definition(Query),
    Request =
        riak_kv_requests:new_query_request(
            Bucket,
            none,
            riak_kv_query:get_querytype(Query),
            AccType,
            riak_kv_query:get_returnterms(Query),
            calculate_buffer_size(Query),
            EvaluatedQuery),
    TimeoutS = riak_kv_query:get_timeout_secs(Query),
    From = riak_kv_query:get_clientpid(Query),
    ReqID = riak_kv_query:get_reqid(Query),
    ClientMonitorRef = erlang:monitor(process, From),
    case riak_core_coverage_plan:create_plan(all, NVal, R, ReqID, riak_kv) of
        {error, Reason} ->
            ?LOG_WARNING("Query coverage plan failed due to ~0p", [Reason]),
            From ! {ReqID, {error, insufficient_vnodes}},
            {stop, insufficient_vnodes};
        {CoverageVnodes, FilterVnodes} ->
            Sender = {raw, ReqID, self()},
            riak_core_vnode_master:coverage(
                Request,
                CoverageVnodes,
                FilterVnodes,
                Sender,
                riak_kv_vnode_master
            ),
            InitMonitor = maps:from_keys(CoverageVnodes, 0),
            VnodesOngoing = sets:from_list(CoverageVnodes, ?VERSION),
            Acc =
                case AccType of
                    keys ->
                        #list_acc{};
                    raw_keys ->
                        #list_acc{};
                    term_with_keys ->
                        #list_acc{};
                    match_count ->
                        #count_acc{};
                    key_count ->
                        #count_acc{};
                    term_with_matchcount ->
                        #map_acc{};
                    term_with_keycount ->
                        #map_acc{}
                    end,
            erlang:send_after(TimeoutS * 1000, self(), {timeout, ReqID}),
            {
                ok, 
                #state{
                    from = {raw, ReqID, From},
                    client_monitor = ClientMonitorRef,
                    timeout_reqid = ReqID,
                    req_id = ReqID,
                    bucket = Bucket,
                    vnode_monitor = InitMonitor,
                    vnodes_ongoing = VnodesOngoing,
                    acc = Acc,
                    result_encoding_fun
                        = riak_kv_query:get_result_encodingfun(Query)
                }
            }
    end.

handle_cast({new_timeout, SecondsToTimeout}, State) ->
    ReqID = riak_kv_query:get_reqid(),
    erlang:send_after(SecondsToTimeout * 1000, self(), {timeout, ReqID}),
    {noreply, State#state{timeout_reqid = ReqID}}.

handle_call({check_progress, vnode_monitor}, _From, State) ->
    {reply, {ok, State#state.vnode_monitor}, State}.

handle_info({timeout, ReqID}, #state{timeout_reqid = ReqID} = State) ->
    {raw, ReqID, From} = State#state.from,
    From ! {ReqID, {error, timeout}},
    ?LOG_WARNING("Query terminated due to timeout"),
    {stop, shutdown, State};
handle_info(
    {{ReqID, Vnode}, {From, _B, ping}}, #state{req_id = ReqID} = State) ->
    riak_kv_vnode:ack_keys(From),
    {
        noreply,
        State#state{
            vnode_monitor = update_monitor(Vnode, State#state.vnode_monitor)}
    };
handle_info(
        {{ReqID, Vnode}, {From, _B, {AccOpt, Results}}}, 
        #state{req_id = ReqID, result_table = none} = State)
            when AccOpt == keys; AccOpt == term_with_keys ->
    riak_kv_vnode:ack_keys(From),
    {AccOpt, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {AccOpt, Results},
            {AccOpt, (State#state.acc)#list_acc.results}
        ),
    case length(UpdResults)
        of 
            ResultsSoFar when ResultsSoFar < ?ETS_THRESHOLD ->
                {
                    noreply,
                    State#state{
                        vnode_monitor =
                            update_monitor(Vnode, State#state.vnode_monitor),
                        acc = #list_acc{results = UpdResults}
                    }
                };
            _ ->
                ResultTable = ets:new(query_results, [?DBTYPE_KEYS, private]),
                true = 
                    ets:insert_new(ResultTable, UpdResults),
                {
                    noreply,
                    State#state{
                        vnode_monitor =
                            update_monitor(Vnode, State#state.vnode_monitor),
                        acc = #list_acc{},
                        result_table = ResultTable
                    }
                }
    end;
handle_info(
        {{ReqID, Vnode}, {From, _B, {AccOpt, Results}}}, 
        #state{req_id = ReqID, result_table = ResultTable} = State)
            when AccOpt == keys; AccOpt == term_with_keys ->
    riak_kv_vnode:ack_keys(From),
    true = ets:insert(ResultTable, Results),
    {
        noreply,
        State#state{
            vnode_monitor =
                update_monitor(Vnode, State#state.vnode_monitor)
        }
    };
handle_info(
        {{ReqID, Vnode}, {From, _B, {raw_keys, Results}}}, 
        #state{req_id = ReqID, result_table = none} = State) ->
    riak_kv_vnode:ack_keys(From),
    {raw_keys, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {raw_keys, Results},
            {raw_keys, (State#state.acc)#list_acc.results}
        ),
    {
        noreply,
        State#state{
            vnode_monitor =
                update_monitor(Vnode, State#state.vnode_monitor),
            acc = #list_acc{results = UpdResults}
        }
    };
handle_info(
        {{ReqID, Vnode}, {From, _B, {match_count, Count}}},
        #state{req_id = ReqID} = State) ->
    riak_kv_vnode:ack_keys(From),
    {match_count, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {match_count, Count},
            {match_count, (State#state.acc)#count_acc.results}
        ),
    {
        noreply,
        State#state{
            vnode_monitor = update_monitor(Vnode, State#state.vnode_monitor),
            acc = #count_acc{results = UpdResults}
        }
    };
handle_info(
        {{ReqID, Vnode}, {From, _B, {key_count, Count}}},
        #state{req_id = ReqID} = State) ->
    riak_kv_vnode:ack_keys(From),
    {key_count, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {key_count, Count},
            {key_count, (State#state.acc)#count_acc.results}
        ),
    {
        noreply,
        State#state{
            vnode_monitor = update_monitor(Vnode, State#state.vnode_monitor),
            acc = #count_acc{results = UpdResults}
        }
    };
handle_info(
        {{ReqID, Vnode}, {From, _B, {term_with_matchcount, RM}}},
        #state{req_id = ReqID} = State) ->
    riak_kv_vnode:ack_keys(From),
    {term_with_matchcount, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {term_with_matchcount, RM},
            {term_with_matchcount, (State#state.acc)#map_acc.results}
        ),
    {
        noreply,
        State#state{
            vnode_monitor = update_monitor(Vnode, State#state.vnode_monitor),
            acc = #map_acc{results = UpdResults}
        }
    };
handle_info(
        {{ReqID, Vnode}, {From, _B, {term_with_keycount, RM}}},
        #state{req_id = ReqID} = State) ->
    riak_kv_vnode:ack_keys(From),
    {term_with_keycount, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {term_with_keycount, RM},
            {term_with_keycount, (State#state.acc)#map_acc.results}
        ),
    {
        noreply,
        State#state{
            vnode_monitor = update_monitor(Vnode, State#state.vnode_monitor),
            acc = #map_acc{results = UpdResults}
        }
    };
handle_info(
    {{ReqID, Vnode}, done}, #state{req_id = ReqID} = State) ->
    UpdCoverageVnodes = sets:del_element(Vnode, State#state.vnodes_ongoing),
    UpdTimings = update_timings(State#state.timings),
    case sets:size(UpdCoverageVnodes) of
        0 ->
            Results =
                case State#state.result_table of
                    none ->
                        extract_results(State#state.acc);
                    RT0 ->
                        ets:tab2list(RT0)
                end,
            {raw, ClientReqID, ClientPid} = State#state.from,
            case State#state.result_encoding_fun of
                raw ->
                    ClientPid ! {ClientReqID, Results};
                EncodingFun ->
                    ClientPid ! {ClientReqID, EncodingFun(Results)}
            end,
            ResultsSent =
                case State#state.result_table of
                    none ->
                        extract_count(State#state.acc);
                    RT1 ->
                        ets:info(RT1, size)
                end,
            log_timings(
                UpdTimings,
                State#state.bucket,
                ResultsSent
            ),
            {stop, normal, State};
        _ ->
            {
                noreply,
                State#state{
                    timings = UpdTimings,
                    vnode_monitor =
                        update_monitor(Vnode, State#state.vnode_monitor),
                    vnodes_ongoing = UpdCoverageVnodes
                }
            }
    end;
handle_info(
        {'DOWN', CMR, process, Pid, _Info},
        #state{client_monitor = CMR} = State) ->
    ?LOG_WARNING("Query terminated due to client=~w termination", [Pid]),
    {stop, shutdown, State};
handle_info(Msg, State) ->
    ?LOG_INFO("Receieved unexpected message ~0p", [Msg]),
    {noreply, State}.

terminate(_Reason, #state{result_table = none}) ->
    ok;
terminate(_Reason, #state{result_table = ResultTable}) ->
    ets:delete(ResultTable),
    ok.

    
format_status(Status) ->
    case maps:get(reason, Status, normal) of
        terminate ->
            State = maps:get(state, Status),
            maps:update(
                state,
                State#state{acc = redacted},
                Status
            );
        _ ->
            Status
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec update_monitor(vnode_id(), vnode_monitor()) -> vnode_monitor().
update_monitor(Vnode, VnodeMonitor) ->
    maps:update_with(Vnode, fun(V) -> V + 1 end, VnodeMonitor).

-spec calculate_buffer_size(
    riak_kv_query:complex_query_definition())
        -> {pos_integer(), non_neg_integer()}.
calculate_buffer_size(_Query) ->
    %% May need to change when adding support for max_results
    ConfiguredSize =
        application:get_env(riak_kv, query_buffer_size, ?DEFAULT_BUFFER_SIZE),
    BufferSize = max(ConfiguredSize, riak_kv_query_buffer:min_buffer()),
    {
        BufferSize,
        BufferSize div 2
    }.

-spec extract_results(result_record()) -> results().
extract_results(Acc) when is_record(Acc, list_acc) ->
    Acc#list_acc.results;
extract_results(Acc) when is_record(Acc, map_acc) ->
    Acc#map_acc.results;
extract_results(Acc) when is_record(Acc, count_acc) ->
    Acc#count_acc.results.

extract_count(Acc) when is_record(Acc, list_acc) ->
    length(Acc#list_acc.results);
extract_count(Acc) when is_record(Acc, map_acc) ->
    lists:sum(maps:values(Acc#map_acc.results));
extract_count(Acc) when is_record(Acc, count_acc) ->
    Acc#count_acc.results.

-spec update_timings(timings()) -> timings().
update_timings(Timings) ->
    MS = timer:now_diff(os:timestamp(), Timings#timings.start_time) div 1000,
    SlowCount =
        case MS > Timings#timings.slow_time of
            true ->
                Timings#timings.slow_count + 1;
            false ->
                Timings#timings.slow_count
        end,
    FastCount = 
        case MS < Timings#timings.fast_time of
            true ->
                Timings#timings.fast_count + 1;
            false ->
                Timings#timings.fast_count
        end,
    Timings#timings{
        max = max(Timings#timings.max, MS),
        min = min(Timings#timings.min, MS),
        count = Timings#timings.count + 1,
        sum = Timings#timings.sum + MS,
        slow_count = SlowCount,
        fast_count = FastCount 
    }.

-spec log_timings(timings(), riak_object:bucket(), non_neg_integer()) -> ok.
log_timings(Timings, Bucket, ResultCount) ->
    Duration = timer:now_diff(os:timestamp(), Timings#timings.start_time),
    ok = riak_kv_stat:update({query_node_time, Duration, ResultCount}),
    log_timings(Timings,
                Bucket,
                ResultCount,
                application:get_env(riak_kv, log_index_fsm, false)).

log_timings(_Timings, _Bucket, _ResultCount, false) ->
    ok;
log_timings(Timings, Bucket, ResultCount, true) ->
    ?LOG_INFO("Index query on bucket=~p " ++
                "max_vnodeq=~w min_vnodeq=~w sum_vnodeq=~w count_vnodeq=~w " ++
                "slow_count_vnodeq=~w fast_count_vnodeq=~w result_count=~w",
                [Bucket,
                    Timings#timings.max, Timings#timings.min,
                    Timings#timings.sum, Timings#timings.count,
                    Timings#timings.slow_count, Timings#timings.fast_count,
                    ResultCount]).
