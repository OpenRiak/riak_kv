%% -------------------------------------------------------------------
%%
%% riak_query_server: Manage complex secondary index query.
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
-define(START_OPTS, []).
-define(DEFAULT_BUFFER_SIZE, 320).
-define(MINIMUM_BUFFER_SIZE, 16).
-define(DBTYPE_KEYS, ordered_set).
-define(ETS_SELECT_KEYS, [{{'$1'}, [], ['$1']}]).

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
        acc :: result_record()|pid()|redacted,
        max_results = unlimited :: pos_integer()|unlimited,
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
-type partial_result_map() ::
    #{binary() => key_list()|term_list()|non_neg_integer()|boolean()}.

-type result_record() :: #count_acc{}|#list_acc{}|#map_acc{}.
-type results() ::
    key_list()|term_list()|count_map()|non_neg_integer()|partial_result_map().

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

-type timings() :: #timings{}.

-type vnode_id() :: non_neg_integer().
-type vnode_monitor() :: #{vnode_id() => non_neg_integer()}|#{}.

-export_type([results/0, key_list/0, term_list/0, partial_result_map/0]).


%%%============================================================================
%%% API
%%%============================================================================

-spec query_start(
    riak_kv_query:complex_query_definition())
        -> {ok, pid(), non_neg_integer()}.
query_start(Query) ->
    {ok, Worker} = gen_server:start_link(?MODULE, Query, ?START_OPTS),
    {ok, Worker, riak_kv_query:get_reqid(Query)}.

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
    EvaluatedQuery = riak_kv_query:get_query_definition(Query),
    TimeoutS = riak_kv_query:get_timeout_secs(Query),
    MaxResults = riak_kv_query:get_maxresults(Query),
    AccType = accumulation_type(Query, MaxResults),
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
            Request =
                riak_kv_requests:new_query_request(
                    Bucket,
                    none,
                    riak_kv_query:get_querytype(Query),
                    AccType,
                    riak_kv_query:get_returnterms(Query),
                    calculate_buffer_size(length(CoverageVnodes), MaxResults),
                    EvaluatedQuery
                ),
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
                case init_acc(AccType) of
                    start_queue ->
                        start_queue(AccType, Query, Bucket, ReqID, From);
                    InitAcc ->
                        InitAcc
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
                    max_results = MaxResults,
                    result_encoding_fun
                        = riak_kv_query:get_result_encodingfun(Query)
                }
            }
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

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
    #state{req_id = ReqID, result_table = none, max_results = MR} = State)
        when AccOpt == terms, is_integer(MR) ->
    {AccOpt, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {AccOpt, Results},
            {AccOpt, (State#state.acc)#list_acc.results}
        ),
    {NextResults, ResultTable} =
        case length(UpdResults) of
            CountSoFar when CountSoFar < MR, CountSoFar < ?ETS_THRESHOLD ->
                riak_kv_vnode:ack_keys(From),
                {UpdResults, none};
            CountSoFar when CountSoFar < MR, CountSoFar >= ?ETS_THRESHOLD ->
                riak_kv_vnode:ack_keys(From),
                NewTable = ets:new(query_results, [?DBTYPE_KEYS, private]),
                true = ets:insert_new(NewTable, UpdResults),
                {#list_acc{}, NewTable};
            _CountSoFar ->
                {CandidateResults, _Discards} = lists:split(MR, UpdResults),
                case {hd(Results), lists:last(CandidateResults)} of
                    {LastR, LastCandidate} when LastR >= LastCandidate ->
                        riak_kv_vnode:stop_fold(From),
                        self() ! {{ReqID, Vnode}, done};
                    _ ->
                        riak_kv_vnode:ack_keys(From)
                end,
                {CandidateResults, none}
        end,
    {
        noreply,
        State#state{
            vnode_monitor = update_monitor(Vnode, State#state.vnode_monitor),
            acc = #list_acc{results = NextResults},
            result_table = ResultTable
        }
    };
handle_info(
    {{ReqID, Vnode}, {From, _B, {AccOpt, Results}}}, 
    #state{req_id = ReqID, result_table = Table, max_results = MR} = State)
        when AccOpt == terms, is_integer(MR) ->
    LastResult = hd(Results),
    true = ets:insert(Table, lists:reverse(Results)),
    case ets:info(Table, size) of
        TableSize when TableSize =< MR ->
            % Maybe done, but code is simpler to ignore optimisation as limit
            % on select_reverse =/= 0
            riak_kv_vnode:ack_keys(From);
        TableSize ->
            Excess = TableSize - MR,
            {TrimList, _Continuation} =
                ets:select_reverse(
                    Table,
                    ?ETS_SELECT_KEYS,
                    Excess
                ),
            lists:foreach(
                fun(TermKey) -> ets:delete(Table, TermKey) end,
                TrimList
            ),
            case ets:last(Table) of
                LastTermKey when LastTermKey =< LastResult ->
                    riak_kv_vnode:stop_fold(From),
                    self() ! {{ReqID, Vnode}, done};
                _LastTermKey ->
                    riak_kv_vnode:ack_keys(From)
            end
    end,
    {
        noreply,
        State#state{
            vnode_monitor =
                update_monitor(Vnode, State#state.vnode_monitor)
        }
    };
handle_info(
        {{ReqID, Vnode}, {From, _B, {AccOpt, Results}}}, 
        #state{req_id = ReqID, result_table = none} = State)
            when AccOpt == keys; AccOpt == terms ->
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
            when AccOpt == keys; AccOpt == terms ->
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
    {{ReqID, Vnode}, {From, _B, {T, Results}}}, 
    #state{req_id = ReqID, result_table = none, acc = QFB} = State)
        when is_pid(QFB), T == raw_keys orelse T == raw_terms ->
    riak_kv_vnode:ack_keys(From),
    ok = riak_kv_query_filebuffer:aggregate_results(QFB, Results),
    {
        noreply,
        State#state{
            vnode_monitor =
                update_monitor(Vnode, State#state.vnode_monitor)
        }
    };
handle_info(
    {{ReqID, Vnode}, {From, _B, {T, Results}}}, 
    #state{req_id = ReqID, result_table = none} = State)
        when T == raw_keys; T == raw_terms ->
    riak_kv_vnode:ack_keys(From),
    {T, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {T, Results},
            {T, (State#state.acc)#list_acc.results}
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
        {{ReqID, Vnode}, {From, _B, {raw_count, Count}}},
        #state{req_id = ReqID} = State) ->
    riak_kv_vnode:ack_keys(From),
    {raw_count, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {raw_count, Count},
            {raw_count, (State#state.acc)#count_acc.results}
        ),
    {
        noreply,
        State#state{
            vnode_monitor = update_monitor(Vnode, State#state.vnode_monitor),
            acc = #count_acc{results = UpdResults}
        }
    };
handle_info(
        {{ReqID, Vnode}, {From, _B, {count, Count}}},
        #state{req_id = ReqID} = State) ->
    riak_kv_vnode:ack_keys(From),
    {count, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {count, Count},
            {count, (State#state.acc)#count_acc.results}
        ),
    {
        noreply,
        State#state{
            vnode_monitor = update_monitor(Vnode, State#state.vnode_monitor),
            acc = #count_acc{results = UpdResults}
        }
    };
handle_info(
        {{ReqID, Vnode}, {From, _B, {term_with_rawcount, RM}}},
        #state{req_id = ReqID} = State) ->
    riak_kv_vnode:ack_keys(From),
    {term_with_rawcount, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {term_with_rawcount, RM},
            {term_with_rawcount, (State#state.acc)#map_acc.results}
        ),
    {
        noreply,
        State#state{
            vnode_monitor = update_monitor(Vnode, State#state.vnode_monitor),
            acc = #map_acc{results = UpdResults}
        }
    };
handle_info(
        {{ReqID, Vnode}, {From, _B, {term_with_count, RM}}},
        #state{req_id = ReqID} = State) ->
    riak_kv_vnode:ack_keys(From),
    {term_with_count, UpdResults} =
        riak_kv_query_buffer:aggregate(
            {term_with_count, RM},
            {term_with_count, (State#state.acc)#map_acc.results}
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
    case {sets:size(UpdCoverageVnodes), State#state.acc} of
        {0, QFB} when is_pid(QFB) ->
            {ok, ResultsBuffered} =
                riak_kv_query_filebuffer:query_complete(QFB),
            log_timings(
                UpdTimings,
                State#state.bucket,
                ResultsBuffered
            ),
            {stop, normal, State};
        {0, Acc} ->
            Results =
                case State#state.result_table of
                    none ->
                        extract_results(Acc);
                    RT0 ->
                        ets:tab2list(RT0)
                end,
            LastResult =
                case State#state.max_results of
                    MR when
                            is_integer(MR),
                            is_list(Results),
                            MR == length(Results) ->
                        lists:last(Results);
                    _ ->
                        none
                    end,
            {raw, ClientReqID, ClientPid} = State#state.from,
            case State#state.result_encoding_fun of
                raw ->
                    ClientPid !
                        {ClientReqID, {Results, LastResult}};
                EncodingFun ->
                    ClientPid !
                        {ClientReqID, {EncodingFun(Results), LastResult}}
            end,
            ResultsSent =
                case State#state.result_table of
                    none ->
                        extract_count(Acc);
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
    {raw, ClientReqID, ClientPid} = State#state.from,
    ClientPid ! {ClientReqID, {error, <<"unexepected_event">>}},
    {stop, shutdown, State}.

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

-spec init_acc(riak_kv_query:accumulation_option()) -> result_record()|start_queue.
init_acc(KeysType) when KeysType == keys; KeysType == raw_keys ->
    #list_acc{};
init_acc(TermType) when TermType == terms; TermType == raw_terms ->
    #list_acc{};
init_acc(CountType) when CountType == count; CountType == raw_count ->
    #count_acc{};
init_acc(CountByType)
        when CountByType == term_with_count; CountByType == term_with_rawcount ->
    #map_acc{};
init_acc(QueueType)
        when QueueType == queue_raw_keys; QueueType == queue_raw_terms ->
    start_queue.

-spec start_queue(
    queue_raw_keys|queue_raw_terms,
    riak_kv_query:complex_query_definition(),
    riak_object:bucket(),
    req_id(),
    pid()
) -> 
    pid().
start_queue(QueueType, Query, Bucket, ReqID, From) ->
    ConvertedType =
        case QueueType of
            queue_raw_keys -> raw_keys;
            queue_raw_terms -> raw_terms
        end,
    {ok, RP} = application:get_env(riak_kv, query_dataroot),
    InactivitySecs =
        riak_kv_query:get_inactivity_timeout_secs(Query),
    {ok, RPid, RRef} =
        riak_kv_query_filebuffer_sup:start_query_filebuffer(
            node(),
            [RP, 1000 * InactivitySecs, Bucket, ConvertedType]
        ),
    From ! {ReqID, {result_queue, RRef}},
    RPid.

-spec update_monitor(vnode_id(), vnode_monitor()) -> vnode_monitor().
update_monitor(Vnode, VnodeMonitor) ->
    maps:update_with(Vnode, fun(V) -> V + 1 end, VnodeMonitor).

-spec calculate_buffer_size(pos_integer(), unlimited|pos_integer()) ->
    {pos_integer(), non_neg_integer()}.
calculate_buffer_size(_VnodeCount, unlimited) ->
    ConfiguredSize =
        application:get_env(riak_kv, query_buffer_size, ?DEFAULT_BUFFER_SIZE),
    BufferSize = max(ConfiguredSize, riak_kv_query_buffer:min_buffer()),
    {
        BufferSize,
        BufferSize div 2
    };
calculate_buffer_size(VnodeCount, MaxResults) ->
    {SBS, SJS} = calculate_buffer_size(VnodeCount, unlimited),
    ResultsPerVnode = (MaxResults div VnodeCount) div 2,
    case calculate_buffer_size(VnodeCount, unlimited) of
        {SBS, SJS} when SBS < ResultsPerVnode ->
            {SBS, SJS};
        _ when ResultsPerVnode > ?MINIMUM_BUFFER_SIZE ->
            % Using the default may lead to a big overshoot, so
            % use a smaller buffer size based on the max_results
            {ResultsPerVnode, ResultsPerVnode div 2};
        _ ->
            {?MINIMUM_BUFFER_SIZE, ?MINIMUM_BUFFER_SIZE div 2}
    end.

-spec accumulation_type(
    riak_kv_query:complex_query_definition(), pos_integer()|unlimited)
        -> riak_kv_query:accumulation_option().
accumulation_type(Query, unlimited) ->
    riak_kv_query:get_accumulator(Query);
accumulation_type(_Query, MaxResults) when is_integer(MaxResults) ->
    terms.

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
    ?LOG_INFO(
        "Index query on bucket=~p "
        "max_vnodeq=~w min_vnodeq=~w sum_vnodeq=~w count_vnodeq=~w "
        "slow_count_vnodeq=~w fast_count_vnodeq=~w result_count=~w",
        [
            Bucket,
            Timings#timings.max, Timings#timings.min,
            Timings#timings.sum, Timings#timings.count,
            Timings#timings.slow_count, Timings#timings.fast_count,
            ResultCount
        ]
    ).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

buffer_size_test() ->
    ?assertMatch({320, 160}, calculate_buffer_size(171, unlimited)),
    ?assertMatch({16, 8}, calculate_buffer_size(171, 1000)),
    ?assertMatch({29, 14}, calculate_buffer_size(171, 10000)).

-endif.