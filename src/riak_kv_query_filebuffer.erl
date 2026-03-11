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
%%
%% @doc Query buffer for aggregating results on disk.
%% 
%% Required for central aggregation and partial fetching of results - unlike
%% the riak_kv_query_buffer does not do local aggregation, just central
%% aggregation at the riak_kv_query_server
%% 
%% Assumption is that a riak_kv_query_server will pass received results using
%% aggregate_results/2.  Once all vnodes are complete it will call
%% query_complete/1.
%% 
%% Multiple result receiving process can in parallel call return_results/2.
%% The result should be returned in the order they were received, in batch
%% sizes of up to the requested size.  A result should be returned once and
%% only once.  Result receivers know that there are no more to fetch when
%% The count of results received is equal to the count of results returned and
%% also the query_complete flag is true.
%% 
%% The reference contains a random secret to protect against a pid() being
%% reused, and a result receiver pulling results from the wrong buffer. 

-module(riak_kv_query_filebuffer).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export(
    [
        new/4,
        return_results/3,
        aggregate_results/2,
        query_complete/1
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

-export(
    [
        safe_encode/1,
        safe_decode/1
    ]
). % Required only for riak_test

-record(state,
    {
        dlog :: term(), % disk_log:log() not exported
        continuation = start :: disk_log:continuation()|start,
        buffer = [] :: list()|redacted,
        rcv_count = 0 :: non_neg_integer(),
        rsp_count = 0 :: non_neg_integer(),
        query_complete = false :: boolean(),
        inactivity_timeout :: pos_integer(),
        queue_reference :: undefined|binary(),
        accumulation_option :: raw_keys|raw_terms,
        bucket :: riak_object:bucket()
            % The bucket is only required to confirm that query result
            % request is aligned with the bucket in the query.  Security
            % requirements may be per-bucket, so prevents an attempt to
            % read query results from another bucket (guessing the shared
            % secret)
    }
).

-define(RSPCOUNT_KEY, <<"returned_count">>).
-define(RCVCOUNT_KEY, <<"queued_count">>).
-define(QCOMPLETE_KEY, <<"query_complete">>).

-type inbound_results() ::
    riak_kv_query_server:key_list() | riak_kv_query_server:term_list().

%%%============================================================================
%%% API
%%%============================================================================

-spec new(
    file:name_all(),
    pos_integer(),
    riak_object:bucket(),
    riak_kv_query:accumulation_option()
) -> 
    {ok, pid(), binary()}.
new(RootPath, InactivityTimeoutMS, Bucket, AccOpt) ->
    {ok, Pid} =
        gen_server:start_link(
            ?MODULE,
            [RootPath, InactivityTimeoutMS, Bucket, AccOpt],
            []
        ),
    Ref = gen_server:call(Pid, get_ref, infinity),
    {ok, Pid, Ref}.

-spec aggregate_results(pid(), inbound_results()) -> ok.
aggregate_results(Pid, Results) when is_list(Results) ->
    gen_server:cast(Pid, {aggregate_results, Results}).

-spec return_results(
    {node(), pid(), binary()} | binary(),
    pos_integer(),
    riak_object:bucket()
) ->
    {ok, riak_kv_query_server:partial_result_map()} |
    {error, term()}.
return_results(RefB, MaxResults, B) when is_binary(RefB), MaxResults >= 0 ->
    return_results(decode_ref(RefB), MaxResults, B);
return_results({N, Pid, Reference}, MaxResults, B) when N == node() ->
    try
        true = check_pid(Pid),
        gen_server:call(
            Pid,
            {return_results, MaxResults, Reference, B},
            infinity
        )
    catch C:EP ->
        ?LOG_WARNING(
            "Call to Query FileBuffer failed with ~0p:~0p",
            [C, EP]
        ),
        {error, result_server_terminated}
    end;
return_results({Node, Pid, Reference}, MaxResults, B) ->
    try
        RemoteResult =
            erpc:call(
                Node,
                riak_kv_query_filebuffer,
                return_results,
                [{Node, Pid, Reference}, MaxResults, B]
            ),
        case RemoteResult of
            {ok, Result} when is_map(Result) ->
                {ok, Result};
            {error, Term} ->
                {error, Term}
        end
    catch 
        error:{erpc, ERpcErrorReason} ->
            ?LOG_WARNING(
                    "Error ~0p connecting to query buffer on ~0p from ~0p",
                    [ERpcErrorReason, Node, node()]
                ),
                {error, node_unreachable};
        _CP:UnexpectedError ->
                ?LOG_ERROR(
                    "Unexpected error ~0p fetching remote results",
                    [UnexpectedError]
                ),
                {error, unexpected_error}
    end;
return_results(_UnexpectedRefFormat, _MaxResults, _B) ->
    ?LOG_WARNING("Queue reference received in unexpected format"),
    {error, unexpected_reference_format}.


-spec query_complete(pid()) -> {ok, non_neg_integer()}.
query_complete(Pid) ->
    gen_server:call(Pid, query_complete, infinity).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([RootPath, InactivityTimeoutMS, Bucket, AccOpt]) ->
    QueueRef = crypto:strong_rand_bytes(4),
    QueueName = encode_ref(QueueRef),
    FilePath =
        riak_kv_overflow_queue:disklog_filename(
            RootPath,
            riak_kv_overflow_queue:generate_ordered_guid()
        ),
    ok = filelib:ensure_dir(FilePath),
    {ok, DL} = disk_log:open([{name, QueueName}, {file, FilePath}]),
    {
        ok,
        #state{
            dlog = DL,
            inactivity_timeout = InactivityTimeoutMS,
            queue_reference = QueueRef,
            accumulation_option = AccOpt,
            bucket = Bucket
        },
        InactivityTimeoutMS
    }.

handle_cast(
    {aggregate_results, Results}, State) ->
    ok = disk_log:alog_terms(State#state.dlog, Results),
    {
        noreply,
        State#state{rcv_count = State#state.rcv_count + length(Results)},
        State#state.inactivity_timeout
    }.

handle_call(get_ref, _From, State) ->
    case disk_log:info(State#state.dlog)
        of InfoList when is_list(InfoList) ->
            {name, Ref} = lists:keyfind(name, 1, InfoList),
            {
                reply,
                Ref,
                State,
                State#state.inactivity_timeout
            }
    end;
handle_call(
    {return_results, _MR, _Ref, OB}, _From, State = #state{bucket = QB})
        when OB =/= QB ->
    {
        reply,
        {error, incorrect_bucket},
        State,
        State#state.inactivity_timeout
    };
handle_call(
    {return_results, _MR, Ref, _OB},
    _From,
    State = #state{queue_reference = QR})
        when Ref =/= QR ->
    {
        reply,
        {error, incorrect_reference},
        State,
        State#state.inactivity_timeout
    };
handle_call(
    {return_results, MaxResults, _Ref, _OB}, _From, State = #state{buffer = B})
        when is_list(B), length(B) >= MaxResults ->
    {ExtractedResults, UpdBuffer} = lists:split(MaxResults, B),
    UpdRspCount = State#state.rsp_count + MaxResults,
    EncodedResults =
        encode_results(
            ExtractedResults,
            UpdRspCount,
            State#state.rcv_count,
            State#state.query_complete,
            State#state.accumulation_option
        ),
    {
        reply,
        {ok, EncodedResults},
        State#state{
            buffer = UpdBuffer,
            rsp_count = UpdRspCount
        },
        State#state.inactivity_timeout
    };
handle_call(
    {return_results, MaxResults, _Ref, _OB}, _From, State = #state{buffer = B})
        when is_list(B) ->
    {ExtendedBuffer, UpdContinuation} =
        replenish_buffer(
            State#state.dlog,
            B,
            State#state.continuation
        ),
    ResultCount = min(MaxResults, length(ExtendedBuffer)),
    UpdRspCount = State#state.rsp_count + ResultCount,
    {ExtractedResults, UpdBuffer} = lists:split(ResultCount, ExtendedBuffer),
    EncodedResults =
        encode_results(
            ExtractedResults,
            UpdRspCount,
            State#state.rcv_count,
            State#state.query_complete,
            State#state.accumulation_option
        ),
    {
        reply,
        {ok, EncodedResults},
        State#state{
            buffer = UpdBuffer,
            rsp_count = UpdRspCount,
            continuation = UpdContinuation
        },
        State#state.inactivity_timeout
    };
handle_call(query_complete, _From, State) ->
    {
        reply,
        {ok, State#state.rcv_count},
        State#state{query_complete = true},
        State#state.inactivity_timeout
    }.
    
handle_info(timeout, State) ->
    {stop, normal, State}.

terminate(_Reason, _State = #state{dlog = DLog}) ->
    case disk_log:info(DLog)
        of InfoList when is_list(InfoList) ->
            {file, FN} = lists:keyfind(file, 1, InfoList),
            ok = disk_log:close(DLog),
            file:delete(FN)
    end.

format_status(Status) ->
    case maps:get(reason, Status, normal) of
        terminate ->
            State = maps:get(state, Status),
            maps:update(
                state,
                State#state{buffer = redacted},
                Status
            );
        _ ->
            Status
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% Internal Functions
%%%============================================================================

-ifdef(TEST).

check_pid(Pid) -> is_process_alive(Pid).

-else.
-spec check_pid(pid()) -> boolean().
check_pid(Pid) ->
    is_process_alive(Pid)
        andalso
        ({error, not_found} /=
            supervisor:get_childspec(
                riak_kv_query_filebuffer_sup,
                Pid)
            )
    .

-endif.

-spec encode_ref(binary()) -> binary().
encode_ref(Ref) ->
    safe_encode(term_to_binary({node(), self(), Ref})).

-spec decode_ref(binary()) -> {node(), pid(), binary()}.
decode_ref(B64Ref) ->
    binary_to_term(safe_decode(B64Ref)).

-if(?OTP_RELEASE < 26).
safe_encode(Bin) ->
    iolist_to_binary(
        string:replace(
            iolist_to_binary(
                string:replace(
                    base64:encode(Bin),
                    "+",
                    "-",
                    all
                )
            ),
            "/",
            "_",
            all
        )
    ).

safe_decode(B64Ref) ->
    base64:decode(
        iolist_to_binary(
            string:replace(
                iolist_to_binary(
                    string:replace(
                        B64Ref,
                        "-",
                        "+",
                        all
                    )
                ),
            "_",
            "/",
            all
            )
        )
    ).
-else.
-define(ENCODE_OPTS, #{mode => urlsafe}).

safe_encode(Bin) ->
    base64:encode(Bin, ?ENCODE_OPTS).

safe_decode(B64Ref) ->
    base64:decode(B64Ref, ?ENCODE_OPTS).
-endif.

-spec encode_results(
    inbound_results(),
    non_neg_integer(),
    non_neg_integer(),
    boolean(),
    riak_kv_query:accumulation_option()
) ->
        riak_kv_query_server:partial_result_map().
encode_results(Results, RspCount, RcvCount, Complete, AccOpt) ->
    #{
        riak_kv_wm_query:get_result_key(AccOpt) => Results,
        ?RSPCOUNT_KEY => RspCount,
        ?RCVCOUNT_KEY => RcvCount,
        ?QCOMPLETE_KEY => Complete
    }.

-spec replenish_buffer(
    term(), inbound_results(), disk_log:continuation()|start
) ->
    {inbound_results(), disk_log:continuation()|start}.
replenish_buffer(DLog, Buffer, Continuation) ->
    case disk_log:chunk(DLog, Continuation) of
        eof ->
            {Buffer, Continuation};
        {Continuation2, Terms} ->
            {Buffer ++ Terms, Continuation2}
    end.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

unlink_new(RootPath, InactivityTimeoutMS, Bucket, AccOpt) ->
    {ok, Pid} =
        gen_server:start(
            ?MODULE,
            [RootPath, InactivityTimeoutMS, Bucket, AccOpt],
            []
        ),
    Ref = gen_server:call(Pid, get_ref, infinity),
    {ok, Pid, Ref}.


to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

to_term(N) ->
    list_to_binary(io_lib:format("T~8..0B", [N])).

generate_keys(Max) ->
    lists:map(fun to_key/1, lists:seq(1, Max)).

generate_terms(Max) ->
    lists:map(fun(N) -> {to_term(N), to_key(N)} end, lists:seq(1, Max)).

fetch_all_test_() ->
    {timeout, 30, fun fetch_all_tester/0}.

fetch_all_tester() ->
    fetch_all_tester(100000, raw_keys),
    fetch_all_tester(60000, raw_terms).

fetch_all_tester(MaxCount, AccOpt) ->
    Generator =
        case AccOpt of
            raw_keys ->
                fun generate_keys/1;
            raw_terms ->
                fun generate_terms/1
        end,
    Bucket = {<<"BucketType">>, <<"BucketName">>},
    BatchSize = MaxCount div 20,
    RootPath = riak_core_test_util:get_test_dir("filebuffer_test/"),
    AllKeys = Generator(MaxCount),
    {InitialKeys, RestKeys} = lists:split(MaxCount div 2, AllKeys),
    {ok, QFB, QFR} = unlink_new(RootPath, 2000, Bucket, AccOpt),
    ExpectedInitResult =
        #{
            riak_kv_wm_query:get_result_key(AccOpt) => [],
            ?RSPCOUNT_KEY => 0,
            ?RCVCOUNT_KEY => 0,
            ?QCOMPLETE_KEY => false
        },
    ?assertMatch(
        {ok, ExpectedInitResult},
        return_results(QFR, BatchSize, Bucket)
    ),
    ?assertMatch(
        {error, incorrect_bucket},
        return_results(QFR, BatchSize, {<<"BucketType">>, <<"ButWrongName">>})
    ),
    send_keys_in_batches(BatchSize, InitialKeys, QFB),
    ExpectedEmptyResult =
        #{
            riak_kv_wm_query:get_result_key(AccOpt) => [],
            ?RSPCOUNT_KEY => 0,
            ?RCVCOUNT_KEY => MaxCount div 2,
            ?QCOMPLETE_KEY => false
        },
    ?assertMatch({ok, ExpectedEmptyResult}, return_results(QFR, 0, Bucket)),
    InitialRsp =
        fetch_keys_in_batches(BatchSize div 2, [], QFR, Bucket, AccOpt),
    ?assert(MaxCount div 2 == length(InitialRsp)),
    ?assertMatch(InitialKeys, InitialRsp),
    ExpectedHWResult =
        #{
            riak_kv_wm_query:get_result_key(AccOpt) => [],
            ?RSPCOUNT_KEY => MaxCount div 2,
            ?RCVCOUNT_KEY => MaxCount div 2,
            ?QCOMPLETE_KEY => false
        },
    ?assertMatch(
        {ok, ExpectedHWResult},
        return_results(QFR, BatchSize, Bucket)
    ),
    send_keys_in_batches(BatchSize, RestKeys, QFB),
    ?assertMatch({ok, MaxCount}, query_complete(QFB)),
    TotalRsp =
        fetch_keys_in_batches(
            BatchSize div 2,
            lists:reverse(InitialRsp),
            QFR,
            Bucket,
            AccOpt
        ),
    ?assert(MaxCount == length(TotalRsp)),
    ?assertMatch(AllKeys, TotalRsp),
    ExpectedFinalResult =
        #{
            riak_kv_wm_query:get_result_key(AccOpt) => [],
            ?RSPCOUNT_KEY => MaxCount,
            ?RCVCOUNT_KEY => MaxCount,
            ?QCOMPLETE_KEY => true
        },
    ?assertMatch(
        {ok, ExpectedFinalResult},
        return_results(QFR, BatchSize, Bucket)
    ),
    {ok, Files} = file:list_dir(RootPath),
    BadRef = safe_encode(term_to_binary({node(), QFB, <<>>})),
    ?assertMatch({_N, QFB, <<>>}, decode_ref(BadRef)),
    ?assertMatch(
        {error, incorrect_reference},
        return_results(BadRef, BatchSize, Bucket)
    ),
    timer:sleep(2000 + 10),
    ?assertMatch(
        {error, result_server_terminated},
        return_results(QFR, BatchSize, Bucket)
    ),
    {ok, FilesLessOne} = file:list_dir(RootPath),
    ?assert(length(Files) - 1 == length(FilesLessOne)).


send_keys_in_batches(BatchSize, KeyList, QFB) when length(KeyList) < BatchSize ->
    aggregate_results(QFB, KeyList);
send_keys_in_batches(BatchSize, KeyList, QFB) ->
    {Results, Rest} = lists:split(BatchSize, KeyList),
    aggregate_results(QFB, Results),
    send_keys_in_batches(BatchSize, Rest, QFB).

fetch_keys_in_batches(MaxResults, Acc, QFR, Bucket, AccOpt) ->
    {ok, ResultMap} = return_results(QFR, MaxResults, Bucket),
    UpdAcc =
        lists:reverse(
            maps:get(
                riak_kv_wm_query:get_result_key(AccOpt),
                ResultMap
            )
        ) ++ Acc,
    case {
        maps:get(?RSPCOUNT_KEY, ResultMap),
        maps:get(?RCVCOUNT_KEY, ResultMap)
    } of
        {Rsp, Rcv} when Rsp == Rcv ->
            lists:reverse(UpdAcc);
        {Rsp, Rcv} when Rsp < Rcv ->
            fetch_keys_in_batches(MaxResults, UpdAcc, QFR, Bucket, AccOpt)
    end.

-if(?OTP_RELEASE == 26).
legacy_encode(Bin) ->
    iolist_to_binary(
        string:replace(
            iolist_to_binary(
                string:replace(
                    base64:encode(Bin),
                    "+",
                    "-",
                    all
                )
            ),
            "/",
            "_",
            all
        )
    ).

legacy_decode(B64Ref) ->
    base64:decode(
        iolist_to_binary(
            string:replace(
                iolist_to_binary(
                    string:replace(
                        B64Ref,
                        "-",
                        "+",
                        all
                    )
                ),
            "_",
            "/",
            all
            )
        )
    ).

urlsafe_encode_test() ->
    RandomNoise =
        lists:map(
            fun(I) -> crypto:strong_rand_bytes(I) end,
            lists:seq(1, 128)
        ),
    lists:foreach(
        fun(Bin) ->
            LB64 = legacy_encode(Bin),
            NB64 = safe_encode(Bin),
            ?assertMatch(Bin, safe_decode(LB64)),
            ?assertMatch(Bin, legacy_decode(NB64))
        end,
        RandomNoise
    ).

-endif.

-endif.