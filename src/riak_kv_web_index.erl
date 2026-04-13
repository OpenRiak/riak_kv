%% --------------------------------------------------------
%%
%% Copyright (c) 2026 Martin Sumner
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
%% @doc Handler for HTTP API requests for (legacy) 2i queries

-module(riak_kv_web_index).

-include("riak_kv_web.hrl").

-behaviour(riak_api_web_handler).

-export(
    [
        match_route/3,
        check_permissions/4,
        parse_query_params/2,
        parse_request_headers/2,
        process_request/2,
        record_request/3
    ]
).

-record(context,
    {
        client = riak_client:new(node(), self()) :: riak_client:riak_client(),
        bucket :: riak_object:bucket(),
        field :: binary(),
        field_type = bin :: bin|int|dollar,
        start_term :: binary(),
        end_term :: binary(),
        max_results = all :: pos_integer() | all,
        return_terms = false :: boolean(),
        pagination_sort = false :: boolean(),
        stream = false :: boolean(),
        term_regex :: binary() | undefined,
        continuation :: binary() | undefined,
        timeout = 60000 :: pos_integer()
    }
).

-type context() :: #context{}.

%% ===================================================================
%% Callback functions
%% ===================================================================

-spec match_route(
    riak_api_web_acceptor:method(),
    unicode:chardata(),
    list(unicode:chardata())
) ->
    nomatch
    | {method_not_allowed, list(riak_api_web_acceptor:method())}
    | {ok, riak_api_web_handler:limits(), context()}.
match_route(
    Method,
    _Path,
    [<<"types">>, BucketType, <<"buckets">>, Bucket, <<"index">>, Idx, ST, ET]
) ->
    case Method of
        'GET' ->
            Context =
                #context{
                    bucket = riak_kv_web_common:set_bucket(BucketType, Bucket),
                    field = Idx,
                    start_term = ST,
                    end_term = ET
                },
            {ok, size_limits(), Context};
        _ ->
            {method_not_allowed, ['GET']}
    end;
match_route(
    Method,
    Path,
    [<<"types">>, BType, <<"buckets">>, Bucket, <<"index">>, Idx, T]
) ->
    match_route(
        Method,
        Path,
        [<<"types">>, BType, <<"buckets">>, Bucket, <<"index">>, Idx, T, T]
    );
match_route(
    Method,
    Path,
    [<<"buckets">>, _Bucket, <<"index">>, _Idx, _ST, _ET] = SplitPath
) ->
    match_route(
        Method,
        Path,
        [<<"types">>, <<"default">>] ++ SplitPath
    );
match_route(
    Method,
    Path,
    [<<"buckets">>, _Bucket, <<"index">>, _Idx, T] = SplitPath
) ->
    match_route(
        Method,
        Path,
        [<<"types">>, <<"default">>] ++ SplitPath ++ [T]
    );
match_route(_Method, _Path, _SP) ->
    nomatch.


%% @doc check_permissions for using this module or route
-spec check_permissions(
    riak_api_web_headers:headers(),
    riak_api_web_socket:scheme(),
    riak_api_web_handler:peer_ip(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
check_permissions(ReqHeaders, Scheme, Peer, Ctx) ->
    Check =
        riak_kv_web_common:check_permissions(
            ReqHeaders,
            Scheme,
            Peer,
            Ctx#context.bucket,
            riak_kv_index
        ),
    case Check of
        true ->
            B = Ctx#context.bucket,
            case riak_kv_web_common:check_type_exists(B) of
                true ->
                    % TODO: Add referrer check in
                    {ok, Ctx};
                false ->
                    {halt, 404, [], <<"Unknown bucket type: ~s">>, [B]}
            end;
        HaltResponse ->
            HaltResponse
    end.

%% @doc parse and validate query params, passed as a map
-spec parse_query_params(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
parse_query_params(Params, Ctx) ->
    maybe
        {ok, Ctx0} ?= validate_query_type(Ctx),
        {ok, Ctx1} ?= validate_timeout(Params, Ctx0),
        {ok, Ctx2} ?= validate_max_results(Params, Ctx1),
        {ok, Ctx3} ?= validate_maybe_true(return_terms, Params, Ctx2),
        {ok, Ctx4} ?= validate_maybe_true(pagination_sort, Params, Ctx3),
        {ok, Ctx5} ?= validate_maybe_true(stream, Params, Ctx4),
        {ok, Ctx6} ?= validate_term_regex(Params, Ctx5),
        {ok, Ctx7} ?= validate_continuation(Params, Ctx6),
        {ok, Ctx7}
    else
        HaltResponse ->
            HaltResponse
    end.

%% @doc parse and validate the request headers
-spec parse_request_headers(
    riak_api_web_headers:headers(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
parse_request_headers(ReqHeaders, Ctx) ->
    case riak_api_web_headers:get_value('Accept', ReqHeaders) of
        undefined ->
            {ok, Ctx};
        AcceptType ->
            {Match, _} =
                riak_kv_web_common:type_match(
                    <<"application/json">>,
                    AcceptType
                ),
            case Match of
                true ->
                    {ok, Ctx};
                false ->
                    ErrMsg = <<"application/json must be accepted">>,
                    {halt, 406, [?TXT_HEADER], ErrMsg, []}
            end
    end.

%% @doc Process the request and produce a response
-spec process_request(
    riak_api_web_body:req_body(),
    context()
) ->
    {
        ok,
        {
            riak_api_web_acceptor:response_code(),
            riak_api_web_headers:header_list(),
            riak_api_web_handler:response_body(),
            boolean(),
            riak_api_web_body:req_body()
        },
        context()
    }
    | riak_api_web_acceptor:halt_response().
process_request(RqBdy, Ctx) ->
    case riak_kv_web_common:confirm_empty_body(RqBdy) of
        {ok, UpdBody} ->
            IndexQuery =
                riak_index:to_index_query(
                    [
                        {field, Ctx#context.field},
                        {start_term, Ctx#context.start_term},
                        {end_term, Ctx#context.end_term},
                        {return_terms, Ctx#context.return_terms},
                        {continuation, Ctx#context.continuation},
                        {term_regex, Ctx#context.term_regex}
                    ]
                ),
            case {IndexQuery, Ctx#context.stream} of
                {{ok, Q}, true} ->
                    process_stream_query(Q, Ctx, UpdBody);
                {{ok, Q}, false} ->
                    process_memory_query(Q, Ctx, UpdBody);
                {{error, Error}, _} ->
                    ErrMsg = <<"Error parsing query ~0p">>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, [Error]}
            end;
        {error, content_too_large} ->
            {halt, 413, [], <<>>, []}
    end.

%% @doc Record the output of the interaction
-spec record_request(
    riak_api_web_handler:timings(),
    riak_api_web_handler:completion(),
    context()
) ->
    ok.
record_request(_Timings, _Completion, _Ctx) ->
    ok.


%% ===================================================================
%% Validation functions
%% ===================================================================

-spec validate_timeout(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
validate_timeout(Params, Ctx) ->
    case riak_kv_web_common:get_timeout(Params) of
        {ok, none} ->
            {ok, Ctx};
        {ok, Timeout} ->
            {ok, Ctx#context{timeout = Timeout}};
        HaltResponse ->
            HaltResponse
    end.

-spec validate_max_results(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
validate_max_results(Params, Ctx) ->
    case lists:keyfind(<<"max_results">>, 1, Params) of
        false ->
            {ok, Ctx};
        {<<"max_results">>, MR} when is_binary(MR) ->
            try
                IntMR = binary_to_integer(MR),
                true = IntMR > 0,
                {ok, Ctx#context{max_results = IntMR}}
            catch
                _:_ ->
                    ErrMsg = 
                        <<
                            "Invalid max_results ~0p "
                            "is not a positive integer"
                        >>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, [MR]}
            end
    end.

-spec validate_maybe_true(
    return_terms|pagination_sort|stream,
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
validate_maybe_true(Key, Params, Ctx) ->
    case lists:keyfind(atom_to_binary(Key), 1, Params) of
        false ->
            {ok, Ctx};
        {_, MT} ->
            case riak_kv_web_common:normalise_boolean_param(MT) of
                true ->
                    case Key of
                        return_terms ->
                            KeyOnly =
                                Ctx#context.field_type == dollar
                                orelse
                                Ctx#context.start_term == Ctx#context.end_term,
                            case KeyOnly of
                                true ->
                                    {ok, Ctx};
                                false ->
                                    {ok, Ctx#context{return_terms = true}}
                            end;
                        pagination_sort ->
                            {ok, Ctx#context{pagination_sort = true}};
                        stream ->
                            {ok, Ctx#context{stream = true}}
                    end;
                bad_param ->
                    ErrMsg =
                        "Invalid ~0p. ~0p is not a boolean",
                    {halt, 400, [?TXT_HEADER], ErrMsg, [Key, MT]};
                _ ->
                    {ok, Ctx}
            end
    end.

-spec validate_query_type(
    context()
) -> 
    {ok, context()}|riak_api_web_acceptor:halt_response().
validate_query_type(Ctx = #context{field = DollarI})
        when DollarI == <<"$key">>; DollarI == <<"$bucket">> ->
    {ok, Ctx#context{field_type = dollar}};
validate_query_type(Ctx = #context{field = Index}) when is_binary(Index) ->
    case byte_size(Index) of
        L when L > 4  ->
            <<_Idx:(L - 4)/binary, Suffix:4/binary>> = Index,
            case string:casefold(Suffix) of
                <<"_bin">> ->
                    {ok, Ctx#context{field_type = bin}};
                <<"_int">> ->
                    {ok, Ctx#context{field_type = int}};
                _ ->
                    ErrMsg = <<"Invalid IndexName ~0p">>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, [Index]}
            end;
        _ ->
            ErrMsg = <<"Invalid IndexName ~0p">>,
            {halt, 400, [?TXT_HEADER], ErrMsg, [Index]}
    end.

-spec validate_term_regex(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()}|riak_api_web_acceptor:halt_response().
validate_term_regex(Params, Ctx) ->
    case lists:keyfind(<<"term_regex">>, 1, Params) of
        false ->
            {ok, Ctx};
        {<<"term_regex">>, Re} when is_binary(Re) ->
            case {re:compile(Re), Ctx#context.field_type} of
                {{ok, _CompiledRe}, FT} when FT =/= int ->
                    {ok, Ctx#context{term_regex = Re}};
                {_, int} ->
                    ErrMsg =
                        <<
                            "Can not use term regular expressions"
                            " on integer queries"
                        >>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, []};
                {{error, ErrorSpec}, _} ->
                    ErrMsg =
                        << "Invalid term regular expression ~p : ~p">>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, [Re, ErrorSpec]}
            end
    end.

-spec validate_continuation(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()}|riak_api_web_acceptor:halt_response().
validate_continuation(Params, Ctx) ->
    case lists:keyfind(<<"continuation">>, 1, Params) of
        false ->
            {ok, Ctx};
        {<<"continuation">>, C} when is_binary(C) ->
            try
                _ = riak_index:decode_continuation(C),
                {ok, Ctx#context{continuation = C, pagination_sort = true}}
            catch
                _ : _ ->
                    ErrMsg =
                        << "Invalid continuation ~p - cannot be decoded">>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, [C]}
            end
    end.

%% ===================================================================
%% Query Handling
%% ===================================================================

-spec process_memory_query(
    any(),
    context(),
    riak_api_web_body:req_body()
) ->
    {
        ok,
        {
            riak_api_web_acceptor:response_code(),
            riak_api_web_headers:header_list(),
            riak_api_web_handler:response_body(),
            boolean(),
            riak_api_web_body:req_body()
        },
        context()
    }
    | riak_api_web_acceptor:halt_response().
process_memory_query(Query, Ctx, ReqBody) ->
    Client = Ctx#context.client,
    Bucket = Ctx#context.bucket,
    InitOpts =
        case Ctx#context.pagination_sort of
            true ->
                [
                    {max_results, Ctx#context.max_results},
                    {pagination_sort, true}
                ];
            false ->
                [
                    {max_results, Ctx#context.max_results}
                ]
        end,
    Opts = riak_index:add_timeout_opt(Ctx#context.timeout, InitOpts),

    %% Do the index lookup...
    case riak_client:get_index(Bucket, Query, Opts, Client) of
        {ok, Results} ->
            Continuation =
                make_continuation(
                    Ctx#context.max_results,
                    Results
                ),
            JsonResults =
                encode_results( 
                    Ctx#context.return_terms,
                    Results,
                    Continuation
                ),
            {
                ok,
                {
                    200,
                    [{'Content-Type', <<"application/json">>}],
                    iolist_to_binary(JsonResults),
                    true,
                    ReqBody
                },
                Ctx
            };
        {error, timeout} ->
            ErrMsg = <<"Request timed out">>,
            {halt, 503, [?TXT_HEADER], ErrMsg, []};
        {error, Reason} ->
            ErrMsg = <<"Query failed due to ~0p">>,
            {halt, 503, [?TXT_HEADER], ErrMsg, [Reason]}
    end.

-spec process_stream_query(
    any(),
    context(),
    riak_api_web_body:req_body()
) ->
    {
        ok,
        {
            riak_api_web_acceptor:response_code(),
            riak_api_web_headers:header_list(),
            riak_api_web_handler:response_body(),
            boolean(),
            riak_api_web_body:req_body()
        },
        context()
    }.
process_stream_query(Query, Ctx, ReqBody) ->
    Client = Ctx#context.client,
    Bucket = Ctx#context.bucket,

    %% Create a new multipart/mixed boundary
    Boundary = riak_core_util:unique_id_62(),
    CTypeHdr =
        {
            'Content-Type',
            iolist_to_binary(
                [<<"multipart/mixed;boundary=">>, list_to_binary(Boundary)]
            )
        },
    InitOpts =
        case Ctx#context.pagination_sort of
            true ->
                [
                    {max_results, Ctx#context.max_results},
                    {pagination_sort, true}
                ];
            false ->
                [
                    {max_results, Ctx#context.max_results}
                ]
        end,
    Opts = riak_index:add_timeout_opt(Ctx#context.timeout, InitOpts),
    {ok, ReqID, FSMPid} = 
        riak_client:stream_get_index(Bucket, Query, Opts, Client),
    StreamFun =
        index_stream_fun(
            {ReqID, FSMPid},
            {Boundary, Ctx#context.return_terms, Ctx#context.max_results}, 
            {undefined, 0},
            Ctx#context.timeout
        ),
    {ok, {200, [CTypeHdr], {stream, StreamFun}, true, ReqBody}, Ctx}.

-type stream_fun() ::
    fun(() -> 
        {binary(), stream_fun()} |
        done |
        error
    ).

-spec index_stream_fun(
    {non_neg_integer(), pid()},
    {unicode:chardata(), boolean(), pos_integer()|all},
    {{binary(), riak_object:key()}|undefined, non_neg_integer()},
    non_neg_integer()
) ->
    stream_fun().
index_stream_fun(
    {ReqID, FSMPid},
    {Boundary, ReturnTerms, MaxResults},
    {LastResult, Count},
    Timeout
) ->
    fun() ->
        receive
            {ReqID, done} ->
                ToComeLessLast =
                    case MaxResults of
                        MR when is_integer(MR) ->
                            (MR - Count) + 1;
                        MR ->
                            MR
                    end,
                Final =
                    case make_continuation(ToComeLessLast, [LastResult]) of
                        undefined ->
                            ["\r\n--", Boundary, "--\r\n"];
                        Continuation ->
                            Json =
                                riak_kv_wm_json:encode(
                                    #{?Q_2I_CONTINUATION_BIN => Continuation}
                                ),
                            [
                                "\r\n--", Boundary, "\r\n",
                                "Content-Type: application/json\r\n\r\n",
                                Json,
                                "\r\n--", Boundary, "--\r\n"
                            ]
                    end,
                {
                    iolist_to_binary(Final),
                    fun() -> done end
                };
            {ReqID, {results, []}} ->
                {
                    <<>>,
                    index_stream_fun(
                        {ReqID, FSMPid},
                        {Boundary, ReturnTerms, MaxResults},
                        {LastResult, Count},
                        Timeout
                    )
                };
            {ReqID, {results, Results}} ->
                JsonResults =
                    encode_results(ReturnTerms, Results, undefined),
                Body = 
                [
                    "\r\n--", Boundary, "\r\n",
                    "Content-Type: application/json\r\n\r\n",
                    JsonResults
                ],
                {
                    iolist_to_binary(Body),
                    index_stream_fun(
                        {ReqID, FSMPid},
                        {Boundary, ReturnTerms, MaxResults},
                        {lists:last(Results), Count + length(Results)},
                        Timeout
                    )
                };
            {ReqID, _Error} ->
                error
        after Timeout ->
            whack_index_fsm(ReqID, FSMPid),
            error
        end
    end.

%% @doc When a streaming index query ends due to timeout, the web acceptor
%% process should not receive messages left over from the query
-spec whack_index_fsm(non_neg_integer(), pid()) -> ok.
whack_index_fsm(ReqID, Pid) ->
    wait_for_death(Pid),
    clear_index_fsm_msgs(ReqID).

wait_for_death(Pid) ->
    Ref = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', Ref, process, Pid, _Info} ->
            ok
    end.

clear_index_fsm_msgs(ReqID) ->
    receive
        {ReqID, _} ->
            clear_index_fsm_msgs(ReqID)
    after
        0 ->
            ok
    end.

%% ===================================================================
%% Internal Functions
%% ===================================================================

-spec make_continuation(
    all | non_neg_integer(),
    list()
) -> 
    binary() | undefined.
make_continuation(MR, Results) when is_integer(MR), length(Results) == MR ->
    riak_index:make_continuation(Results);
make_continuation(_, _) ->
    undefined.

size_limits() ->
    {
        32,
        1024,
        0
    }.

%% ===================================================================
%% JSON Encoding implementations
%% ===================================================================

otp_encode_results(true, Results, undefined) ->
    riak_kv_wm_json:encode(
        #{?Q_RESULTS_BIN => Results},
        fun results_encode/2
    );
otp_encode_results(true, Results, Continuation) ->
    riak_kv_wm_json:encode(
        #{?Q_RESULTS_BIN => Results,
            ?Q_2I_CONTINUATION_BIN => Continuation},
        fun results_encode/2
    );
otp_encode_results(false, Results, undefined) ->
    riak_kv_wm_json:encode(
        #{?Q_KEYS_BIN => Results},
        fun keys_encode/2
    );
otp_encode_results(false, Results, Continuation) ->
    riak_kv_wm_json:encode(
        #{?Q_KEYS_BIN => Results,
            ?Q_2I_CONTINUATION_BIN => Continuation},
        fun keys_encode/2
    ).

results_encode({Term, Key}, Encode) when is_binary(Term), is_binary(Key) ->
    [${, [Encode(Term, Encode), $: | Encode(Key, Encode)], $}];
results_encode({Term, Key}, Encode) when is_integer(Term), is_binary(Key) ->
    [
        ${,
        [Encode(integer_to_binary(Term), Encode), $: | Encode(Key, Encode)],
        $}
    ];
results_encode(Result, Encode) ->
    riak_kv_wm_json:encode_value(Result, Encode).

keys_encode({_Term, Key}, Encode) when is_binary(Key) ->
    riak_kv_wm_json:encode_value(Key, Encode);
keys_encode(Object, Encode) ->
    riak_kv_wm_json:encode_value(Object, Encode).

encode_results(ReturnTerms, Results, Continuation) ->
    otp_encode_results(ReturnTerms, Results, Continuation).

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

otp_encode_results(ReturnTerms, Results) ->
    otp_encode_results(ReturnTerms, Results, undefined).

encoder_test_() ->
    {timeout, 600, fun encode_tester/0}.

encode_tester() ->
    timer:sleep(100), % awkward silence to tidy screen output
    encode_implementation_tester(otp).

encode_implementation_tester(_Otp) ->
    garbage_collect(),

    io:format(user, "~n~nTesting Implementation ~w~n", [otp]),
    ResultSetsTiny =
        [{<<"1K">>, large_results(1000)},
            {<<"2K">>, large_results(2000)},
            {<<"3K">>, large_results(3000)},
            {<<"5K">>, large_results(5000)},
            {<<"8K">>, large_results(8000)}],
    encode_tester(ResultSetsTiny, microseconds),

    garbage_collect(),

    ResultSetsSmall =
        [{<<"13K">>, large_results(13000)},
            {<<"21K">>, large_results(21000)},
            {<<"34K">>, large_results(34000)},
            {<<"55K">>, large_results(55000)}],
    encode_tester(ResultSetsSmall, milliseconds),

    garbage_collect(),

    ResultSetsMid =
        [{<<"100K">>, large_results(100000)},
            {<<"200K">>, large_results(200000)},
            {<<"300K">>, large_results(300000)},
            {<<"500K">>, large_results(500000)}],
    encode_tester(ResultSetsMid, milliseconds),

    ok.

encode_tester(ResultSets, Unit) ->
    Divisor =
        case Unit of
            microseconds ->
                1;
            milliseconds ->
                1000
        end,
    Fun = fun otp_encode_results/2,
    
    TotalTime =
        lists:sum(
            lists:map(
                fun({Tag, RS}) ->
                    garbage_collect(),
                    {TC, _Json} =
                        timer:tc(fun() -> Fun(true, RS) end),
                    io:format(
                        user,
                        "Result set of ~s in ~w ~p ",
                        [Tag, TC div Divisor, Unit]),
                    TC
                end,
                ResultSets
            )
        ),
    io:format(user, "Total time ~w ~p~n", [TotalTime div Divisor, Unit]).

large_results(N) ->
    lists:map(
        fun(I) -> {generate_term(I), generate_key(I)} end,
        lists:seq(1, N)).

generate_term(I) ->
    iolist_to_binary(io_lib:format("q~9..0B", [I])).

generate_key(K) ->
    iolist_to_binary(io_lib:format("k~9..0B", [rand:uniform(K)])).

extract_params(URI) ->
    uri_string:dissect_query(
        maps:get(
            query,
            uri_string:normalize(URI, [return_map])
        )
    ).

test_uri(URI) ->
    URIBase = <<"types/T/buckets/B/index/index_bin/aStart/zEnd">>,
    << URIBase/binary, URI/binary >>.

valid_dollarkey_test() ->
    InitCtx =
        #context{
            bucket = {<<"T">>, <<"B">>},
            field = <<"$key">>,
            start_term = <<"aStart">>,
            end_term = <<"zEnd">>
        },
    {ok, Ctx1} = parse_query_params([], InitCtx),
    ?assertMatch(dollar, Ctx1#context.field_type),
    {ok, Ctx2} = parse_query_params([], InitCtx#context{field = <<"$bucket">>}),
    ?assertMatch(dollar, Ctx2#context.field_type).

validate_return_terms_test() ->
    % If $bucket, $key or equality query - return_terms should be ignored
    InitCtx =
        #context{
            bucket = {<<"T">>, <<"B">>},
            field = <<"$key">>,
            start_term = <<"aStart">>,
            end_term = <<"zEnd">>
        },
    {ok, Ctx1} = parse_query_params([{<<"return_terms">>, true}], InitCtx),
    ?assertMatch(false, Ctx1#context.return_terms),
    {ok, Ctx2} =
        parse_query_params(
            [{<<"return_terms">>, true}],
            InitCtx#context{field = <<"$bucket">>}
        ),
    ?assertMatch(false, Ctx2#context.return_terms),
    {ok, Ctx3} =
        parse_query_params(
            [{<<"return_terms">>, true}],
            InitCtx#context{
                field = <<"field_bin">>,
                start_term = <<"term">>,
                end_term = <<"term">>
            }
        ),
    ?assertMatch(false, Ctx3#context.return_terms),
    ValidCtx =
        #context{
            bucket = {<<"T">>, <<"B">>},
            field = <<"field_bin">>,
            start_term = <<"aStart">>,
            end_term = <<"zEnd">>
        },
    {ok, Ctx4} = parse_query_params([{<<"return_terms">>, true}], ValidCtx),
    ?assertMatch(true, Ctx4#context.return_terms).

validation_test() ->
    InitCtx =
        #context{
            bucket = {<<"T">>, <<"B">>},
            field = <<"index_bin">>,
            start_term = <<"aStart">>,
            end_term = <<"zEnd">>
        },
    C1 =
        base64:encode(
            term_to_binary(
                {<<"mTerm">>, <<"K">>}
            )
        ),
    URI1 = test_uri(<<"?timeout=10">>),
    QP1 = extract_params(URI1),
    {ok, Ctx1} = parse_query_params(QP1, InitCtx),
    ?assertMatch(10, Ctx1#context.timeout),
    ?assertMatch(bin, Ctx1#context.field_type),
    URI2 =
        test_uri(
            iolist_to_binary(
                [
                    <<"?max_results=10&continuation=">>,
                    C1,
                    <<"&return_terms">>,
                    <<"&pagination_sort=true">>,
                    <<"&stream=true">>,
                    <<"&term_regex=">>,
                    uri_string:quote(<<".*[A-Z]{1}">>)
                ]
            )
        ),
    {ok, Ctx2} = parse_query_params(extract_params(URI2), InitCtx),
    ?assertMatch(10, Ctx2#context.max_results),
    ?assertMatch(C1, Ctx2#context.continuation),
    ?assertMatch(true, Ctx2#context.return_terms),
    ?assertMatch(true, Ctx2#context.pagination_sort),
    ?assertMatch(true, Ctx2#context.stream),
    ?assertMatch(<<".*[A-Z]{1}">>, Ctx2#context.term_regex),

    % Try and regex an integer query
    ?assertMatch(
        halt,
        element(
            1,
            parse_query_params(
                extract_params(URI2),
                InitCtx#context{field = <<"index_int">>}
            )
        )
    ),
    ?assertMatch(
        halt,
        element(
            1,
            parse_query_params(
                extract_params(URI2),
                InitCtx#context{field = <<"indexbin">>}
            )
        )
    ),
    ?assertMatch(
        halt,
        element(
            1,
            parse_query_params(
                extract_params(URI2),
                InitCtx#context{field = <<"idx">>}
            )
        )
    ),

    URI3 = test_uri(<<"?max_results=A">>),
    ?assertMatch(
        halt,
        element(1, parse_query_params(extract_params(URI3), InitCtx))
    ),
    URI4 = test_uri(<<"?timeout=A">>),
    ?assertMatch(
        halt,
        element(1, parse_query_params(extract_params(URI4), InitCtx))
    ),
    URI5 = test_uri(<<"?continuation=unencodedboundary">>),
    ?assertMatch(
        halt,
        element(1, parse_query_params(extract_params(URI5), InitCtx))
    ),
    URI6 = test_uri(<<"?return_terms=keys">>),
    ?assertMatch(
        halt,
        element(1, parse_query_params(extract_params(URI6), InitCtx))
    ),
    URI7 =
        test_uri(
            iolist_to_binary(
                [
                    <<"?term_regex=">>,
                    uri_string:quote(<<"(*invalid)">>)
                ]
            )
        ),
    ?assertMatch(
        halt,
        element(1, parse_query_params(extract_params(URI7), InitCtx))
    ),

    URI8 = test_uri(<<"?return_terms=false">>),
    {ok, Ctx8} = parse_query_params(extract_params(URI8), InitCtx),
    ?assertMatch(false, Ctx8#context.return_terms).

accept_header_test() ->
    InitCtx =
        #context{
            bucket = {<<"T">>, <<"B">>},
            field = <<"index_bin">>,
            start_term = <<"aStart">>,
            end_term = <<"zEnd">>
        },
    Hdr1 = riak_api_web_headers:make([{'Accept', <<"application/json">>}]),
    ?assertMatch(ok, element(1, parse_request_headers(Hdr1, InitCtx))),
    Hdr2 = riak_api_web_headers:make([{'Accept', <<"application/*">>}]),
    ?assertMatch(ok, element(1, parse_request_headers(Hdr2, InitCtx))),
    Hdr3 =
        riak_api_web_headers:make(
            [
                {'Accept', <<"text/plain, application/*">>}
            ]
        ),
    ?assertMatch(ok, element(1, parse_request_headers(Hdr3, InitCtx))),
    Hdr4 =
        riak_api_web_headers:make(
            [
                {'Accept', <<"text/*, application/octet-stream">>}
            ]
        ),
    ?assertMatch(halt, element(1, parse_request_headers(Hdr4, InitCtx))),
    Hdr5 =
        riak_api_web_headers:make(
            [
                {'Accept', <<"*/*, application/octet-stream">>}
            ]
        ),
    ?assertMatch(ok, element(1, parse_request_headers(Hdr5, InitCtx))),
    Hdr6 =
        riak_api_web_headers:make([
                {'Accept', <<"application/octet-stream">>}
            ]
        ),
    ?assertMatch(halt, element(1, parse_request_headers(Hdr6, InitCtx))),
    Hdr7 =
        riak_api_web_headers:make([
                {'Accept', <<"*/*">>}
            ]
        ),
    ?assertMatch(ok, element(1, parse_request_headers(Hdr7, InitCtx))),
    Hdr8 = riak_api_web_headers:make([]),
    ?assertMatch(ok, element(1, parse_request_headers(Hdr8, InitCtx))).

simple_stream_test() ->
    ReqID = rand:uniform(1000) + 1,
    Boundary = riak_core_util:unique_id_62(),
    GenFun =
        fun(I) ->
            {
                list_to_binary(io_lib:format(<<"T~8..0B">>, [I])),
                list_to_binary(io_lib:format(<<"K~8..0B">>, [I]))
            }
        end,
    Me = self(),
    FSMPid =
        spawn(
            fun() ->
                Me ! {ReqID, {results, lists:map(GenFun, lists:seq(1, 100))}},
                Me ! {ReqID, {results, lists:map(GenFun, lists:seq(101, 200))}},
                Me ! {ReqID, {results, []}},
                Me ! {ReqID, {results, lists:map(GenFun, lists:seq(201, 300))}},
                Me ! {ReqID, done}
            end
        ),
    StreamFun =
        index_stream_fun(
            {ReqID, FSMPid},
            {Boundary, true, 1000},
            {undefined, 0},
            10000
        ),
    {RBin1, StreamFun1} = StreamFun(),
    {RBin2, StreamFun2} = StreamFun1(),
    {RBin3, StreamFun3} = StreamFun2(),
    {RBin4, StreamFun4} = StreamFun3(),
    {RBin5, StreamFun5} = StreamFun4(),
    ?assertMatch(done, StreamFun5()),
    ?assertMatch(<<>>, RBin3),
    Bin = iolist_to_binary([RBin1, RBin2, RBin3, RBin4, RBin5]),
    R = 
        decode_results(
            Bin,
            iolist_to_binary(["\r\n--", Boundary]),
            <<"\r\nContent-Type: application/json\r\n\r\n">>,
            <<"--\r\n">>,
            []
        ),
    ?assertMatch(300, length(R)),
    ok.

    decode_results(Footer, _Boundary, _Header, Footer, Acc) ->
        Acc;
    decode_results(Bin, Boundary, Header, Footer, Acc) ->
        HS = byte_size(Header),
        BS = byte_size(Boundary),
        case Bin of
            << Header:HS/binary, Rest/binary >> ->
                [JsonBin, Remainder] =
                    string:split(Rest, Boundary, leading),
                case JsonBin of
                    <<>> ->
                        decode_results(
                            Remainder,
                            Boundary,
                            Header,
                            Footer,
                            Acc
                        );
                    JsonBin ->
                        #{<<"results">> := RL} =
                            riak_kv_wm_json:decode(JsonBin),
                        decode_results(
                            Remainder,
                            Boundary,
                            Header,
                            Footer,
                            Acc ++ RL
                        )
                end;
            << Boundary:BS/binary, Rest/binary >> ->
                decode_results(Rest, Boundary, Header, Footer, Acc)
        end.

-endif.