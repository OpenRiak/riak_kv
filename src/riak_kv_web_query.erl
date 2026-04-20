%% -------------------------------------------------------------------
%%
%% Copyright (c) 2026 Martin Sumner.
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

-module(riak_kv_web_query).
-include_lib("riak_kv/include/riak_kv_web.hrl").

-if(?OTP_RELEASE == 26).
-feature(maybe_expr, enable).
-endif.

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

-export([get_result_key/1, encode_key/2, encode_key_withterm/2]).

-record(context, {
    client = riak_client:new(node(), self()) :: riak_client:riak_client(),
    bucket :: riak_object:bucket(),
    request_type :: submit_query | fetch_results,
    queue_request :: undefined | #{atom() => term()}
}).

-type context() :: #context{}.

-define(ACCKEY_KEYS, <<"keys">>).
-define(ACCKEY_TERMS, <<"terms">>).
-define(ACCKEY_COUNT, <<"count">>).
-define(ACCKEY_TERMCOUNT, <<"term_with_count">>).
-define(ACCKEY_RAWKEYS, <<"raw_keys">>).
-define(ACCKEY_RAWTERMS, <<"raw_terms">>).
-define(ACCKEY_RAWCOUNT, <<"raw_count">>).
-define(ACCKEY_TERMRAWCOUNT, <<"term_with_rawcount">>).

-define(AGGREGATION_EXPRESSION, <<"aggregation_expression">>).
-define(ACCUMULATION_OPTION, <<"accumulation_option">>).
-define(ACCUMULATION_TERM, <<"accumulation_term">>).
-define(SUBSTITUTIONS, <<"substitutions">>).
-define(TIMEOUT, <<"timeout">>).
-define(INACTIVITY_TIMEOUT, <<"inactivity_timeout">>).
-define(MAX_RESULTS, <<"max_results">>).
-define(CONTINUATION, <<"continuation">>).
-define(QUERY_LIST, <<"query_list">>).
-define(QL_AGGREGATION_TAG, <<"aggregation_tag">>).
-define(QL_INDEX_NAME, <<"index_name">>).
-define(QL_START_TERM, <<"start_term">>).
-define(QL_END_TERM, <<"end_term">>).
-define(QL_REGULAR_EXPRESSION, <<"regular_expression">>).
-define(QL_EVALUATION_EXPRESSION, <<"evaluation_expression">>).
-define(QL_FILTER_EXPRESSION, <<"filter_expression">>).

-define(REQUIRED_KEYS, [?QUERY_LIST]).
-define(POSSIBLE_KEYS, [
    ?AGGREGATION_EXPRESSION,
    ?ACCUMULATION_OPTION,
    ?ACCUMULATION_TERM,
    ?SUBSTITUTIONS,
    ?TIMEOUT,
    ?INACTIVITY_TIMEOUT,
    ?QUERY_LIST,
    ?MAX_RESULTS,
    ?CONTINUATION
]).
-define(REQUIRED_QL_KEYS, [
    ?QL_INDEX_NAME,
    ?QL_START_TERM,
    ?QL_END_TERM
]).
-define(POSSIBLE_QL_KEYS, [
    ?QL_AGGREGATION_TAG,
    ?QL_INDEX_NAME,
    ?QL_START_TERM,
    ?QL_END_TERM,
    ?QL_REGULAR_EXPRESSION,
    ?QL_EVALUATION_EXPRESSION,
    ?QL_FILTER_EXPRESSION
]).

-define(QUERY_TIMEOUT, 60).
-define(QUEUE_INACTIVITY_TIMEOUT, 120).
-define(MAX_RESULTS_FROM_QUEUE, 1000).

-define(HEAD_CONTINUATION, "X-Riak-Continuation").

-type query_map() ::
    #{binary() => binary() | non_neg_integer() | list(map())}.

-type stage() ::
    json_decode
    | key_check
    | query_key_check
    | init_timeout
    | riak_kv_query:validation_stage().

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
match_route(Method, _, [<<"types">>, T, <<"buckets">>, B, <<"query">>]) ->
    case Method of
        'GET' ->
            Context =
                #context{
                    bucket = riak_kv_web_common:set_bucket(T, B),
                    request_type = fetch_results
                },
            {ok, {32, 2048, 0}, Context};
        'POST' ->
            Context =
                #context{
                    bucket = riak_kv_web_common:set_bucket(T, B),
                    request_type = submit_query
                },
            {ok, {32, 2048, 64 * 1024}, Context};
        _Other ->
            {method_not_allowed, ['GET', 'POST']}
    end;
match_route(Method, Path, [<<"buckets">>, B, <<"query">>]) ->
    match_route(
        Method,
        Path,
        [<<"types">>, <<"default">>, <<"buckets">>, B, <<"query">>]
    );
match_route(_Method, _Path, _SplitPath) ->
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
            % The PB API doesn't check type exists, however, the FSM will crash
            % if it does not exist - so better to give a sensible error here.
            % Note this requires the fetching (and discarding) of the type
            % properties.
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
parse_query_params(_Params, #context{request_type = submit_query} = Ctx) ->
    {ok, Ctx};
parse_query_params(Params, #context{request_type = fetch_results} = Ctx) ->
    case lists:keyfind(<<"result_queue">>, 1, Params) of
        {<<"result_queue">>, Queue} when is_binary(Queue) ->
            MaxResults =
                case lists:keyfind(<<"max_results">>, 1, Params) of
                    {<<"max_results">>, MR} ->
                        MR;
                    _ ->
                        application:get_env(
                            riak_kv,
                            queue_raw_max_results,
                            ?MAX_RESULTS_FROM_QUEUE
                        )
                end,
            try
                MRI =
                    case is_integer(MaxResults) of
                        true ->
                            MaxResults;
                        false ->
                            binary_to_integer(MaxResults)
                    end,
                true = is_integer(MRI),
                true = MRI >= 0,
                {
                    ok,
                    Ctx#context{
                        queue_request =
                            make_queue_request(
                                Ctx#context.bucket,
                                Queue,
                                MRI
                            )
                    }
                }
            catch
                _:_ ->
                    json_validation_error(
                        <<"Invalid max_results query parameter">>
                    )
            end;
        _ ->
            json_validation_error(
                <<"No valid result_queue reference passed as query parameter">>
            )
    end.

%% @doc parse and validate the request headers
-spec parse_request_headers(
    riak_api_web_headers:headers(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
parse_request_headers(ReqHeaders, Ctx) ->
    AcceptedTypes =
        case riak_api_web_headers:get_value('Accept', ReqHeaders) of
            undefined ->
                [<<"application/json">>];
            SingleType when is_binary(SingleType) ->
                [SingleType];
            MultipleTypes when is_list(MultipleTypes) ->
                MultipleTypes
        end,
    case riak_kv_web_common:type_match(<<"application/json">>, AcceptedTypes) of
        true ->
            {ok, Ctx};
        false ->
            {
                halt,
                406,
                [?TXT_HEADER],
                <<"application/json must be accepted">>,
                []
            }
    end.

%% @doc Process the request and produce a response
-spec process_request(
    riak_api_web_body:req_body() | none,
    context()
) ->
    {
        ok,
        {
            riak_api_web_acceptor:response_code(),
            riak_api_web_headers:header_list(),
            riak_api_web_handler:response_body(),
            boolean(),
            riak_api_web_body:req_body() | none
        },
        context()
    }
    | riak_api_web_acceptor:halt_response().
process_request(none, #context{request_type = fetch_results} = Ctx) ->
    QRR =
        riak_client:query_result_request(
            Ctx#context.queue_request,
            Ctx#context.client
        ),
    case QRR of
        {ok, ResultMap} ->
            ResultBin = encode_queued_results(ResultMap),
            {
                ok,
                {
                    200,
                    [?JSN_HEADER],
                    ResultBin,
                    true,
                    none
                },
                Ctx
            };
        {error, result_server_terminated} ->
            {
                halt,
                410,
                % Response code for Gone, and likely to be permanent.
                % This may be as a result of an error on the server, but
                % is probably as a result of an error on the client - and
                % so to help with load-balancers tracking server errors,
                % err on the side of blaming the client
                [?JSN_HEADER],
                iolist_to_binary(
                    riak_kv_wm_json:encode(
                        #{
                            error =>
                                <<
                                    "queue no longer present or"
                                    " not currently reachable"
                                >>
                        }
                    )
                ),
                []
            };
        {error, unexpected_reference_format} ->
            json_validation_error(
                <<"queue reference passed had an invalid format">>
            );
        {error, Reason} ->
            {
                halt,
                500,
                [?JSN_HEADER],
                iolist_to_binary(
                    riak_kv_wm_json:encode(
                        #{error => <<"~0p">>}
                    )
                ),
                [Reason]
            }
    end;
process_request(RqBdy, #context{request_type = submit_query} = Ctx) ->
    case riak_api_web_body:get_body(RqBdy, all, 60000) of
        {error, content_too_large} ->
            {halt, 413, [], <<>>, []};
        {ObjBody, UpdRqBody} when is_binary(ObjBody) ->
            case decode_json_body(ObjBody) of
                {ok, QueryMap} ->
                    case validate_query(QueryMap, Ctx#context.bucket) of
                        {ok, Query} ->
                            case process_query(Query, Ctx) of
                                {ok, RspHdrs, RspBdy} ->
                                    {
                                        ok,
                                        {
                                            200,
                                            RspHdrs,
                                            RspBdy,
                                            true,
                                            UpdRqBody
                                        },
                                        Ctx
                                    };
                                HaltResponse ->
                                    HaltResponse
                            end;
                        {error, Stage, Reason} ->
                            {
                                halt,
                                400,
                                [?JSN_HEADER],
                                expand_query_reason(Stage, Reason),
                                []
                            }
                    end;
                {error, Reason} when is_binary(Reason) ->
                    {
                        halt,
                        400,
                        [?JSN_HEADER],
                        expand_query_reason(json_decode, Reason),
                        []
                    }
            end
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
%% External Helper Functions
%% ===================================================================

-spec get_result_key(riak_kv_query:accumulation_option()) -> binary().
get_result_key(keys) -> ?ACCKEY_KEYS;
get_result_key(raw_keys) -> ?ACCKEY_RAWKEYS;
get_result_key(terms) -> ?ACCKEY_TERMS;
get_result_key(raw_terms) -> ?ACCKEY_RAWTERMS;
get_result_key(count) -> ?ACCKEY_COUNT;
get_result_key(raw_count) -> ?ACCKEY_RAWCOUNT;
get_result_key(term_with_count) -> ?ACCKEY_TERMCOUNT;
get_result_key(term_with_rawcount) -> ?ACCKEY_TERMRAWCOUNT.

%% ===================================================================
%% Internal Functions
%% ===================================================================

-spec validate_query(
    query_map(),
    riak_object:bucket()
) ->
    {ok, riak_kv_query:complex_query_definition()}
    | {error, stage(), binary()}.
validate_query(QueryMap, Bucket) ->
    case check_keys(maps:keys(QueryMap), request) of
        ok ->
            QueryList = maps:get(?QUERY_LIST, QueryMap),
            case check_querylist(QueryList, false) of
                ok ->
                    make_query_request(Bucket, QueryMap);
                {error, Reason} ->
                    {error, query_key_check, Reason}
            end;
        {error, Reason} ->
            {error, key_check, Reason}
    end.

-spec check_querylist(list(binary()), boolean()) -> ok | {error, binary()}.
check_querylist([], true) ->
    ok;
check_querylist([], false) ->
    {error, <<"No valid query provided">>};
check_querylist([HdQuery | Rest], _AtLeastOne) ->
    case check_keys(maps:keys(HdQuery), query) of
        ok ->
            check_querylist(Rest, true);
        Error ->
            Error
    end.

-spec check_keys(
    list(binary()),
    request | query
) ->
    ok | {error, binary()}.
check_keys(Keys, request) ->
    check_keys(Keys, ?REQUIRED_KEYS, ?POSSIBLE_KEYS);
check_keys(Keys, query) ->
    check_keys(Keys, ?REQUIRED_QL_KEYS, ?POSSIBLE_QL_KEYS).

-spec check_keys(
    list(binary()),
    list(binary()),
    list(binary())
) ->
    ok | {error, binary()}.
check_keys(Keys, RequiredKeys, PossibleKeys) ->
    RequiredKeyList =
        lists:filter(
            fun(K) -> lists:member(K, Keys) end,
            RequiredKeys
        ),
    PossibleKeyList =
        lists:filter(
            fun(K) -> lists:member(K, PossibleKeys) end,
            Keys
        ),
    case RequiredKeyList of
        RequiredKeys ->
            case PossibleKeyList of
                Keys ->
                    ok;
                NotAllKeys ->
                    ExtraKeys = lists:subtract(Keys, NotAllKeys),
                    {
                        error,
                        iolist_to_binary(
                            io_lib:format(
                                <<"Unexpected keys in request ~0p">>,
                                [ExtraKeys]
                            )
                        )
                    }
            end;
        NotAllRequiredKeys ->
            MissingKeys = lists:subtract(RequiredKeys, NotAllRequiredKeys),
            {
                error,
                iolist_to_binary(
                    io_lib:format(
                        <<"Missing required keys in request ~0p">>,
                        [MissingKeys]
                    )
                )
            }
    end.

-spec make_query_request(
    riak_object:bucket(), query_map()
) ->
    {ok, riak_kv_query:complex_query_definition()}
    | riak_kv_query:validation_error().
make_query_request(BucketType, QueryMap) ->
    maybe
        {ok, Timeout, InactivityTimeout} ?= fetch_timeouts(QueryMap),
        QueryType =
            case maps:get(?QUERY_LIST, QueryMap) of
                QueryList when length(QueryList) == 1 ->
                    single_query;
                QueryList when length(QueryList) > 1 ->
                    combo_query
            end,
        InitQuery =
            riak_kv_query:new(
                BucketType,
                QueryType,
                Timeout,
                InactivityTimeout
            ),
        {ok, Q1} ?= add_accumulation(QueryMap, InitQuery),
        {ok, Q2} ?= add_queries(QueryMap, Q1, QueryList),
        case maps:get(?CONTINUATION, QueryMap, none) of
            none ->
                {ok, Q2};
            Continuation ->
                riak_kv_query:add_continuation(Q2, Continuation)
        end
    else
        {error, Stage, Reason} ->
            {error, Stage, Reason}
    end.

-spec decode_json_body(binary()) -> {ok, map()} | {error, binary()}.
decode_json_body(JsonBody) ->
    try
        DecodedBody = riak_kv_wm_json:decode(JsonBody),
        {ok, DecodedBody}
    catch
        error:Reason ->
            ExpandedReason =
                iolist_to_binary(
                    io_lib:format(
                        <<"Malformed json request - ~0p">>,
                        [Reason]
                    )
                ),
            {error, ExpandedReason}
    end.

json_validation_error(Text) when is_binary(Text) ->
    {
        halt,
        400,
        [?JSN_HEADER],
        iolist_to_binary(riak_kv_wm_json:encode(#{error => Text})),
        []
    }.

-spec make_queue_request(
    riak_object:bucket(), binary(), non_neg_integer()
) ->
    #{atom() => term()}.
make_queue_request(Bucket, EncodedQueueRef, MaxResults) ->
    #{
        bucket => Bucket,
        encoded_queue_reference => EncodedQueueRef,
        max_results => MaxResults
    }.

-spec encode_queued_results(
    riak_kv_query_server:partial_result_map()
) -> binary().
encode_queued_results(ResultMap) ->
    case maps:is_key(get_result_key(raw_keys), ResultMap) of
        true ->
            iolist_to_binary(
                riak_kv_wm_json:encode(
                    ResultMap,
                    fun riak_kv_web_query:encode_key/2
                )
            );
        false ->
            case maps:is_key(get_result_key(raw_terms), ResultMap) of
                true ->
                    iolist_to_binary(
                        riak_kv_wm_json:encode(
                            ResultMap,
                            fun riak_kv_web_query:encode_key_withterm/2
                        )
                    )
            end
    end.

-spec add_accumulation(
    query_map(),
    riak_kv_query:complex_query_definition()
) ->
    {ok, riak_kv_query:complex_query_definition()}
    | riak_kv_query:validation_error().
add_accumulation(QueryMap, InitQuery) ->
    AccOpt = maps:get(?ACCUMULATION_OPTION, QueryMap, undefined),
    AccTerm = maps:get(?ACCUMULATION_TERM, QueryMap, undefined),
    MaxResults = maps:get(?MAX_RESULTS, QueryMap, undefined),
    case riak_kv_query:add_accumulation_option(InitQuery, AccOpt) of
        {ok, UpdQuery0} ->
            case riak_kv_query:add_accumulation_term(UpdQuery0, AccTerm) of
                {ok, UpdQuery1} ->
                    case MaxResults of
                        undefined ->
                            {ok, UpdQuery1};
                        MR ->
                            riak_kv_query:add_maxresults(UpdQuery1, MR)
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec add_queries(
    query_map(),
    riak_kv_query:complex_query_definition(),
    list(#{binary() => binary()})
) ->
    {ok, riak_kv_query:complex_query_definition()}
    | riak_kv_query:validation_error().
add_queries(QueryMap, Query, QueryList) ->
    AggExpr =
        maps:get(?AGGREGATION_EXPRESSION, QueryMap, undefined),
    case riak_kv_query:add_aggregation_expression(Query, AggExpr) of
        {ok, Q2} ->
            Subs =
                maps:get(?SUBSTITUTIONS, QueryMap, maps:new()),
            riak_kv_query:add_queries(
                Q2,
                lists:map(fun convert_query/1, QueryList),
                Subs
            );
        Error ->
            Error
    end.

-spec expand_query_reason(stage(), binary()) -> binary().
expand_query_reason(Stage, Reason) ->
    iolist_to_binary(
        io_lib:format(
            <<"Validation failure at stage ~w due to ~s">>,
            [Stage, Reason]
        )
    ).

-spec convert_query(map()) -> riak_kv_query:query_user_input().
convert_query(QM) ->
    {
        maps:get(<<"aggregation_tag">>, QM, undefined),
        maps:get(<<"index_name">>, QM),
        maps:get(<<"start_term">>, QM),
        maps:get(<<"end_term">>, QM),
        maps:get(<<"regular_expression">>, QM, undefined),
        maps:get(<<"evaluation_expression">>, QM, undefined),
        maps:get(<<"filter_expression">>, QM, undefined)
    }.

-spec fetch_timeouts(
    query_map()
) ->
    {ok, pos_integer(), pos_integer()} | {error, init_timeout, binary()}.
fetch_timeouts(QueryMap) ->
    Timeout =
        maps:get(
            ?TIMEOUT,
            QueryMap,
            application:get_env(riak_kv, query_timeout_secs, ?QUERY_TIMEOUT)
        ),
    InactivityTimeout =
        maps:get(
            ?INACTIVITY_TIMEOUT,
            QueryMap,
            application:get_env(
                riak_kv,
                queue_inactivity_timeout_secs,
                ?QUEUE_INACTIVITY_TIMEOUT
            )
        ),
    case Timeout of
        T when is_integer(T), T > 0 ->
            case InactivityTimeout of
                IT when is_integer(IT), IT > 0 ->
                    {ok, T, IT};
                _ ->
                    {error, init_timeout, <<"Bad inactivity timeout">>}
            end;
        _ ->
            {error, init_timeout, <<"Bad timeout">>}
    end.

%% ===================================================================
%% Internal Functions
%% ===================================================================

-spec process_query(
    riak_kv_query:complex_query_definition(),
    context()
) ->
    {ok, riak_api_web_headers:header_list(), binary()}
    | riak_api_web_acceptor:halt_response().
process_query(InitQuery, Ctx) ->
    Client = Ctx#context.client,
    AccOpt = riak_kv_query:get_accumulator(InitQuery),
    {ok, Query} =
        riak_kv_query:add_result_encodingfun(
            InitQuery,
            encoding_function(AccOpt)
        ),
    case riak_client:query(Query, Client) of
        {error, timeout} ->
            {halt, 503, [?JSN_HEADER], <<"timeout">>, []};
        {error, Reason} ->
            Error = <<"Query with option ~w failed - ~0p">>,
            {halt, 500, [?JSN_HEADER], Error, [AccOpt, Reason]};
        {result_queue, ResultReference} when is_binary(ResultReference) ->
            {
                ok,
                [?JSN_HEADER],
                iolist_to_binary(
                    riak_kv_wm_json:encode(
                        #{result_queue => ResultReference}
                    )
                )
            };
        {JsonEncodedResults, none} when is_binary(JsonEncodedResults) ->
            {ok, JsonEncodedResults};
        {JsonEncodedResults, {{LT, LK}}} when
            is_binary(JsonEncodedResults),
            is_binary(LT),
            is_binary(LK)
        ->
            Continuation = riak_kv_query:make_continuation(LT, LK),
            {
                true,
                [
                    ?JSN_HEADER,
                    {?HEAD_CONTINUATION, Continuation}
                ],
                JsonEncodedResults
            }
    end.

-spec encoding_function(riak_kv_query:accumulation_option()) ->
    fun((riak_kv_query_server:results()) -> binary()).
encoding_function(AccOpt) ->
    fun(Results) -> encode_results(AccOpt, Results) end.

-spec encode_results(
    riak_kv_query:accumulation_option(), riak_kv_query_server:results()
) -> binary().
encode_results(AccOpt, Results) when AccOpt == keys; AccOpt == raw_keys ->
    iolist_to_binary(
        riak_kv_wm_json:encode(
            #{get_result_key(AccOpt) => Results},
            fun riak_kv_web_query:encode_key/2
        )
    );
encode_results(AccOpt, Results) when AccOpt == terms; AccOpt == raw_terms ->
    iolist_to_binary(
        riak_kv_wm_json:encode(
            #{get_result_key(AccOpt) => Results},
            fun riak_kv_web_query:encode_key_withterm/2
        )
    );
encode_results(AccOpt, Count) when AccOpt == count; AccOpt == raw_count ->
    iolist_to_binary(
        riak_kv_wm_json:encode(#{get_result_key(AccOpt) => Count})
    );
encode_results(AccOpt, CountMap) when
    AccOpt == term_with_count; AccOpt == term_with_rawcount
->
    iolist_to_binary(
        riak_kv_wm_json:encode(#{get_result_key(AccOpt) => CountMap})
    ).

encode_key({{_Term, Key}}, Encode) when is_binary(Key) ->
    encode_key(Key, Encode);
encode_key({Key}, Encode) when is_binary(Key) ->
    encode_key(Key, Encode);
encode_key(Key, Encode) ->
    riak_kv_wm_json:encode_value(Key, Encode).

encode_key_withterm({TermKeyTuple}, Encode) when is_tuple(TermKeyTuple) ->
    encode_key_withterm(TermKeyTuple, Encode);
encode_key_withterm({Term, Key}, Encode) when is_binary(Term), is_binary(Key) ->
    [123, [Encode(Term, Encode), $: | Encode(Key, Encode)], 125];
encode_key_withterm(Result, Encode) ->
    riak_kv_wm_json:encode_value(Result, Encode).

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").

invalid_json_test() ->
    InvalidJson =
        <<% Missing comma after example_bin
        "\n"
        "            {\n"
        "                \"accumulation_option\" : \"keys\",\n"
        "                \"timeout\" : 60,\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"index_name\" : \"example_bin\"\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        }\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    R = decode_json_body(InvalidJson),
    io:format("~p~n", [R]),
    ?assertMatch(
        {error, <<"Malformed json request - {invalid_byte,34}">>},
        R
    ).

simple_query_test() ->
    SimpleQueryJson =
        <<"\n"
        "            {\n"
        "                \"timeout\" : 60,\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        }\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(SimpleQueryJson),
    {ok, Q} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assert(riak_kv_query:is_query(Q)).

invalid_query_ae1_test() ->
    IQJson =
        <<"\n"
        "            {\n"
        "                \"aggregation_expression\" : \"$1 INTERSECT $2\",\n"
        "                \"timeout\" : 60,\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        }\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    {error, S, _E} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assertMatch(aggregation_expression, S).

invalid_query_ae2_test() ->
    IQJson =
        <<"\n"
        "            {\n"
        "                \"timeout\" : 60,\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 1,\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        },\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 2,\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        }\n"
        "\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    {error, S, _E} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assertMatch(aggregation_expression, S).

invalid_query_ae3_test() ->
    IQJson =
        <<"\n"
        "            {\n"
        "                \"aggregation_expression\" : \"$1 INTERSECT $2\",\n"
        "                \"timeout\" : 60,\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        },\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 2,\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        }\n"
        "\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    {error, S, E} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assertMatch(query_evaluation, S),
    ?assertMatch(<<"Untagged query in combination request">>, E).

valid_query_ae4_test() ->
    IQJson =
        <<"\n"
        "            {\n"
        "                \"aggregation_expression\" : \"$1 INTERSECT $2\",\n"
        "                \"timeout\" : 60,\n"
        "                \"inactivity_timeout\" : 180,\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 1,\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        },\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 2,\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        }\n"
        "\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    {ok, Q} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assert(riak_kv_query:is_query(Q)),
    QueryList = maps:get(<<"query_list">>, M),
    ?assertMatch(ok, check_querylist(QueryList, false)).

valid_query_ae5_test() ->
    IQJson =
        <<"\n"
        "            {\n"
        "                \"aggregation_expression\" : \"$1 INTERSECT $2\",\n"
        "                \"timeout\" : 60,\n"
        "                \"accumulation_option\" : \"keys\",\n"
        "                \"substitutions\" :\n"
        "                    {\"low_dob\" : \"20210804\", \"high_dob\" : \"20223101\", \"gnsc\" : \"Ma\"},\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 1,\n"
        "                            \"index_name\" : \"example1_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\",\n"
        "                            \"evaluation_expression\" :\n"
        "                                \"delim($term, \\\"|\\\", ($fn, $dob, $dod, $gns, $pcs)) | slice($gns, 2, $gns)\",\n"
        "                            \"filter_expression\" : \"($dob BETWEEN :low_dob AND :high_dob\) AND contains($gns, :gnsc)\"\n"
        "                        },\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 2,\n"
        "                            \"index_name\" : \"example2_bin\",\n"
        "                            \"start_term\" : \"C\",\n"
        "                            \"end_term\"   : \"D\"\n"
        "                        }\n"
        "\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    {ok, Q} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assert(riak_kv_query:is_query(Q)),
    QueryList = maps:get(<<"query_list">>, M),
    ?assertMatch(ok, check_querylist(QueryList, false)).

invalid_query_ae6_test() ->
    % unescaped "|" in eval expression
    IQJson =
        <<"\n"
        "            {\n"
        "                \"aggregation_expression\" : \"$1 INTERSECT $2\",\n"
        "                \"timeout\" : 60,\n"
        "                \"accumulation_option\" : \"keys\",\n"
        "                \"substitutions\" :\n"
        "                    {\"low_dob\" : \"20210804\", \"high_dob\" : \"20223101\", \"gnsc\" : \"Ma\"},\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 1,\n"
        "                            \"index_name\" : \"example1_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\",\n"
        "                            \"evaluation_expression\" :\n"
        "                                \"delim($term, |, ($fn, $dob, $dod, $gns, $pcs)) | slice($gns, 2, $gns)\",\n"
        "                            \"filter_expression\" : \"($dob BETWEEN :low_dob AND :high_dob\) AND contains($gns, :gnsc)\"\n"
        "                        },\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 2,\n"
        "                            \"index_name\" : \"example2_bin\",\n"
        "                            \"start_term\" : \"C\",\n"
        "                            \"end_term\"   : \"D\"\n"
        "                        }\n"
        "\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, query_evaluation, <<"Invalid eval function">>},
        make_query_request({<<"BT">>, <<"B">>}, M)
    ).

invalid_query_ae7_test() ->
    % BETWEN not BETWEEN
    IQJson =
        <<"\n"
        "            {\n"
        "                \"aggregation_expression\" : \"$1 INTERSECT $2\",\n"
        "                \"timeout\" : 60,\n"
        "                \"accumulation_option\" : \"keys\",\n"
        "                \"substitutions\" :\n"
        "                    {\"low_dob\" : \"20210804\", \"high_dob\" : \"20223101\", \"gnsc\" : \"Ma\"},\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 1,\n"
        "                            \"index_name\" : \"example1_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\",\n"
        "                            \"evaluation_expression\" :\n"
        "                                \"delim($term, \\\"|\\\", ($fn, $dob, $dod, $gns, $pcs)) | slice($gns, 2, $gns)\",\n"
        "                            \"filter_expression\" : \"($dob BETWEN :low_dob AND :high_dob\) AND contains($gns, :gnsc)\"\n"
        "                        },\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 2,\n"
        "                            \"index_name\" : \"example2_bin\",\n"
        "                            \"start_term\" : \"C\",\n"
        "                            \"end_term\"   : \"D\"\n"
        "                        }\n"
        "\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, query_evaluation, <<"Invalid filter function">>},
        make_query_request({<<"BT">>, <<"B">>}, M)
    ).

invalid_query_ae8_test() ->
    % missing substitution
    IQJson =
        <<"\n"
        "            {\n"
        "                \"aggregation_expression\" : \"$1 INTERSECT $2\",\n"
        "                \"timeout\" : 60,\n"
        "                \"accumulation_option\" : \"keys\",\n"
        "                \"substitutions\" :\n"
        "                    {\"low_dob\" : \"20210804\", \"gnsc\" : \"Ma\"},\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 1,\n"
        "                            \"index_name\" : \"example1_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\",\n"
        "                            \"evaluation_expression\" :\n"
        "                                \"delim($term, \\\"|\\\", ($fn, $dob, $dod, $gns, $pcs)) | slice($gns, 2, $gns)\",\n"
        "                            \"filter_expression\" : \"($dob BETWEEN :low_dob AND :high_dob\) AND contains($gns, :gnsc)\"\n"
        "                        },\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 2,\n"
        "                            \"index_name\" : \"example2_bin\",\n"
        "                            \"start_term\" : \"C\",\n"
        "                            \"end_term\"   : \"D\"\n"
        "                        }\n"
        "\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, query_evaluation, <<"Invalid filter function">>},
        make_query_request({<<"BT">>, <<"B">>}, M)
    ).

invalid_query_to_test() ->
    IQJson =
        <<"\n"
        "            {\n"
        "                \"aggregation_expression\" : \"$1 INTERSECT $2\",\n"
        "                \"timeout\" : 0,\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 1,\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        },\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 2,\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        }\n"
        "\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    {error, S, E} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assertMatch(init_timeout, S),
    ?assertMatch(<<"Bad timeout">>, E).

invalid_query_extratag_test() ->
    IQJson =
        <<"\n"
        "            {\n"
        "                \"aggregation_expression\" : \"$1 INTERSECT $2\",\n"
        "                \"timeout\" : 60,\n"
        "                \"subs\" : {\"dob\" : \"19260812\"},\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 1,\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        },\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 2,\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\",\n"
        "                            \"end_key\"   : \"B\"\n"
        "                        }\n"
        "\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, <<"Unexpected keys in request [<<\"subs\">>]">>},
        check_keys(maps:keys(M), request)
    ),
    ?assertMatch(
        {error, <<"Unexpected keys in request [<<\"end_key\">>]">>},
        check_querylist(maps:get(<<"query_list">>, M), false)
    ).

invalid_query_missingtag1_test() ->
    IQJson =
        <<"\n"
        "            {\n"
        "                \"aggregation_expression\" : \"$1 INTERSECT $2\",\n"
        "                \"timeout\" : 60,\n"
        "                \"subs\" : {\"dob\" : \"19260812\"}\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, <<"Missing required keys in request [<<\"query_list\">>]">>},
        check_keys(maps:keys(M), request)
    ).

invalid_query_missingtag2_test() ->
    IQJson =
        <<"\n"
        "            {\n"
        "                \"aggregation_expression\" : \"$1 INTERSECT $2\",\n"
        "                \"timeout\" : 60,\n"
        "                \"query_list\" :\n"
        "                    [\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 1,\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"start_term\" : \"A\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        },\n"
        "                        {\n"
        "                            \"aggregation_tag\" : 2,\n"
        "                            \"index_name\" : \"example_bin\",\n"
        "                            \"end_term\"   : \"B\"\n"
        "                        }\n"
        "\n"
        "                    ]\n"
        "            }\n"
        "        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, <<"Missing required keys in request [<<\"start_term\">>]">>},
        check_querylist(maps:get(<<"query_list">>, M), false)
    ).

encode_results_test() ->
    BinMC = encode_results(raw_count, 500),
    ?assertMatch(
        500,
        maps:get(?ACCKEY_RAWCOUNT, riak_kv_wm_json:decode(BinMC))
    ),
    BinKC = encode_results(count, 600),
    ?assertMatch(
        600,
        maps:get(?ACCKEY_COUNT, riak_kv_wm_json:decode(BinKC))
    ),
    KeyList = [<<"K00001">>, <<"K00002">>, <<"K0003">>],
    BinKL = encode_results(keys, KeyList),
    ?assertMatch(
        KeyList,
        maps:get(?ACCKEY_KEYS, riak_kv_wm_json:decode(BinKL))
    ),
    KeyListT = [{<<"K00001">>}, {<<"K00002">>}, {<<"K0003">>}],
    BinKLT = encode_results(keys, KeyListT),
    ?assertMatch(
        KeyList,
        maps:get(?ACCKEY_KEYS, riak_kv_wm_json:decode(BinKLT))
    ),
    TermKeyList = [{<<"T0001">>, <<"K0002">>}, {<<"T0002">>, <<"K0001">>}],
    BinTKL = encode_results(terms, TermKeyList),
    ?assertMatch(
        TermKeyList,
        lists:sort(
            lists:map(
                fun(M) ->
                    [{T, K}] = maps:to_list(M),
                    {T, K}
                end,
                maps:get(?ACCKEY_TERMS, riak_kv_wm_json:decode(BinTKL))
            )
        )
    ),
    TermKeyListT =
        [{{<<"T0001">>, <<"K0002">>}}, {{<<"T0002">>, <<"K0001">>}}],
    BinTKLT = encode_results(terms, TermKeyListT),
    ?assertMatch(
        TermKeyList,
        lists:sort(
            lists:map(
                fun(M) ->
                    [{T, K}] = maps:to_list(M),
                    {T, K}
                end,
                maps:get(?ACCKEY_TERMS, riak_kv_wm_json:decode(BinTKLT))
            )
        )
    ),
    TermCount = #{<<"T0001">> => 12, <<"T0002">> => 10},
    BinTKC = encode_results(term_with_count, TermCount),
    ?assertMatch(
        10,
        maps:get(
            <<"T0002">>,
            maps:get(?ACCKEY_TERMCOUNT, riak_kv_wm_json:decode(BinTKC))
        )
    ),
    BinTMC = encode_results(term_with_rawcount, TermCount),
    ?assertMatch(
        12,
        maps:get(
            <<"T0001">>,
            maps:get(?ACCKEY_TERMRAWCOUNT, riak_kv_wm_json:decode(BinTMC))
        )
    ).

validate_parameter_test_() ->
    {
        foreach,
        setup(),
        cleanup(),
        [
            fun validate_parameters/0,
            fun validate_headers/0
        ]
    }.

validate_parameters() ->
    InitCtx =
        #context{bucket = {<<"T">>, <<"B">>}, request_type = fetch_results},
    DummyQRef = base64:encode(term_to_binary({node(), self(), make_ref()})),
    {ok, Ctx1} =
        parse_query_params(
            [{<<"max_results">>, <<"1000">>}, {<<"result_queue">>, DummyQRef}],
            InitCtx
        ),
    ?assert(is_map(Ctx1#context.queue_request)),
    ?assertMatch(1000, maps:get(max_results, Ctx1#context.queue_request)),

    {ok, Ctx2} =
        parse_query_params(
            [{<<"result_queue">>, DummyQRef}],
            InitCtx
        ),
    ?assert(is_map(Ctx2#context.queue_request)),
    ?assertMatch(
        ?MAX_RESULTS_FROM_QUEUE,
        maps:get(max_results, Ctx2#context.queue_request)
    ),

    {halt, 400, [?JSN_HEADER], Error3, []} =
        parse_query_params(
            [{<<"max_results">>, <<"-1">>}, {<<"result_queue">>, DummyQRef}],
            InitCtx
        ),
    ?assert(is_binary(Error3)),
    {halt, 400, [?JSN_HEADER], Error4, []} =
        parse_query_params(
            [{<<"max_results">>, <<"A">>}, {<<"result_queue">>, DummyQRef}],
            InitCtx
        ),
    ?assert(is_binary(Error4)),
    {halt, 400, [?JSN_HEADER], Error5, []} =
        parse_query_params(
            [{<<"max_results">>, <<"100">>}],
            InitCtx
        ),
    ?assert(is_binary(Error5)),
    InitCtxQ =
        #context{bucket = {<<"T">>, <<"B">>}, request_type = submit_query},
    {ok, _Ctx3} = parse_query_params([], InitCtxQ).

validate_headers() ->
    InitCtx1 =
        #context{bucket = {<<"T">>, <<"B">>}, request_type = fetch_results},
    InitCtx2 =
        #context{bucket = {<<"T">>, <<"B">>}, request_type = submit_query},
    Header1 = riak_api_web_headers:make([{'Accept', <<"application/json">>}]),
    Header2 =
        riak_api_web_headers:make(
            [{'Accept', [<<"application/octet-stream">>, <<"*/*;q=0.9">>]}]
        ),
    Header3 = riak_api_web_headers:make([]),
    lists:foreach(
        fun({Ctx, Hdrs}) ->
            ?assertMatch(
                {ok, _},
                parse_request_headers(Hdrs, Ctx)
            )
        end,
        [
            {InitCtx1, Header1},
            {InitCtx1, Header2},
            {InitCtx1, Header3},
            {InitCtx2, Header1}
        ]
    ),
    Header4 = riak_api_web_headers:make([{'Accept', <<"text/xml">>}]),
    {halt, 406, [TXT_HEADER], Error, []} =
        parse_request_headers(Header4, InitCtx1),
    {halt, 406, [TXT_HEADER], Error, []} =
        parse_request_headers(Header4, InitCtx2),
    ?assert(is_binary(Error)).

setup() ->
    riak_kv_test_util:common_setup(?MODULE, fun configure/1).

cleanup() ->
    riak_kv_test_util:common_cleanup(?MODULE, fun configure/1).

configure(load) ->
    application:set_env(riak_core, default_bucket_props, []),
    application:set_env(riak_kv, storage_backend, riak_kv_memory_backend);
configure(_) ->
    ok.

-endif.
