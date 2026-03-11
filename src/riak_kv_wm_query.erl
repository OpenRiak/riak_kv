%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
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

%% @doc Webmachine resource for running queries on secondary indexes.
%%
%% Available operations:
%%
%% ```
%% POST types/<BucketType>/buckets/<Bucket>/query
%% POST buckets/<Bucket>/query (legacy support for untyped buckets)
%% ```
%%
%% The query should be posted as the HTTP body, where there are the following
%% JSON keys at the root of the document
%%
%% ?AGGREGATION_EXPRESSION
%% ?ACCUMULATION_OPTION
%% ?ACCUMULATION_TERM
%% ?SUBSTITUTIONS
%% ?TIMEOUT
%% ?MAX_RESULTS
%% ?CONTINUATION
%% ?QUERY_LIST
%% 
%% Each query in the query list must be a JSON document supporting the
%% following keys:
%% 
%% ?QL_AGGREGATION_TAG
%% ?QL_INDEX_NAME
%% ?QL_START_TERM
%% ?QL_END_TERM
%% ?QL_REGULAR_EXPRESSION
%% ?QL_EVALUATION_EXPRESSION
%% ?QL_FILTER_EXPRESSION
%% 
%% For details on usage see https://openriak.github.io/riak_kv/QueryAPI.html.
%% 
%% Queries POST'd will return a JSON object with the result format determined
%% by the ?ACCUMULATION_OPTION passed in the query
%% 
%% ```
%% GET types/<BucketType>/bucket/<Bucket>/query
%% GET buckets/<Bucket>/query (legacy support for untyped buckets)
%% ```
%% 
%% The GET operation is used to extract results queued using a query with the
%% ?ACCUMULATION_OPTION of `queue_raw_keys` or `queue_raw_terms`
%% 
%% Requests support two query parameters:
%% 
%% ?result_queue=<EncodedQueueReference>%max_results=<NonNegInteger>
%% 
%% The result_queue is a mandatory parameter, and is returned as the
%% ?RSP_RESULT_QUEUE key in the JSON object received from POSTing a query with
%% a queue-based ?ACCUMULATION_OPTION.
%% 
%% Multiple client-side processes may request results from the queue
%% concurrently, from any connected node within the cluster.  Each result will
%% be returned once only.

-module(riak_kv_wm_query).

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

%% webmachine resource exports
-export([
    init/1,
    service_available/2,
    is_authorized/2,
    forbidden/2,
    allowed_methods/2,
    malformed_request/2,
    resource_exists/2,
    process_post/2,
    encode_key/2,
    encode_key_withterm/2,
    content_types_provided/2,
    return_queued_results/2
]).

-export(
    [
        get_result_key/1
    ]
).

-record(ctx,
    {
        client,       %% riak_client() - the store client
        riak,         %% local | {node(), atom()} - params for riak client
        bucket_type,  %% Bucket type (from uri)
        query_request,        %% The query..
        queue_request,
        security,
        method
    }
).

-type context() :: #ctx{}.
-type request_data() :: #wm_reqdata{}.

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

-define(ACCKEY_KEYS, <<"keys">>).
-define(ACCKEY_TERMS, <<"terms">>).
-define(ACCKEY_COUNT, <<"count">>).
-define(ACCKEY_TERMCOUNT, <<"term_with_count">>).
-define(ACCKEY_RAWKEYS, <<"raw_keys">>).
-define(ACCKEY_RAWTERMS, <<"raw_terms">>).
-define(ACCKEY_RAWCOUNT, <<"raw_count">>).
-define(ACCKEY_TERMRAWCOUNT, <<"term_with_rawcount">>).

-define(REQUIRED_KEYS, [?QUERY_LIST]).
-define(POSSIBLE_KEYS,
    [
        ?AGGREGATION_EXPRESSION,
        ?ACCUMULATION_OPTION,
        ?ACCUMULATION_TERM,
        ?SUBSTITUTIONS,
        ?TIMEOUT,
        ?INACTIVITY_TIMEOUT,
        ?QUERY_LIST,
        ?MAX_RESULTS,
        ?CONTINUATION
    ]
).
-define(REQUIRED_QL_KEYS,
    [
        ?QL_INDEX_NAME,
        ?QL_START_TERM,
        ?QL_END_TERM
    ]
).
-define(POSSIBLE_QL_KEYS,
    [
        ?QL_AGGREGATION_TAG,
        ?QL_INDEX_NAME,
        ?QL_START_TERM,
        ?QL_END_TERM,
        ?QL_REGULAR_EXPRESSION,
        ?QL_EVALUATION_EXPRESSION,
        ?QL_FILTER_EXPRESSION
    ]
).

-define(REQUEST_CLASS, {riak_kv, secondary_index}).

-define(QUERY_TIMEOUT, 60).
-define(QUEUE_INACTIVITY_TIMEOUT, 120).
-define(MAX_RESULTS_FROM_QUEUE, 1000).

-define(HEAD_CONTINUATION, "X-Riak-Continuation").

-type query_map() ::
    #{binary() => binary()|non_neg_integer()|list(map())}.

-spec init(proplists:proplist()) -> {ok, context()}.
%% @doc Initialize this resource.
init(Props) ->
    {ok, #ctx{
       riak=proplists:get_value(riak, Props),
       bucket_type=proplists:get_value(bucket_type, Props)
      }}.

-spec service_available(request_data(), context()) ->
    {boolean(), request_data(), context()}.
%% @doc Determine whether or not a connection to Riak
%%      can be established. Also, extract query params.
service_available(RD, Ctx0=#ctx{riak=RiakProps}) ->
    Ctx = riak_kv_wm_utils:ensure_bucket_type(RD, Ctx0, #ctx.bucket_type),
    ClientID = riak_kv_wm_utils:get_client_id(RD),
    case riak_kv_wm_utils:get_riak_client(RiakProps, ClientID) of
        {ok, C} ->
            {
                true,
                RD,
                Ctx#ctx{client = C, method = wrq:method(RD)}
            };
        Error ->
            {
                false,
                wrq:set_resp_body(
                    io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
                    wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
                Ctx
            }
    end.

resource_exists(RD, #ctx{bucket_type=BType}=Ctx) ->
    {riak_kv_wm_utils:bucket_type_exists(BType), RD, Ctx}.
    
-spec is_authorized(request_data(), context()) ->
    {true | string() | {halt, 426}, request_data(), context()}.
is_authorized(ReqData, Ctx) ->
    case riak_api_web_security:is_authorized(ReqData) of
        false ->
            {"Basic realm=\"Riak\"", ReqData, Ctx};
        {true, SecContext} ->
            {true, ReqData, Ctx#ctx{security=SecContext}};
        insecure ->
            %% XXX 301 may be more appropriate here, but since the http and
            %% https port are different and configurable, it is hard to figure
            %% out the redirect URL to serve.
            {
                {halt, 426},
                wrq:append_to_resp_body(
                    <<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>,
                    ReqData
                ),
                Ctx
            }
    end.

-spec forbidden(request_data(), context())
        -> {boolean(), request_data(), context()}.
forbidden(ReqDataIn, #ctx{security = undefined} = Context) ->
    riak_kv_wm_utils:is_forbidden(ReqDataIn, ?REQUEST_CLASS, Context);
forbidden(ReqDataIn, #ctx{bucket_type = BT, security = Sec} = Context) ->
    {Answer, ReqData, _} = Result =
        riak_kv_wm_utils:is_forbidden(ReqDataIn, ?REQUEST_CLASS, Context),
    case Answer of
        false ->
            Bucket = erlang:list_to_binary(
                riak_kv_wm_utils:maybe_decode_uri(
                    ReqData, wrq:path_info(bucket, ReqData))),
            case riak_core_security:check_permission(
                    {"riak_kv.index", {BT, Bucket}}, Sec) of
                {false, Error, _} ->
                    {true,
                        wrq:append_to_resp_body(
                            unicode:characters_to_binary(Error, utf8, utf8),
                            wrq:set_resp_header(
                                "Content-Type", "text/plain", ReqData)),
                        Context};
                {true, _} ->
                    {false, ReqData, Context}
            end;
        _ ->
            Result
    end.

-spec allowed_methods(
    request_data(), context()) -> {list(atom()), request_data(), context()}.
allowed_methods(RD, Ctx) ->
    {['POST', 'GET'], RD, Ctx}.

-spec malformed_request(
    request_data(), context()) ->
        {boolean(), request_data(), context()}.
malformed_request(RD, Ctx) when Ctx#ctx.method =:= 'POST' ->
    Bucket = get_bucket(RD),
    BT = riak_kv_wm_utils:maybe_bucket_type(Ctx#ctx.bucket_type, Bucket),
    Body = riak_kv_wm_utils:accept_value("application/json", wrq:req_body(RD)),
    case decode_json_body(Body) of
        {ok, QueryMap} ->
            case check_keys(maps:keys(QueryMap), request) of
                ok ->
                    QueryList = maps:get(?QUERY_LIST, QueryMap),
                    case check_querylist(QueryList, false) of
                        ok ->
                            case make_query_request(BT, QueryMap) of
                                {ok, Query} ->
                                    {false, RD, Ctx#ctx{query_request = Query}};
                                {error, Stage, Reason} ->
                                    {
                                        true,
                                        return_json_error(
                                            expand_query_reason(Stage, Reason),
                                            RD
                                        ),
                                        Ctx
                                    }
                                end;
                        {error, Reason} ->
                            {true, return_json_error(Reason, RD), Ctx}
                    end;
                {error, Reason} ->
                    {true, return_json_error(Reason, RD), Ctx}
            end;
        {error, Reason} ->
            {true, return_json_error(Reason, RD), Ctx}
    end;
malformed_request(RD, Ctx) when Ctx#ctx.method =:= 'GET' ->
    Bucket = get_bucket(RD),
    BT = riak_kv_wm_utils:maybe_bucket_type(Ctx#ctx.bucket_type, Bucket),
    case wrq:get_qs_value("result_queue", RD) of
        QueueString when is_list(QueueString) ->
            case wrq:get_qs_value("max_results", RD) of
                undefined ->
                    application:get_env(
                        riak_kv,
                        queue_raw_max_results,
                        ?MAX_RESULTS_FROM_QUEUE
                    );
                MaxResultsString ->
                    try
                        case list_to_integer(MaxResultsString) of
                            MR when is_integer(MR), MR >= 0 ->
                                {
                                    false,
                                    RD,
                                    Ctx#ctx{
                                        queue_request =
                                            make_queue_request(
                                                BT,
                                                list_to_binary(QueueString),
                                                MR
                                            )
                                    }
                                }
                                
                        end
                    catch
                        _CP:_EP ->
                            {
                                true,
                                return_json_error(
                                    "Invalid max_results parameter",
                                    RD
                                ),
                                Ctx
                            }
                    end
            end;
        _ ->
            {
                true,
                return_json_error(
                    "No valid result_queue reference"
                    "passed as query parameter",
                    RD
                ),
                Ctx
            }
    end.

-spec content_types_provided(request_data(), context()) ->
    {[{ContentType::string(), Producer::atom()}], request_data(), context()}.
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for bucket lists.
content_types_provided(RD, Ctx) when Ctx#ctx.method =:= 'POST' ->
    {[{"application/json", nop}], RD, Ctx};
content_types_provided(RD, Ctx) when Ctx#ctx.method =:= 'GET' ->
    {[{"application/json", return_queued_results}], RD, Ctx}.

-spec return_queued_results(
    request_data(), context()
) -> 
    {binary(), request_data(), context()}.
return_queued_results(RD, Ctx = #ctx{queue_request = QR, client = C}) ->
    case riak_client:query_result_request(QR, C) of
        {ok, ResultMap} ->
            {encode_queued_results(ResultMap), RD, Ctx};
        {error, result_server_terminated} ->
            {
                {halt, 410},
                    % Response code for Gone, and likely to be permanent.
                    % This may be as a result of an error on the server, but
                    % is probably as a result of an error on the client - and
                    % so to help with load-balancers tracking server errors,
                    % err on the side of blaming the client
                return_json_error(
                    "queue no longer present or not currently reachable\n",
                    RD
                ),
                Ctx
            };
        {error, unexpected_reference_format} ->
            {
                {halt, 400},
                return_json_error(
                    "queue reference passed had an invalid format\n",
                    RD
                ),
                Ctx
            };
        {error, Reason} ->
            {{error, Reason}, RD, Ctx}
    end.

%% The bucket is available in the dispatch properties, however it may need to
%% URL quoted, and so it needs to be unquoted.
%% 
%% Note that it is possible to disable quoting, and force it per request using
%% the "X-Riak-URL-Encoding" header - hence why the full RD is required to
%% decide on the unquoting or not of one part.
get_bucket(RD) ->
    list_to_binary(
            riak_kv_wm_utils:maybe_decode_uri(RD, wrq:path_info(bucket, RD)
        )
    ).

expand_query_reason(Stage, Reason) ->
    lists:flatten(
        io_lib:format(
            << "Validation failure at stage ~0p due to ~s">>, 
            [Stage, Reason]
        )
    ).

-spec return_json_error(string(), request_data()) -> request_data().
return_json_error(Reason, RD) ->
    wrq:append_to_resp_body(
        riak_kv_wm_json:encode(#{error => Reason}),
        wrq:set_resp_header(
            ?HEAD_CTYPE, "application/json", RD
        )
    ).

-spec decode_json_body(binary()) -> {ok, map()}| {error, term()}.
decode_json_body(JsonBody) ->
    try
        DecodedBody = riak_kv_wm_json:decode(JsonBody),
        {ok, DecodedBody}
    catch
        error:Reason ->
            ExpandedReason =
                lists:flatten(
                    io_lib:format(
                        <<"Malformed json request - ~0p">>, 
                        [Reason]
                    )
                ),
            {error, ExpandedReason}
    end.

check_querylist([], true) ->
    ok;
check_querylist([], false) ->
    {error, <<"No valid query provided">>};
check_querylist([HdQuery|Rest], _AtLeastOne) ->
    case check_keys(maps:keys(HdQuery), query) of
        ok ->
            check_querylist(Rest, true);
        Error ->
            Error
    end.

check_keys(Keys, request) ->
    check_keys(Keys, ?REQUIRED_KEYS, ?POSSIBLE_KEYS);
check_keys(Keys, query) ->
    check_keys(Keys, ?REQUIRED_QL_KEYS, ?POSSIBLE_QL_KEYS).

-spec check_keys(
    list(binary()), list(binary()), list(binary())) -> ok|{error, string()}.
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
                        lists:flatten(
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
                lists:flatten(
                    io_lib:format(
                        <<"Missing required keys in request ~0p">>,
                        [MissingKeys]
                    )
                )
            }
    end.

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

-spec make_query_request(
    riak_object:bucket(), query_map()) ->
        {ok, riak_kv_query:complex_query_definition()}|riak_kv_query:validation_error().
make_query_request(BucketType, QueryMap) ->
    case fetch_timeouts(QueryMap) of
        {ok, Timeout, InactivityTimeout} ->
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
            case add_accumulation(QueryMap, InitQuery) of
                {ok, Q1} ->
                    case add_queries(QueryMap, Q1, QueryList) of
                        {ok, Q2} ->
                            case maps:get(?CONTINUATION, QueryMap, none) of
                                none ->
                                    {ok, Q2};
                                Continuation ->
                                    riak_kv_query:add_continuation(Q2, Continuation)
                            end;
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec fetch_timeouts(
    query_map()
) ->
    {ok, pos_integer(), pos_integer()} | riak_kv_query:validation_error().
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
                    {error, init, <<"Bad inactivity timeout">>}
            end;
        _ ->
            {error, init, <<"Bad timeout">>}
    end.

-spec add_accumulation(
    query_map(), riak_kv_query:complex_query_definition())
        -> 
            {ok, riak_kv_query:complex_query_definition()} |
            riak_kv_query:validation_error().
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
    list(#{binary() => binary()})) ->
        {ok, riak_kv_query:complex_query_definition()}|
        riak_kv_query:validation_error().
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

-spec process_post(request_data(), context()) ->
    {boolean()|{halt, pos_integer()}, request_data(), context()}.
%% @doc Produce the JSON response to an index lookup.
process_post(RD, Ctx) ->
    Client = Ctx#ctx.client,
    AccOpt = riak_kv_query:get_accumulator(Ctx#ctx.query_request),
    {ok, Query} =
        riak_kv_query:add_result_encodingfun(
            Ctx#ctx.query_request,
            encoding_function(AccOpt)
        ),
    case riak_client:query(Query, Client) of
        {error, timeout} ->
            {{halt, 503}, return_json_error("timeout", RD), Ctx};
        {error, Reason} ->
            Error =
                lists:flatten(
                    io_lib:format(
                        <<"Query with option ~w failed - ~0p">>,
                        [AccOpt, Reason]
                    )
                ),
            {{halt, 500}, return_json_error(Error, RD), Ctx};
        {result_queue, ResultReference} when is_binary(ResultReference) ->
            {
                true,
                wrq:append_to_resp_body(
                    riak_kv_wm_json:encode(
                        #{result_queue => ResultReference}
                    ),
                    wrq:set_resp_header(?HEAD_CTYPE, "application/json", RD)
                ),
                Ctx
            };
        {JsonEncodedResults, none} when is_binary(JsonEncodedResults) ->
            {
                true,
                wrq:append_to_resp_body(
                    JsonEncodedResults,
                    wrq:set_resp_header(?HEAD_CTYPE, "application/json", RD)
                ),
                Ctx
            };
        {JsonEncodedResults, {{LT,  LK}}}
                when
                    is_binary(JsonEncodedResults),
                    is_binary(LT),
                    is_binary(LK) ->
            Continuation = riak_kv_query:make_continuation(LT, LK),
            {
                true,
                wrq:append_to_resp_body(
                    JsonEncodedResults,
                    wrq:set_resp_header(
                        ?HEAD_CONTINUATION,
                        Continuation,
                        wrq:set_resp_header(
                            ?HEAD_CTYPE,
                            "application/json",
                            RD
                        )
                    )
                ),
                Ctx
            }
    end.

-spec encoding_function(riak_kv_query:accumulation_option()) ->
    fun((riak_kv_query_server:results()) -> binary()).
encoding_function(AccOpt) ->
    fun(Results) -> encode_results(AccOpt, Results) end.

-spec encode_queued_results(
    riak_kv_query_server:partial_result_map()) -> binary().
encode_queued_results(ResultMap) ->
    case maps:is_key(get_result_key(raw_keys), ResultMap) of
        true ->
            iolist_to_binary(
                riak_kv_wm_json:encode(
                    ResultMap,
                    fun riak_kv_wm_query:encode_key/2
                )
            );
        false ->
            case maps:is_key(get_result_key(raw_terms), ResultMap) of
                true ->
                    iolist_to_binary(
                        riak_kv_wm_json:encode(
                            ResultMap,
                            fun riak_kv_wm_query:encode_key_withterm/2
                        )
                    )
            end
    end.

-spec encode_results(
    riak_kv_query:accumulation_option(), riak_kv_query_server:results()) -> binary().
encode_results(AccOpt, Results) when AccOpt == keys; AccOpt == raw_keys ->
    iolist_to_binary(
        riak_kv_wm_json:encode(
            #{get_result_key(AccOpt) => Results},
            fun riak_kv_wm_query:encode_key/2
        )
    );
encode_results(AccOpt, Results) when AccOpt == terms; AccOpt == raw_terms ->
    iolist_to_binary(
        riak_kv_wm_json:encode(
            #{get_result_key(AccOpt) => Results},
            fun riak_kv_wm_query:encode_key_withterm/2
        )
    );
encode_results(AccOpt, Count) when AccOpt == count; AccOpt == raw_count ->
    iolist_to_binary(
        riak_kv_wm_json:encode(#{get_result_key(AccOpt) => Count})
    );
encode_results(AccOpt, CountMap)
        when AccOpt == term_with_count; AccOpt == term_with_rawcount ->
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
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

invalid_json_test() ->
    InvalidJson =
        <<"
            {
                \"accumulation_option\" : \"keys\",
                \"timeout\" : 60,
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"example_bin\"
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        }
                    ]
            }
        ">>, % Missing comma after example_bin
    R = decode_json_body(InvalidJson),
    io:format("~p~n", [R]),
    ?assertMatch(
        {error, "Malformed json request - {invalid_byte,34}"},
        R
    ).

simple_query_test() ->
    SimpleQueryJson =
        <<"
            {
                \"timeout\" : 60,
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        }
                    ]
            }
        ">>,
    {ok, M} = decode_json_body(SimpleQueryJson),
    {ok, Q} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assert(riak_kv_query:is_query(Q)).

invalid_query_ae1_test() ->
    IQJson =
        <<"
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"timeout\" : 60,
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        }
                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    {error, S, _E} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assertMatch(aggregation_expression, S).

invalid_query_ae2_test() ->
    IQJson =
        <<"
            {
                \"timeout\" : 60,
                \"query_list\" :
                    [
                        {
                            \"aggregation_tag\" : 1,
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        },
                        {
                            \"aggregation_tag\" : 2,
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        }

                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    {error, S, _E} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assertMatch(aggregation_expression, S).

invalid_query_ae3_test() ->
    IQJson =
        <<"
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"timeout\" : 60,
                \"query_list\" :
                    [
                        {
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        },
                        {
                            \"aggregation_tag\" : 2,
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        }

                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    {error, S, E} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assertMatch(query_evaluation, S),
    ?assertMatch(<<"Untagged query in combination request">>, E).

valid_query_ae4_test() ->
    IQJson =
        <<"
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"timeout\" : 60,
                \"inactivity_timeout\" : 180,
                \"query_list\" :
                    [
                        {
                            \"aggregation_tag\" : 1,
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        },
                        {
                            \"aggregation_tag\" : 2,
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        }

                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    {ok, Q} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assert(riak_kv_query:is_query(Q)),
    QueryList = maps:get(<<"query_list">>, M),
    ?assertMatch(ok, check_querylist(QueryList, false)).

valid_query_ae5_test() ->
    IQJson =
        <<"
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"timeout\" : 60,
                \"accumulation_option\" : \"keys\",
                \"substitutions\" :
                    {\"low_dob\" : \"20210804\", \"high_dob\" : \"20223101\", \"gnsc\" : \"Ma\"},
                \"query_list\" :
                    [
                        {
                            \"aggregation_tag\" : 1,
                            \"index_name\" : \"example1_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\",
                            \"evaluation_expression\" :
                                \"delim($term, \\\"|\\\", ($fn, $dob, $dod, $gns, $pcs)) | slice($gns, 2, $gns)\",
                            \"filter_expression\" : \"($dob BETWEEN :low_dob AND :high_dob\) AND contains($gns, :gnsc)\"
                        },
                        {
                            \"aggregation_tag\" : 2,
                            \"index_name\" : \"example2_bin\",
                            \"start_term\" : \"C\",
                            \"end_term\"   : \"D\"
                        }

                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    {ok, Q} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assert(riak_kv_query:is_query(Q)),
    QueryList = maps:get(<<"query_list">>, M),
    ?assertMatch(ok, check_querylist(QueryList, false)).

invalid_query_ae6_test() ->
    IQJson = % unescaped "|" in eval expression
        <<"
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"timeout\" : 60,
                \"accumulation_option\" : \"keys\",
                \"substitutions\" :
                    {\"low_dob\" : \"20210804\", \"high_dob\" : \"20223101\", \"gnsc\" : \"Ma\"},
                \"query_list\" :
                    [
                        {
                            \"aggregation_tag\" : 1,
                            \"index_name\" : \"example1_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\",
                            \"evaluation_expression\" :
                                \"delim($term, |, ($fn, $dob, $dod, $gns, $pcs)) | slice($gns, 2, $gns)\",
                            \"filter_expression\" : \"($dob BETWEEN :low_dob AND :high_dob\) AND contains($gns, :gnsc)\"
                        },
                        {
                            \"aggregation_tag\" : 2,
                            \"index_name\" : \"example2_bin\",
                            \"start_term\" : \"C\",
                            \"end_term\"   : \"D\"
                        }

                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, query_evaluation, <<"Invalid eval function">>},
        make_query_request({<<"BT">>, <<"B">>}, M)
    ).

invalid_query_ae7_test() ->
    IQJson = % BETWEN not BETWEEN
        <<"
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"timeout\" : 60,
                \"accumulation_option\" : \"keys\",
                \"substitutions\" :
                    {\"low_dob\" : \"20210804\", \"high_dob\" : \"20223101\", \"gnsc\" : \"Ma\"},
                \"query_list\" :
                    [
                        {
                            \"aggregation_tag\" : 1,
                            \"index_name\" : \"example1_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\",
                            \"evaluation_expression\" :
                                \"delim($term, \\\"|\\\", ($fn, $dob, $dod, $gns, $pcs)) | slice($gns, 2, $gns)\",
                            \"filter_expression\" : \"($dob BETWEN :low_dob AND :high_dob\) AND contains($gns, :gnsc)\"
                        },
                        {
                            \"aggregation_tag\" : 2,
                            \"index_name\" : \"example2_bin\",
                            \"start_term\" : \"C\",
                            \"end_term\"   : \"D\"
                        }

                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, query_evaluation, <<"Invalid filter function">>},
        make_query_request({<<"BT">>, <<"B">>}, M)
    ).

invalid_query_ae8_test() ->
    IQJson = % missing substitution
        <<"
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"timeout\" : 60,
                \"accumulation_option\" : \"keys\",
                \"substitutions\" :
                    {\"low_dob\" : \"20210804\", \"gnsc\" : \"Ma\"},
                \"query_list\" :
                    [
                        {
                            \"aggregation_tag\" : 1,
                            \"index_name\" : \"example1_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\",
                            \"evaluation_expression\" :
                                \"delim($term, \\\"|\\\", ($fn, $dob, $dod, $gns, $pcs)) | slice($gns, 2, $gns)\",
                            \"filter_expression\" : \"($dob BETWEEN :low_dob AND :high_dob\) AND contains($gns, :gnsc)\"
                        },
                        {
                            \"aggregation_tag\" : 2,
                            \"index_name\" : \"example2_bin\",
                            \"start_term\" : \"C\",
                            \"end_term\"   : \"D\"
                        }

                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, query_evaluation, <<"Invalid filter function">>},
        make_query_request({<<"BT">>, <<"B">>}, M)
    ).

invalid_query_to_test() ->
    IQJson =
        <<"
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"timeout\" : 0,
                \"query_list\" :
                    [
                        {
                            \"aggregation_tag\" : 1,
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        },
                        {
                            \"aggregation_tag\" : 2,
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        }

                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    {error, S, E} = make_query_request({<<"BT">>, <<"B">>}, M),
    ?assertMatch(init, S),
    ?assertMatch(<<"Bad timeout">>, E).

invalid_query_extratag_test() ->
    IQJson =
        <<"
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"timeout\" : 60,
                \"subs\" : {\"dob\" : \"19260812\"},
                \"query_list\" :
                    [
                        {
                            \"aggregation_tag\" : 1,
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        },
                        {
                            \"aggregation_tag\" : 2,
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\",
                            \"end_key\"   : \"B\"
                        }

                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, "Unexpected keys in request [<<\"subs\">>]"},
        check_keys(maps:keys(M), request)
    ),
    ?assertMatch(
        {error, "Unexpected keys in request [<<\"end_key\">>]"},
        check_querylist(maps:get(<<"query_list">>, M), false)
    ).

invalid_query_missingtag1_test() ->
    IQJson =
        <<"
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"timeout\" : 60,
                \"subs\" : {\"dob\" : \"19260812\"}
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, "Missing required keys in request [<<\"query_list\">>]"},
        check_keys(maps:keys(M), request)
    ).
    
invalid_query_missingtag2_test() ->
    IQJson =
        <<"
            {
                \"aggregation_expression\" : \"$1 INTERSECT $2\",
                \"timeout\" : 60,
                \"query_list\" :
                    [
                        {
                            \"aggregation_tag\" : 1,
                            \"index_name\" : \"example_bin\",
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        },
                        {
                            \"aggregation_tag\" : 2,
                            \"index_name\" : \"example_bin\",
                            \"end_term\"   : \"B\"
                        }

                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    ?assertMatch(
        {error, "Missing required keys in request [<<\"start_term\">>]"},
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
                fun(M) -> [{T, K}] = maps:to_list(M), {T, K} end,
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
                fun(M) -> [{T, K}] = maps:to_list(M), {T, K} end,
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
    )
    .

-endif.