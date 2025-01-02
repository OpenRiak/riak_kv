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
%% POST types/BucketType/buckets/Bucket/query
%% ```
%%
%% The query should be posted as the HTTP body, where there are the following
%% JSON keys at the root of the document
%% 
%% - aggregation_expression (optional)
%% If multiple queries are to be run, the aggregation expression is used to
%% inform the database how those results should be combined, using $1, $2 etc
%% to refer to the numeric aggregation_tag for each query - with the key words
%% UNION, INTERSECT and SUBTRACT to show how the sets of results are to be
%% combined.  Parenthesis may be used for clarity.
%% e.g. ($1 INTERSECT $2) UNION ($3 SUBTRACT $1)
%% 
%% - accumulation_option (optional - default = keys)
%% There are six options for accumulating the results from a single query: 
%% keys (return a list of keys), term_with_keys (return a list of term/key
%% tuples), match_count (return a count of the matches made), key_count (return
%% a count of unique keys matched), term_with_matchcount/term_with_keycount
%% (return a map of term to either count of matches, or count of unique keys).
%% If an aggregation_expression is used, only keys, key_count and match_count
%% can be used.
%% 
%% - accumulation_term (optional - default = $term)
%% When using an accumulation option of term_with_keys, term_with_matchcount or
%% term_with_keycount which term in the evaluated index term should be used.
%% The defualt is $term - the whole term.  However a sub-term extracted in the
%% evaluation expression may be used instead.  
%% 
%% - result_provision (not yet implemented)
%% 
%% - max_results (not yet implemented)
%% 
%% - term_rate_kpersec (not yet implemented)
%% 
%% - substitutions (optional)
%% A array of key/value pairs that match string that are referred to in queries
%% to substitution values that should replace those keys in the query.
%% e.g. {"low_dob" : "19550301", "high_dob" : "19560630"} can be passed as
%% substitutions to populate an evaluation of
%% "$dob" BETWEEN ":low_dob" AND ":high_dob"
%% 
%% - timeout (optional)
%% The timeout in seconds to wait for the query to complete
%% 
%% - query_list
%% A list of queries (should be a list of just one query if an
%% aggregation_expression is not used).
%% Each query has the following parts:
%% - aggregation_tag (optional unless an aggregation_expression is used)
%% - index_name (should be a binary index)
%% - start_term
%% - end_term
%% - regular expression (optional alternative to using evaluation or filter
%%  expressions)
%% - evaluation_expression (optional, an expression to extract projected
%% attributes from the term)
%% - filter_expression (optional, an expression to filter results based on
%% those projected attributes)
 
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
    encode_key_withterm/2
]).

-record(ctx, {
          client,       %% riak_client() - the store client
          riak,         %% local | {node(), atom()} - params for riak client
          bucket_type,  %% Bucket type (from uri)
          query,        %% The query..
          security,     %% security context
          accumulation_option
         }).
-type context() :: #ctx{}.
-type request_data() :: #wm_reqdata{}.

-define(AGGREGATION_EXPRESSION, <<"aggregation_expression">>).
-define(ACCUMULATION_OPTION, <<"accumulation_option">>).
-define(ACCUMULATION_TERM, <<"accumulation_term">>).
-define(SUBSTITUTIONS, <<"substitutions">>).
-define(TIMEOUT, <<"timeout">>).
-define(QUERY_LIST, <<"query_list">>).
-define(QL_AGGREGATION_TAG, <<"aggregation_tag">>).
-define(QL_INDEX_NAME, <<"index_name">>).
-define(QL_START_TERM, <<"start_term">>).
-define(QL_END_TERM, <<"end_term">>).
-define(QL_REGULAR_EXPRESSION, <<"regular_expression">>).
-define(QL_EVALUATION_EXPRESSION, <<"evaluation_expression">>).
-define(QL_FILTER_EXPRESSION, <<"filter_expression">>).

-define(ACCKEY_KEYS, <<"keys">>).
-define(ACCKEY_RAWKEYS, <<"raw_keys">>).
-define(ACCKEY_TERMKEYS, <<"term_with_keys">>).
-define(ACCKEY_KEYCOUNT, <<"key_count">>).
-define(ACCKEY_MATCHCOUNT, <<"match_count">>).
-define(ACCKEY_TERMMATCHCOUNT, <<"term_with_matchcount">>).
-define(ACCKEY_TERMKEYCOUNT, <<"term_with_keycount">>).

-define(REQUIRED_KEYS, [?QUERY_LIST]).
-define(POSSIBLE_KEYS,
    [
        ?AGGREGATION_EXPRESSION,
        ?ACCUMULATION_OPTION,
        ?ACCUMULATION_TERM,
        ?SUBSTITUTIONS,
        ?TIMEOUT,
        ?QUERY_LIST
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
    case riak_kv_wm_utils:get_riak_client(RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            {true, RD, Ctx#ctx { client=C }};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
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
    {['POST'], RD, Ctx}.

-spec malformed_request(
    request_data(), context()) ->
        {boolean(), request_data(), context()}.
malformed_request(RD, Ctx) ->
    Bucket =
        list_to_binary(
            riak_kv_wm_utils:maybe_decode_uri(RD, wrq:path_info(bucket, RD)
        )
    ),
    BT = riak_kv_wm_utils:maybe_bucket_type(Ctx#ctx.bucket_type, Bucket),
    Body = riak_kv_wm_utils:accept_value("application/json", wrq:req_body(RD)),
    case decode_json_body(Body) of
        {ok, QueryMap} ->
            case check_keys(maps:keys(QueryMap), request) of
                ok ->
                    QueryList = maps:get(?QUERY_LIST, QueryMap),
                    case check_querylist(QueryList, false) of
                        ok ->
                            case make_query(BT, QueryMap) of
                                {ok, Query} ->
                                    {false, RD, Ctx#ctx{query = Query}};
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
    end.

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

-spec make_query(
    riak_object:bucket(), query_map()) ->
        {ok, riak_kv_query:complex_query_definition()}|riak_kv_query:validation_error().
make_query(BucketType, QueryMap) ->
    Timeout =
        maps:get(
            ?TIMEOUT,
            QueryMap,
            application:get_env(riak_kv, query_timeout_secs, ?QUERY_TIMEOUT)
        ),
    case Timeout of
        T when is_integer(T), T > 0 ->
            InitQuery =
                case maps:get(?QUERY_LIST, QueryMap) of
                    QueryList when length(QueryList) == 1 ->
                        riak_kv_query:new(BucketType, single_query, Timeout);
                    QueryList when length(QueryList) > 1 ->
                        riak_kv_query:new(BucketType, combo_query, Timeout)
                end,
            case make_accumulation(QueryMap, InitQuery) of
                {ok, Q1} ->
                    AggExpr =
                        maps:get(
                            ?AGGREGATION_EXPRESSION, QueryMap, undefined
                        ),
                    case riak_kv_query:add_aggregation_expression(Q1, AggExpr) of
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
                    end;
                Error ->
                    Error
            end;
        _ ->
            {error, init, <<"Bad timeout">>}
    end.
                    
-spec make_accumulation(
    query_map(), riak_kv_query:complex_query_definition())
        -> 
            {ok, riak_kv_query:complex_query_definition()} |
            riak_kv_query:validation_error().
make_accumulation(QueryMap, InitQuery) ->
    AccOpt = maps:get(?ACCUMULATION_OPTION, QueryMap, undefined),
    AccTerm = maps:get(?ACCUMULATION_TERM, QueryMap, undefined),
    case riak_kv_query:add_accumulation_option(InitQuery, AccOpt) of
        {ok, UpdQuery} ->
            riak_kv_query:add_accumulation_term(UpdQuery, AccTerm);
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
    AccOpt = riak_kv_query:get_accumulator(Ctx#ctx.query),
    {ok, Query} =
        riak_kv_query:add_result_encodingfun(
            Ctx#ctx.query,
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
        JsonEncodedResults ->
            {
                true,
                wrq:append_to_resp_body(
                    JsonEncodedResults,
                    wrq:set_resp_header(?HEAD_CTYPE, "application/json", RD)
                ),
                Ctx
            }
    end.

-spec encoding_function(riak_kv_query:accumulation_option()) ->
    fun((riak_kv_query_server:results()) -> binary()).
encoding_function(AccOpt) ->
    fun(Results) -> encode_results(AccOpt, Results) end.

-spec encode_results(
    riak_kv_query:accumulation_option(), riak_kv_query_server:results()) -> binary().
encode_results(keys, Results) ->
    iolist_to_binary(
        riak_kv_wm_json:encode(
            #{?ACCKEY_KEYS => Results},
            fun encode_key/2
        )
    );
encode_results(raw_keys, Results) ->
    iolist_to_binary(
        riak_kv_wm_json:encode(
            #{?ACCKEY_RAWKEYS => Results}
        )
    );
encode_results(term_with_keys, Results) ->
    iolist_to_binary(
        riak_kv_wm_json:encode(
            #{?ACCKEY_TERMKEYS => Results},
            fun riak_kv_wm_query:encode_key_withterm/2
        )
    );
encode_results(match_count, Count) ->
    iolist_to_binary(
        riak_kv_wm_json:encode(#{?ACCKEY_MATCHCOUNT => Count})
    );
encode_results(key_count, Count) ->
    iolist_to_binary(
        riak_kv_wm_json:encode(#{?ACCKEY_KEYCOUNT => Count})
    );
encode_results(term_with_matchcount, CountMap) ->
    iolist_to_binary(
        riak_kv_wm_json:encode(#{?ACCKEY_TERMMATCHCOUNT => CountMap})
    );
encode_results(term_with_keycount, CountMap) ->
    iolist_to_binary(
        riak_kv_wm_json:encode(#{?ACCKEY_TERMKEYCOUNT => CountMap})
    ).

encode_key({Key}, Encode) when is_binary(Key) ->
    encode_key(Key, Encode);
encode_key(Key, Encode) ->
    riak_kv_wm_json:encode_value(Key, Encode).

encode_key_withterm({TermKeyTuple}, Encode) when is_tuple(TermKeyTuple) ->
    encode_key_withterm(TermKeyTuple, Encode);
encode_key_withterm({Term, Key}, Encode) when is_binary(Term), is_binary(Key) ->
    ["{", [Encode(Term, Encode), $: | Encode(Key, Encode)], "}"];
encode_key_withterm(Result, Encode) ->
    riak_kv_wm_json:encode_value(Result, Encode).

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
    {ok, Q} = make_query({<<"BT">>, <<"B">>}, M),
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
    {error, S, _E} = make_query({<<"BT">>, <<"B">>}, M),
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
    {error, S, _E} = make_query({<<"BT">>, <<"B">>}, M),
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
    {error, S, E} = make_query({<<"BT">>, <<"B">>}, M),
    ?assertMatch(query_evaluation, S),
    ?assertMatch(<<"Untagged query in combination request">>, E).

valid_query_ae4_test() ->
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
                            \"start_term\" : \"A\",
                            \"end_term\"   : \"B\"
                        }

                    ]
            }
        ">>,
    {ok, M} = decode_json_body(IQJson),
    {ok, Q} = make_query({<<"BT">>, <<"B">>}, M),
    ?assert(riak_kv_query:is_query(Q)),
    QueryList = maps:get(<<"query_list">>, M),
    ?assertMatch(ok, check_querylist(QueryList, false)).

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
    {error, S, E} = make_query({<<"BT">>, <<"B">>}, M),
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
    BinMC = encode_results(match_count, 500),
    ?assertMatch(
        500,
        maps:get(?ACCKEY_MATCHCOUNT, riak_kv_wm_json:decode(BinMC))
    ),
    BinKC = encode_results(key_count, 600),
    ?assertMatch(
        600,
        maps:get(?ACCKEY_KEYCOUNT, riak_kv_wm_json:decode(BinKC))
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
    BinTKL = encode_results(term_with_keys, TermKeyList),
    ?assertMatch(
        TermKeyList,
        lists:sort(
            lists:map(
                fun(M) -> [{T, K}] = maps:to_list(M), {T, K} end,
                maps:get(?ACCKEY_TERMKEYS, riak_kv_wm_json:decode(BinTKL))
            )
        )
    ),
    TermKeyListT =
        [{{<<"T0001">>, <<"K0002">>}}, {{<<"T0002">>, <<"K0001">>}}],
    BinTKLT = encode_results(term_with_keys, TermKeyListT),
    ?assertMatch(
        TermKeyList,
        lists:sort(
            lists:map(
                fun(M) -> [{T, K}] = maps:to_list(M), {T, K} end,
                maps:get(?ACCKEY_TERMKEYS, riak_kv_wm_json:decode(BinTKLT))
            )
        )
    ),
    TermCount = #{<<"T0001">> => 12, <<"T0002">> => 10},
    BinTKC = encode_results(term_with_keycount, TermCount),
    ?assertMatch(
        10,
        maps:get(
            <<"T0002">>,
            maps:get(?ACCKEY_TERMKEYCOUNT, riak_kv_wm_json:decode(BinTKC))
        )
    ),
    BinTMC = encode_results(term_with_matchcount, TermCount),
    ?assertMatch(
        12,
        maps:get(
            <<"T0001">>,
            maps:get(?ACCKEY_TERMMATCHCOUNT, riak_kv_wm_json:decode(BinTMC))
        )
    )
    .

-endif.