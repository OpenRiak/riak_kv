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
%% GET /buckets/Bucket/index/IndexName/Key
%% GET /buckets/Bucket/index/IndexName/Start/End'''
%%
%%   Run an index lookup, return the results as JSON.
%%
%% ```
%% GET /types/Type/buckets/Bucket/index/IndexName/Key
%% GET /types/Type/buckets/Bucket/index/IndexName/Start/End'''
%%
%%   Run an index lookup over the Bucket in BucketType, returning the
%%   results in JSON.
%%
%% Both URL formats support the following query-string options:
%% <ul>
%%   <li><tt>max_results=Integer</tt><br />
%%         limits the number of results returned</li>
%%   <li><tt>stream=true</tt><br />
%%         streams the results back in multipart/mixed chunks</li>
%%   <li><tt>continuation=C</tt><br />
%%         the continuation returned from the last query, used to
%%         fetch the next "page" of results.</li>
%%   <li><tt>return_terms=true</tt><br />
%%         when querying with a range, returns the value of the index
%%         along with the key.</li>
%%   <li><tt>pagination_sort=true|false</tt><br />
%%         whether the results will be sorted. Ignored when max_results
%%         is set, as pagination requires sorted results.</li>
%% </ul>

-module(riak_kv_wm_index).

%% webmachine resource exports
-export([
    init/1,
    service_available/2,
    is_authorized/2,
    forbidden/2,
    malformed_request/2,
    content_types_provided/2,
    encodings_provided/2,
    resource_exists/2,
    produce_index_results/2
]).
-export([
    mochijson_encode_results/2,
    mochijson_encode_results/3,
    otp_encode_results/2,
    otp_encode_results/3,
    results_encode/2,
    keys_encode/2
]).

-record(ctx, {
          client,       %% riak_client() - the store client
          riak,         %% local | {node(), atom()} - params for riak client
          bucket_type,  %% Bucket type (from uri)
          bucket,       %% The bucket to query.
          index_query,   %% The query..
          streamed = false :: boolean(), %% stream results to client
          max_results :: all | pos_integer(), %% maximum number of 2i results to return, the page size.
          return_terms = false :: boolean(), %% should the index values be returned
          timeout :: non_neg_integer() | undefined | infinity,
          pagination_sort :: boolean() | undefined,
          security       %% security context
         }).
-type context() :: #ctx{}.

-define(DEFAULT_JSON_ENCODING, otp).

-include_lib("kernel/include/logger.hrl").

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").
-include("riak_kv_index.hrl").

-spec init(proplists:proplist()) -> {ok, context()}.
%% @doc Initialize this resource.
init(Props) ->
    {ok, #ctx{
       riak=proplists:get_value(riak, Props),
       bucket_type=proplists:get_value(bucket_type, Props),
       max_results=all % may be refined once params evaluated
      }}.


-spec service_available(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
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
            {{halt, 426}, wrq:append_to_resp_body(<<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>, ReqData), Ctx}
    end.

-spec forbidden(#wm_reqdata{}, context())
        -> {boolean(), #wm_reqdata{}, context()}.
forbidden(ReqDataIn, #ctx{security = undefined} = Context) ->
    Class = request_class(ReqDataIn),
    riak_kv_wm_utils:is_forbidden(ReqDataIn, Class, Context);
forbidden(ReqDataIn, #ctx{bucket_type = BT, security = Sec} = Context) ->
    Class = request_class(ReqDataIn),
    {Answer, ReqData, _} = Result =
        riak_kv_wm_utils:is_forbidden(ReqDataIn, Class, Context),
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

-spec request_class(#wm_reqdata{}) -> term().
request_class(ReqData) ->
    case wrq:get_qs_value(?Q_STREAM, ?Q_FALSE, ReqData) of
        ?Q_TRUE ->
            {riak_kv, stream_secondary_index};
        _ ->
            {riak_kv, secondary_index}
    end.

-spec malformed_request(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether query parameters are badly-formed.
%%      Specifically, we check that the index operation is of
%%      a known type.
malformed_request(RD, Ctx) ->
    %% Pull the params...
    Bucket =
        list_to_binary(
            riak_kv_wm_utils:maybe_decode_uri(RD, wrq:path_info(bucket, RD))),
    IndexField =
        list_to_binary(
            riak_kv_wm_utils:maybe_decode_uri(RD, wrq:path_info(field, RD))),
    Args1 = wrq:path_tokens(RD),
    Args2 =
        [list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, X))
            || X <- Args1],
    ReturnTerms0 = wrq:get_qs_value(?Q_2I_RETURNTERMS, "false", RD),
    ReturnTerms1 = normalize_boolean(string:to_lower(ReturnTerms0)),
    Continuation = wrq:get_qs_value(?Q_2I_CONTINUATION, RD),
    PgSort0 = wrq:get_qs_value(?Q_2I_PAGINATION_SORT, RD),
    PgSort =
        case PgSort0 of
            undefined ->
                undefined;
            _ ->
                normalize_boolean(string:to_lower(PgSort0))
        end,
    MaxVal = validate_max(wrq:get_qs_value(?Q_2I_MAX_RESULTS, RD)),
    TermRegex = wrq:get_qs_value(?Q_2I_TERM_REGEX, RD),
    Timeout0 =  wrq:get_qs_value("timeout", RD),
    {Start, End} =
        case {IndexField, Args2} of
            {<<"$bucket">>, _} -> {undefined, undefined};
            {_, []} -> {undefined, undefined};
            {_, [S]} -> {S, S};
            {_, [S, E]} -> {S, E}
        end,
    IsEqualOp = length(Args1) == 1,
    InternalReturnTerms = not( IsEqualOp orelse IndexField == <<"$field">> ),
    Stream0 = wrq:get_qs_value("stream", "false", RD),
    Stream = normalize_boolean(string:to_lower(Stream0)),
    case riak_index:compile_re(TermRegex) of
        {error, ReError} ->
            {true,
                wrq:set_resp_body(
                    io_lib:format(
                        "Invalid term regular expression ~p : ~p",
                        [TermRegex, ReError]
                    ),
                    wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
                Ctx};
        {ok, CompiledRegex} ->
            QueryResult =
                riak_index:to_index_query(
                    [{field, IndexField}, {start_term, Start}, {end_term, End},
                        {max_results, MaxVal},
                        {continuation, Continuation},
                        {return_terms, InternalReturnTerms},
                        {term_regex, CompiledRegex}
                    ]
               ),
            case QueryResult of
                {error, Reason} ->
                    {true,
                        wrq:set_resp_body(
                            io_lib:format(
                                "Invalid query: ~p~n", [Reason]),
                                wrq:set_resp_header(
                                    ?HEAD_CTYPE, "text/plain", RD)),
                        Ctx};
                {ok, Query} ->
                    case riak_index:is_valid_index(Query) of
                        false ->
                            {true,
                                wrq:set_resp_body(
                                    "Can not use term regular expressions"
                                    " on integer queries",
                                    wrq:set_resp_header(
                                        ?HEAD_CTYPE, "text/plain", RD)),
                                Ctx};
                        _ ->
                            ValidationResult =
                                validate_query(
                                    PgSort,
                                    ReturnTerms1,
                                    validate_timeout(Timeout0),
                                    MaxVal,
                                    Stream,
                                    RD
                                ),
                            case ValidationResult of
                                ok ->
                                    PgSortFinal =
                                        case Continuation of
                                            undefined ->
                                                PgSort;
                                            _ ->
                                                true
                                        end,
                                    NewCtx =
                                        Ctx#ctx{
                                            bucket = Bucket,
                                            index_query = Query,
                                            max_results = MaxVal,
                                            return_terms =
                                                riak_index:return_terms(
                                                    ReturnTerms1, Query),
                                            timeout=Timeout0,
                                            pagination_sort = PgSortFinal,
                                            streamed = Stream
                                        },
                                    {false, RD, NewCtx};
                                ErrorMessage ->
                                    {true, ErrorMessage, Ctx}
                            end
                    end
            end
    end.


validate_query({malformed, BadPgSort}, _, _, _, _, RD) ->
    wrq:set_resp_body(
        io_lib:format(
            "Invalid ~p. ~p is not a boolean",
            [?Q_2I_PAGINATION_SORT, BadPgSort]),
            wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD));
validate_query(_, {malformed, BadReturnTerms}, _, _, _, RD) ->
    wrq:set_resp_body(
        io_lib:format(
            "Invalid ~p. ~p is not a boolean",
            [?Q_2I_RETURNTERMS, BadReturnTerms]),
            wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD));
validate_query(_, _, {malformed, BadN}, _, _, RD) ->
    wrq:append_to_resp_body(
        io_lib:format(
            "Bad timeout "
            "value ~p. Must be a non-negative integer~n",
            [BadN]),
            wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD));
validate_query(_, _, _, {malformed, BadMR}, _, RD) ->
    wrq:set_resp_body(
        io_lib:format(
            "Invalid ~p. ~p is not a positive integer",
            [?Q_2I_MAX_RESULTS, BadMR]),
            wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD));
validate_query(_, _, _, _, {malformed, BadStreamReq}, RD) ->
    wrq:set_resp_body(
        io_lib:format(
            "Invalid stream. ~p is not a boolean",
            [BadStreamReq]),
            wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD));
validate_query(_, _, _, _, _, _RD) ->
    %% Request is valid.
    ok.

-spec validate_timeout(
        undefined|string()) ->
            non_neg_integer()|{malformed, string()}.
validate_timeout(undefined) ->
    undefined;
validate_timeout(Str) ->
    try list_to_integer(Str) of
        Int when Int >= 0 ->
            Int;
        Neg ->
            {malformed, Neg}
    catch
        _:_ ->
            {malformed, Str}
    end.

-spec validate_max(
    undefined|string()) -> non_neg_integer()|all|{malformed, string()}.
validate_max(undefined) ->
    all;
validate_max(N) when is_list(N) ->
    try list_to_integer(N) of
        Max when Max > 0  ->
            Max;
        ZeroOrLess ->
            {malformed, ZeroOrLess}
    catch _:_ ->
        {malformed, N}
    end.

-spec normalize_boolean(string()) -> boolean()|{malformed, string()}.
normalize_boolean("false") ->
    false;
normalize_boolean("true") ->
    true;
normalize_boolean(OtherString) ->
    {malformed, OtherString}.

-spec content_types_provided(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Producer::atom()}], #wm_reqdata{}, context()}.
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for bucket lists.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_index_results}], RD, Ctx}.


-spec encodings_provided(#wm_reqdata{}, context()) ->
    {[{Encoding::string(), Producer::function()}], #wm_reqdata{}, context()}.
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for bucket lists.
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.


resource_exists(RD, #ctx{bucket_type=BType}=Ctx) ->
    {riak_kv_wm_utils:bucket_type_exists(BType), RD, Ctx}.

-spec produce_index_results(#wm_reqdata{}, context()) ->
    {binary(), #wm_reqdata{}, context()}.
%% @doc Produce the JSON response to an index lookup.
produce_index_results(RD, Ctx) ->
    case wrq:get_qs_value("stream", "false", RD) of
        "true" ->
            handle_streaming_index_query(RD, Ctx);
        _ ->
            handle_all_in_memory_index_query(RD, Ctx)
    end.

handle_streaming_index_query(RD, Ctx) ->
    Client = Ctx#ctx.client,
    Bucket = riak_kv_wm_utils:maybe_bucket_type(Ctx#ctx.bucket_type, Ctx#ctx.bucket),
    Query = Ctx#ctx.index_query,
    MaxResults = Ctx#ctx.max_results,
    ReturnTerms = Ctx#ctx.return_terms,
    Timeout = Ctx#ctx.timeout,
    PgSort = Ctx#ctx.pagination_sort,

    %% Create a new multipart/mixed boundary
    Boundary = riak_core_util:unique_id_62(),
    CTypeRD = wrq:set_resp_header(
                "Content-Type",
                "multipart/mixed;boundary="++Boundary,
                RD),

    Opts0 = [{max_results, MaxResults}] ++ [{pagination_sort, PgSort} || PgSort /= undefined],
    Opts = riak_index:add_timeout_opt(Timeout, Opts0),

    {ok, ReqID, FSMPid} = 
        riak_client:stream_get_index(Bucket, Query, Opts, Client),
    StreamFun = index_stream_helper(ReqID, FSMPid, Boundary, ReturnTerms, MaxResults, proplists:get_value(timeout, Opts), undefined, 0),
    {{stream, {<<>>, StreamFun}}, CTypeRD, Ctx}.

index_stream_helper(ReqID, FSMPid, Boundary, ReturnTerms, MaxResults, Timeout, LastResult, Count) ->
    fun() ->
            receive
                {ReqID, done} ->
                    Final = case make_continuation(MaxResults, [LastResult], Count) of
                                undefined -> ["\r\n--", Boundary, "--\r\n"];
                                Continuation ->
                                    Json = mochijson2:encode(mochify_continuation(Continuation)),
                                    ["\r\n--", Boundary, "\r\n",
                                     "Content-Type: application/json\r\n\r\n",
                                     Json,
                                     "\r\n--", Boundary, "--\r\n"]
                            end,
                    {iolist_to_binary(Final), done};
                {ReqID, {results, []}} ->
                    {<<>>, index_stream_helper(ReqID, FSMPid, Boundary, ReturnTerms, MaxResults, Timeout, LastResult, Count)};
                {ReqID, {results, Results}} ->
                    %% JSONify the results
                    JsonResults = encode_results(ReturnTerms, Results),
                    Body = ["\r\n--", Boundary, "\r\n",
                            "Content-Type: application/json\r\n\r\n",
                            JsonResults],
                    LastResult1 = last_result(Results),
                    Count1 = Count + length(Results),
                    {iolist_to_binary(Body),
                     index_stream_helper(ReqID, FSMPid, Boundary, ReturnTerms, MaxResults, Timeout, LastResult1, Count1)};
                {ReqID, Error} ->
                    stream_error(Error, Boundary)
            after Timeout ->
                    whack_index_fsm(ReqID, FSMPid),
                    stream_error({error, timeout}, Boundary)
            end
    end.

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

stream_error(Error, Boundary) ->
    ?LOG_ERROR("Error in index wm: ~p", [Error]),
    ErrorJson = encode_error(Error),
    Body = ["\r\n--", Boundary, "\r\n",
            "Content-Type: application/json\r\n\r\n",
            ErrorJson,
            "\r\n--", Boundary, "--\r\n"],
    {iolist_to_binary(Body), done}.

encode_error({error, E}) ->
    encode_error(E);
encode_error(Error) when is_atom(Error); is_binary(Error) ->
    mochijson2:encode({struct, [{error, Error}]});
encode_error(Error) ->
    E = io_lib:format("~p",[Error]),
    mochijson2:encode({struct, [{error, erlang:iolist_to_binary(E)}]}).

handle_all_in_memory_index_query(RD, Ctx) ->
    Client = Ctx#ctx.client,
    Bucket = riak_kv_wm_utils:maybe_bucket_type(Ctx#ctx.bucket_type, Ctx#ctx.bucket),
    Query = Ctx#ctx.index_query,
    Timeout = Ctx#ctx.timeout,

    % Standard secondary index query
    MaxResults = Ctx#ctx.max_results,
    ReturnTerms = Ctx#ctx.return_terms,
    PgSort = Ctx#ctx.pagination_sort,
    
    Opts0 = 
        [{max_results, MaxResults}] ++ 
            [{pagination_sort, PgSort} || PgSort /= undefined],
    Opts = 
        riak_index:add_timeout_opt(Timeout, Opts0),

    %% Do the index lookup...
    case riak_client:get_index(Bucket, Query, Opts, Client) of
        {ok, Results} ->
            Continuation =
                make_continuation(
                    MaxResults, Results, length(Results)),
            JsonResults =
                encode_results(
                    ReturnTerms,  Results, Continuation),
            {JsonResults, RD, Ctx};
        {error, timeout} ->
            {{halt, 503},
            wrq:set_resp_header("Content-Type", "text/plain",
                                wrq:append_to_response_body(
                                io_lib:format("request timed out~n",
                                                []),
                                RD)),
            Ctx};
        {error, Reason} ->
            {{error, Reason}, RD, Ctx}
    end.


%% @doc Like `lists:last/1' but doesn't choke on an empty list
-spec last_result([] | list()) -> term() | undefined.
last_result([]) ->
    undefined;
last_result(L) ->
    lists:last(L).

%% @doc if this is a paginated query make a continuation
-spec make_continuation(Max::non_neg_integer() | undefined,
                        list(),
                        ResultCount::non_neg_integer()) -> binary() | undefined.
make_continuation(MaxResults, Results, MaxResults) ->
    riak_index:make_continuation(Results);
make_continuation(_, _, _)  ->
    undefined.

%% ===================================================================
%% JSON Encoding implementations
%% ===================================================================

mochijson_encode_results(ReturnTerms, Results) ->
    mochijson_encode_results(ReturnTerms, Results, undefined).

mochijson_encode_results(true, Results, Continuation) ->
    JsonKeys2 = {struct, [{?Q_RESULTS, [{struct, [{Val, Key}]} || {Val, Key} <- Results]}] ++
                        mochify_continuation(Continuation)},
    mochijson2:encode(JsonKeys2);
mochijson_encode_results(false, Results, Continuation) ->
    JustTheKeys = filter_values(Results),
    JsonKeys1 = {struct, [{?Q_KEYS, JustTheKeys}] ++ mochify_continuation(Continuation)},
    mochijson2:encode(JsonKeys1).
    
mochify_continuation(undefined) ->
    [];
mochify_continuation(Continuation) ->
    [{?Q_2I_CONTINUATION, Continuation}].

filter_values([]) ->
    [];
filter_values([{_, _} | _T]=Results) ->
    [K || {_V, K} <- Results];
filter_values(Results) ->
    Results.


otp_encode_results(ReturnTerms, Results) ->
    otp_encode_results(ReturnTerms, Results, undefined).

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
    ["{", [Encode(Term, Encode), $: | Encode(Key, Encode)], "}"];
results_encode({Term, Key}, Encode) when is_integer(Term), is_binary(Key) ->
    ["{",
        [Encode(integer_to_binary(Term), Encode), $: | Encode(Key, Encode)],
        "}"];
results_encode(Result, Encode) ->
    riak_kv_wm_json:encode_value(Result, Encode).

keys_encode({_Term, Key}, Encode) when is_binary(Key) ->
    riak_kv_wm_json:encode_value(Key, Encode);
keys_encode(Object, Encode) ->
    riak_kv_wm_json:encode_value(Object, Encode).

encode_results(ReturnTerms, Results) ->
    encode_results(ReturnTerms, Results, undefined).

encode_results(ReturnTerms, Results, Continuation) ->
    Library =
        app_helper:get_env(
            riak_kv, secondary_index_json, ?DEFAULT_JSON_ENCODING),
    case Library of
        mochijson ->
            mochijson_encode_results(ReturnTerms, Results, Continuation);
        otp ->
            otp_encode_results(ReturnTerms, Results, Continuation)
    end.



%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


otp_decode(JsonIOL) ->
    riak_kv_wm_json:decode(iolist_to_binary(JsonIOL)).

compare_encode_test() ->
    Results = large_results(10),
    OTPjsonA = otp_encode_results(true, Results),
    MjsonA = mochijson_encode_results(true, Results),
    ?assert(mochijson2:decode(MjsonA) == mochijson2:decode(OTPjsonA)),
    ?assert(otp_decode(MjsonA) == otp_decode(OTPjsonA)),
    ?assert(iolist_to_binary(MjsonA) == iolist_to_binary(OTPjsonA)),
    Continuation = make_continuation(10, Results, 10),
    OTPjsonB = otp_encode_results(true, Results, Continuation),
    MjsonB = mochijson_encode_results(true, Results, Continuation),
    {struct, MDecodeOTPjsonB} = mochijson2:decode(OTPjsonB),
    {struct, MDecodeMjsonB} = mochijson2:decode(MjsonB),
    ?assert(lists:sort(MDecodeOTPjsonB) == lists:sort(MDecodeMjsonB)),
    OTPjsonC = otp_encode_results(false, Results),
    MjsonC = mochijson_encode_results(false, Results),
    ?assert(mochijson2:decode(MjsonC) == mochijson2:decode(OTPjsonC)),
    OTPjsonD = otp_encode_results(false, Results, Continuation),
    MjsonD = mochijson_encode_results(false, Results, Continuation),
    {struct, MDecodeOTPjsonD} = mochijson2:decode(OTPjsonD),
    {struct, MDecodeMjsonD} = mochijson2:decode(MjsonD),
    ?assert(lists:sort(MDecodeOTPjsonD) == lists:sort(MDecodeMjsonD)),
    IntIdxResults = [{1, <<"K1">>}, {2, <<"K2">>}],
    OTPjsonE = otp_encode_results(true, IntIdxResults),
    MjsonE = mochijson_encode_results(true, IntIdxResults),
    ?assert(mochijson2:decode(MjsonE) == mochijson2:decode(OTPjsonE)),
    ?assert(otp_decode(MjsonE) == otp_decode(OTPjsonE)),
    ?assert(iolist_to_binary(MjsonE) == iolist_to_binary(OTPjsonE))

    .

encoder_test_() ->
    {timeout, 600, fun encode_tester/0}.

encode_tester() ->
    timer:sleep(100), % awkward silence to tidy screen output
    encode_implementation_tester(mochijson2),
    encode_implementation_tester(otp).

encode_implementation_tester(Imp) ->
    garbage_collect(),

    io:format(user, "~n~nTesting Implementation ~w~n", [Imp]),
    ResultSetsTiny =
        [{<<"1K">>, large_results(1000)},
            {<<"2K">>, large_results(2000)},
            {<<"3K">>, large_results(3000)},
            {<<"5K">>, large_results(5000)},
            {<<"8K">>, large_results(8000)}],
    encode_tester(Imp, ResultSetsTiny, microseconds),

    garbage_collect(),

    ResultSetsSmall =
        [{<<"13K">>, large_results(13000)},
            {<<"21K">>, large_results(21000)},
            {<<"34K">>, large_results(34000)},
            {<<"55K">>, large_results(55000)}],
    encode_tester(Imp, ResultSetsSmall, milliseconds),

    garbage_collect(),

    ResultSetsMid =
        [{<<"100K">>, large_results(100000)},
            {<<"200K">>, large_results(200000)},
            {<<"300K">>, large_results(300000)},
            {<<"500K">>, large_results(500000)}],
    encode_tester(Imp, ResultSetsMid, milliseconds),

    % garbage_collect(),
    
    % ResultSetsLarge =
    %     [{<<"1M">>, large_results(1000000)},
    %         {<<"2M">>, large_results(2000000)},
    %         {<<"3M">>, large_results(3000000)},
    %         {<<"5M">>, large_results(5000000)}],
    % encode_tester(Imp, ResultSetsLarge, milliseconds),

    % garbage_collect(),
    % ResultSetsHuge =
    %     [{<<"8M">>, large_results(8000000)}],
    % encode_tester(Imp, ResultSetsHuge, milliseconds),

    % garbage_collect(),
    % ResultSetsSuperSize =
    %     [{<<"13M">>, large_results(13000000)}],
    % encode_tester(Imp, ResultSetsSuperSize, milliseconds),

    ok.

encode_tester(Lib, ResultSets, Unit) ->
    Divisor =
        case Unit of
            microseconds ->
                1;
            milliseconds ->
                1000
        end,
    Fun =
        case Lib of
            mochijson2 ->
                fun mochijson_encode_results/2;
            otp ->
                fun otp_encode_results/2
        end,
    
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

-endif.