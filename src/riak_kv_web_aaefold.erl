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

-module(riak_kv_web_aaefold).

-if(?OTP_RELEASE == 26).
-feature(maybe_expr, enable).
-endif.

-include_lib("kernel/include/logger.hrl").
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

-record(filter, {
    key_range = all :: {binary(), binary()} | all,
    date_range = all :: {date, non_neg_integer(), non_neg_integer()} | all,
    hash_method = pre_hash :: {rehash, non_neg_integer()} | pre_hash,
    segment_filter = all :: segment_list() | all,
    change_method = count :: {job, pos_integer()} | local | count
}).

-record(context, {
    fold_type = {undefined} :: fold_type(),
    filter_expected = true :: boolean(),
    tree_size :: leveled_tictac:tree_size() | undefined,
    repl_queue :: atom() | undefined,
    filter = #filter{} :: filter()
}).

-type segment_list() ::
    {segments, list(pos_integer()), leveled_tictac:tree_size() | n_val}.
-type filter() :: #filter{}.
-type context() :: #context{}.
-type fold_type() ::
    {n_val, root | branch | clocks, pos_integer()}
    | {
        range_action,
        merge_tree | clocks | repl_keys | repair_keys | erase_keys | reap_tombs,
        riak_object:bucket()
    }
    | {
        range_query,
        object_stats | find_tombs,
        riak_object:bucket()
    }
    | {
        find_keys,
        riak_object:bucket(),
        {sibling_count, pos_integer()} | {object_size, pos_integer()}
    }
    | {bucket_list, pos_integer() | undefined}
    | {undefined}.

%% ===================================================================
%% Callback functions
%% ===================================================================

%% Available operations (NOTE: within square brackets means optional):
%% GET /cachedtrees/nvals/NVal/root
%% GET /cachedtrees/nvals/NVal/branch?filter
%% GET /cachedtrees/nvals/NVal/keysclocks?filter
%% GET /rangetrees/[types/Type/]buckets/Bucket/trees/Size?filter
%% GET /rangetrees/[types/Type/]buckets/Bucket/keysclocks?filter
%% GET /rangerepl/[types/Type/]buckets/Bucket/queuename/Queue?filter
%% GET /rangerepair/[types/Type/]buckets/Bucket?filter
%% GET /siblings/[types/Type/]buckets/Bucket/counts/Cnt?filter
%% GET /objectsizes/[types/Type/]buckets/Bucket/sizes/Size?filter
%% GET /objectstats/[types/Type/]buckets/Bucket?filter
%% GET /tombs/[types/Type/]buckets/Bucket?filter
%% GET /reap/[types/Type/]buckets/Bucket?filter
%% GET /erase/[types/Type/]buckets/Bucket?filter
%% GET /aaebucketlist?nval

-spec match_route(
    riak_api_web_acceptor:method(),
    unicode:chardata(),
    list(unicode:chardata())
) ->
    nomatch
    | {method_not_allowed, list(riak_api_web_acceptor:method())}
    | {ok, riak_api_web_handler:limits(), context()}.
match_route(Method, _Path, [<<"cachedtrees">>, <<"nvals">>, NVal, Type]) ->
    case check_integer(NVal) of
        NValInt when is_integer(NValInt), NValInt > 0 ->
            case Type of
                <<"root">> ->
                    only_get(
                        #context{
                            fold_type = {n_val, root, NValInt},
                            filter_expected = false
                        },
                        Method
                    );
                <<"branch">> ->
                    only_get(
                        #context{fold_type = {n_val, branch, NValInt}},
                        Method
                    );
                <<"keysclocks">> ->
                    only_get(
                        #context{fold_type = {n_val, clocks, NValInt}},
                        Method
                    );
                _ ->
                    nomatch
            end;
        _NotValidInt ->
            nomatch
    end;
match_route(Method, Path, [<<"rangetrees">> | Rest]) ->
    case Rest of
        [<<"buckets">> | _] ->
            match_route(
                Method,
                Path,
                [<<"rangetrees">>, <<"types">>, <<"default">>] ++ Rest
            );
        [<<"types">>, Type, <<"buckets">>, Bucket, <<"trees">>, Size] ->
            case check_size(Size) of
                invalid ->
                    nomatch;
                {valid, ValidSize} ->
                    FoldType =
                        {
                            range_action,
                            merge_tree,
                            riak_kv_web_common:set_bucket(Type, Bucket)
                        },
                    only_get(
                        #context{fold_type = FoldType, tree_size = ValidSize},
                        Method
                    )
            end;
        [<<"types">>, Type, <<"buckets">>, Bucket, <<"keysclocks">>] ->
            FoldType =
                {
                    range_action,
                    clocks,
                    riak_kv_web_common:set_bucket(Type, Bucket)
                },
            only_get(
                #context{fold_type = FoldType},
                Method
            );
        _ ->
            nomatch
    end;
match_route(Method, Path, [<<"rangerepl">> | Rest]) ->
    case Rest of
        [<<"buckets">> | _] ->
            match_route(
                Method,
                Path,
                [<<"rangerepl">>, <<"types">>, <<"default">>] ++ Rest
            );
        [<<"types">>, Type, <<"buckets">>, Bucket, <<"queuename">>, Queue] ->
            BT = riak_kv_web_common:set_bucket(Type, Bucket),
            only_get(
                #context{
                    fold_type = {range_action, repl_keys, BT},
                    repl_queue = check_queuename(Queue)
                },
                Method
            );
        _ ->
            nomatch
    end;
match_route(Method, Path, [<<"rangerepair">> | Rest]) ->
    case Rest of
        [<<"buckets">> | _] ->
            match_route(
                Method,
                Path,
                [<<"rangerepair">>, <<"types">>, <<"default">>] ++ Rest
            );
        [<<"types">>, Type, <<"buckets">>, Bucket] ->
            BT = riak_kv_web_common:set_bucket(Type, Bucket),
            only_get(
                #context{fold_type = {range_action, repair_keys, BT}},
                Method
            );
        _ ->
            nomatch
    end;
match_route(Method, Path, [<<"siblings">> | Rest]) ->
    case Rest of
        [<<"buckets">> | _] ->
            match_route(
                Method,
                Path,
                [<<"siblings">>, <<"types">>, <<"default">>] ++ Rest
            );
        [<<"types">>, Type, <<"buckets">>, Bucket, <<"counts">>, Count] ->
            BT = riak_kv_web_common:set_bucket(Type, Bucket),
            case check_integer(Count) of
                Int when is_integer(Int), Int > 0 ->
                    only_get(
                        #context{
                            fold_type = {find_keys, BT, {sibling_count, Int}}
                        },
                        Method
                    );
                _ ->
                    nomatch
            end;
        _ ->
            nomatch
    end;
match_route(Method, Path, [<<"objectstats">> | Rest]) ->
    case Rest of
        [<<"buckets">> | _] ->
            match_route(
                Method,
                Path,
                [<<"objectstats">>, <<"types">>, <<"default">>] ++ Rest
            );
        [<<"types">>, Type, <<"buckets">>, Bucket] ->
            BT = riak_kv_web_common:set_bucket(Type, Bucket),
            only_get(
                #context{fold_type = {range_query, object_stats, BT}},
                Method
            );
        _ ->
            nomatch
    end;
match_route(Method, Path, [<<"objectsizes">> | Rest]) ->
    case Rest of
        [<<"buckets">> | _] ->
            match_route(
                Method,
                Path,
                [<<"objectsizes">>, <<"types">>, <<"default">>] ++ Rest
            );
        [<<"types">>, Type, <<"buckets">>, Bucket, <<"sizes">>, Size] ->
            BT = riak_kv_web_common:set_bucket(Type, Bucket),
            case check_integer(Size) of
                Int when is_integer(Int), Int > 0 ->
                    only_get(
                        #context{
                            fold_type = {find_keys, BT, {object_size, Int}}
                        },
                        Method
                    );
                _ ->
                    nomatch
            end;
        _ ->
            nomatch
    end;
match_route(Method, Path, [<<"tombs">> | Rest]) ->
    case Rest of
        [<<"buckets">> | _] ->
            match_route(
                Method,
                Path,
                [<<"tombs">>, <<"types">>, <<"default">>] ++ Rest
            );
        [<<"types">>, Type, <<"buckets">>, Bucket] ->
            BT = riak_kv_web_common:set_bucket(Type, Bucket),
            only_get(
                #context{fold_type = {range_query, find_tombs, BT}},
                Method
            );
        _ ->
            nomatch
    end;
match_route(Method, Path, [<<"reap">> | Rest]) ->
    case Rest of
        [<<"buckets">> | _] ->
            match_route(
                Method,
                Path,
                [<<"reap">>, <<"types">>, <<"default">>] ++ Rest
            );
        [<<"types">>, Type, <<"buckets">>, Bucket] ->
            BT = riak_kv_web_common:set_bucket(Type, Bucket),
            only_get(
                #context{fold_type = {range_action, reap_tombs, BT}},
                Method
            );
        _ ->
            nomatch
    end;
match_route(Method, Path, [<<"erase">> | Rest]) ->
    case Rest of
        [<<"buckets">> | _] ->
            match_route(
                Method,
                Path,
                [<<"erase">>, <<"types">>, <<"default">>] ++ Rest
            );
        [<<"types">>, Type, <<"buckets">>, Bucket] ->
            BT = riak_kv_web_common:set_bucket(Type, Bucket),
            only_get(
                #context{fold_type = {range_action, erase_keys, BT}},
                Method
            );
        _ ->
            nomatch
    end;
match_route(Method, _Path, [<<"aaebucketlist">>]) ->
    only_get(
        #context{
            fold_type = {bucket_list, undefined},
            filter_expected = false
        },
        Method
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
            undefined,
            undefined
        ),
    case Check of
        true ->
            {ok, Ctx};
        HaltResponse ->
            HaltResponse
    end.

%% @doc parse and validate query params, passed as a map
-spec parse_query_params(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
parse_query_params(Params, Ctx = #context{filter_expected = true}) ->
    case lists:keyfind(<<"filter">>, 1, Params) of
        {<<"filter">>, B64FilterJson} when is_binary(B64FilterJson) ->
            MaybeSegList = element(1, Ctx#context.fold_type) == n_val,
            case validate_range_filter(B64FilterJson, MaybeSegList) of
                {valid, Filter} ->
                    {ok, Ctx#context{filter = Filter}};
                {invalid, ErrMsg} ->
                    {halt, 400, [?TXT_HEADER], ErrMsg, []}
            end;
        _UseDefault ->
            {ok, Ctx}
    end;
parse_query_params(Params, Ctx) ->
    case {Ctx#context.fold_type, lists:keyfind(<<"nval">>, 1, Params)} of
        {{bucket_list, undefined}, {<<"nval">>, NVal}} ->
            case check_integer(NVal) of
                Int when is_integer(Int), Int > 0 ->
                    {ok, Ctx#context{fold_type = {bucket_list, Int}}};
                _NotInt ->
                    ErrMsg = <<"Invalid nval in query params ~0p">>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, [NVal]}
            end;
        {{bucket_list, undefined}, false} ->
            ErrMsg = <<"Missing nval in query params">>,
            {halt, 400, [?TXT_HEADER], ErrMsg, []};
        _ ->
            {ok, Ctx}
    end.

%% @doc parse and validate the request headers
-spec parse_request_headers(
    riak_api_web_headers:headers(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
parse_request_headers(_ReqHeaders, Ctx) ->
    {ok, Ctx}.

%% @doc Process the request and produce a response
-spec process_request(
    none,
    context()
) ->
    {
        ok,
        {
            riak_api_web_acceptor:response_code(),
            riak_api_web_headers:header_list(),
            riak_api_web_handler:response_body(),
            boolean(),
            none
        },
        context()
    }
    | riak_api_web_acceptor:halt_response().
process_request(none, Ctx) ->
    Query = convert_to_query(Ctx),
    case Query of
        invalid ->
            {halt, 400, [?TXT_HEADER], <<"Invalid query definition">>, []};
        _Valid ->
            case riak_client:aae_fold(Query) of
                {ok, Results} ->
                    QueryName = element(1, Query),
                    JsonResults =
                        riak_kv_clusteraae_fsm:json_encode_results(
                            QueryName,
                            Results
                        ),
                    {ok, {200, [?JSN_HEADER], JsonResults, true, none}, Ctx};
                {error, timeout} ->
                    {halt, 503, [?TXT_HEADER], <<"Request timed out">>, []};
                {error, Reason} ->
                    ErrMsg = <<"Fold failure due to ~0p">>,
                    {halt, 500, [?TXT_HEADER], ErrMsg, [Reason]}
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
%% Validation functions
%% ===================================================================

-type filter_field() ::
    segment_filter | key_range | date_range | hash_iv | change_method.

-spec validate_range_filter(
    binary(),
    boolean()
) ->
    {valid, filter()} | {invalid, binary()}.
validate_range_filter(B64JsonB, MaybeSegList) when is_binary(B64JsonB) ->
    try
        case riak_kv_wm_json:decode(base64:decode(B64JsonB)) of
            Filter when is_map(Filter) ->
                validate_range_filter(
                    Filter,
                    [
                        segment_filter,
                        key_range,
                        date_range,
                        hash_iv,
                        change_method
                    ],
                    #filter{}
                );
            Filter when is_list(Filter), MaybeSegList ->
                true = check_seglist(Filter),
                {valid, #filter{segment_filter = {segments, Filter, n_val}}}
        end
    catch
        _:Error ->
            ?LOG_WARNING("Issure decoding filter ~0p", [Error]),
            {invalid, <<"Exception decoding filter">>}
    end.

-spec validate_range_filter(
    map(), list(filter_field()), filter()
) ->
    {valid, filter()} | {invalid, binary()}.
validate_range_filter(_FilterJson, [] = _Fields, Filter) ->
    {valid, Filter};
validate_range_filter(FilterJson, [Field | Fields], Filter0) ->
    FieldVal = maps:get(atom_to_binary(Field), FilterJson, undefined),
    case validate_field(Field, FieldVal, Filter0) of
        {valid, Filter} ->
            validate_range_filter(FilterJson, Fields, Filter);
        {invalid, Reason} when is_binary(Reason) ->
            {invalid, Reason}
    end.

-spec validate_field(
    filter_field(), any(), filter()
) ->
    {valid, filter()} | {invalid, binary()}.

validate_field(segment_filter, <<"all">>, Filter) ->
    {valid, Filter};
validate_field(segment_filter, undefined, Filter) ->
    {valid, Filter};
validate_field(segment_filter, SegF, Filter) when is_map(SegF) ->
    ValidTreeSize =
        case maps:get(<<"tree_size">>, SegF, undefined) of
            TreeSize when is_binary(TreeSize) ->
                case check_size(TreeSize) of
                    invalid ->
                        {invalid, <<"Segment filter has invalid tree_size">>};
                    {valid, ValidSize} ->
                        {valid, ValidSize}
                end;
            undefined ->
                {invalid, <<"Segment filter has no tree_size">>}
        end,
    ValidSegList =
        case maps:get(<<"segments">>, SegF, undefined) of
            SegList when is_list(SegList) ->
                case check_seglist(SegList) of
                    true ->
                        {valid, SegList};
                    _ ->
                        {invalid, <<"Segment filter non-integer segment">>}
                end;
            undefined ->
                {invalid, <<"Segment filter has no segment list">>};
            _ ->
                {invalid, <<"Segment filter has invalid segment list">>}
        end,
    case {ValidTreeSize, ValidSegList} of
        {{valid, VTS}, {valid, VSL}} ->
            {valid, Filter#filter{segment_filter = {segments, VSL, VTS}}};
        {{invalid, ITS}, _} ->
            {invalid, ITS};
        {_, {invalid, ISL}} ->
            {invalid, ISL}
    end;
validate_field(segment_filter, _Other, _Filter) ->
    {invalid, <<"Segment filter is badly formed">>};
validate_field(key_range, <<"all">>, Filter) ->
    {valid, Filter};
validate_field(key_range, undefined, Filter) ->
    {valid, Filter};
validate_field(key_range, KeyRange, Filter) when is_map(KeyRange) ->
    Start = maps:get(<<"start">>, KeyRange, undefined),
    End = maps:get(<<"end">>, KeyRange, undefined),
    case {Start, End} of
        {Start, End} when is_binary(Start), is_binary(End), End >= Start ->
            {valid, Filter#filter{key_range = {Start, End}}};
        _Other ->
            {
                invalid,
                <<"Key range does not contain both a valid start and end">>
            }
    end;
validate_field(key_range, _Other, _Filter) ->
    {invalid, <<"Key range is badly formed">>};
validate_field(date_range, <<"all">>, Filter) ->
    {valid, Filter};
validate_field(date_range, undefined, Filter) ->
    {valid, Filter};
validate_field(date_range, DateRange, Filter) when is_map(DateRange) ->
    Start = maps:get(<<"start">>, DateRange, undefined),
    End = maps:get(<<"end">>, DateRange, undefined),
    case {Start, End} of
        {Start, End} when
            is_integer(Start),
            Start >= 0,
            is_integer(End),
            End >= 0,
            End >= Start
        ->
            {valid, Filter#filter{date_range = {date, Start, End}}};
        _Other ->
            {
                invalid,
                <<"Date range does not contain both a valid start and end">>
            }
    end;
validate_field(date_range, _Other, _Filter) ->
    {invalid, <<"Date range is badly formed">>};
validate_field(hash_iv, undefined, Filter) ->
    {valid, Filter};
validate_field(hash_iv, <<"pre_hash">>, Filter) ->
    {valid, Filter};
validate_field(hash_iv, IV, Filter) when is_integer(IV) andalso IV > -1 ->
    {valid, Filter#filter{hash_method = {rehash, IV}}};
validate_field(hash_iv, _Other, _Filter) ->
    {invalid, <<"Hash initialisation vector not an integer">>};
validate_field(change_method, undefined, Filter) ->
    {valid, Filter};
validate_field(change_method, <<"count">>, Filter) ->
    {valid, Filter#filter{change_method = count}};
validate_field(change_method, <<"local">>, Filter) ->
    {valid, Filter#filter{change_method = local}};
validate_field(change_method, #{<<"job_id">> := JobID}, Filter) when
    is_integer(JobID)
->
    {valid, Filter#filter{change_method = {job, JobID}}};
validate_field(change_method, _Other, _Filter) ->
    {invalid, <<"Change method is badly formed">>}.

-spec check_integer(binary()) -> integer() | false.
check_integer(Bin) ->
    try
        binary_to_integer(Bin)
    catch
        _:_ ->
            false
    end.

-spec check_size(binary()) -> {valid, leveled_tictac:tree_size()} | invalid.
check_size(TreeSize) ->
    try
        TS = binary_to_existing_atom(TreeSize),
        true = leveled_tictac:valid_size(TS),
        {valid, TS}
    catch
        _:_ ->
            invalid
    end.

-spec check_seglist(list()) -> boolean().
check_seglist(SegList) ->
    IntMembers =
        lists:filter(
            fun(I) -> is_integer(I) andalso I >= 0 end,
            SegList
        ),
    length(IntMembers) == length(SegList).

-spec check_queuename(binary()) -> atom().
check_queuename(Queue) ->
    try
        binary_to_existing_atom(Queue)
    catch
        _:_ ->
            undefined
    end.

%% ===================================================================
%% Internal Functions
%% ===================================================================

-spec only_get(
    context(),
    riak_api_web_acceptor:method()
) ->
    {method_not_allowed, list(riak_api_web_acceptor:method())}
    | {ok, riak_api_web_handler:limits(), context()}.
only_get(Context, 'GET') ->
    {ok, size_limits(), Context};
only_get(_Context, _Method) ->
    {method_not_allowed, ['GET']}.

size_limits() ->
    {
        16,
        1024,
        0
    }.

%% ===================================================================
%% Compose query
%% ===================================================================

-spec convert_to_query(
    context()
) ->
    invalid | riak_kv_clusteraae_fsm:query_definition().
convert_to_query(Ctx) ->
    case Ctx#context.fold_type of
        {n_val, root, N} ->
            {merge_root_nval, N};
        {n_val, branch, N} ->
            case (Ctx#context.filter)#filter.segment_filter of
                {segments, SL, n_val} when length(SL) > 0 ->
                    {merge_branch_nval, N, SL};
                _ ->
                    invalid
            end;
        {n_val, clocks, N} ->
            case (Ctx#context.filter)#filter.segment_filter of
                {segments, SL, _} when length(SL) > 0 ->
                    case (Ctx#context.filter)#filter.date_range of
                        {date, Start, End} ->
                            {fetch_clocks_nval, N, SL, {date, Start, End}};
                        _ ->
                            {fetch_clocks_nval, N, SL}
                    end;
                _ ->
                    invalid
            end;
        {range_action, merge_tree, B} ->
            {
                merge_tree_range,
                B,
                (Ctx#context.filter)#filter.key_range,
                Ctx#context.tree_size,
                (Ctx#context.filter)#filter.segment_filter,
                (Ctx#context.filter)#filter.date_range,
                (Ctx#context.filter)#filter.hash_method
            };
        {range_action, clocks, B} ->
            {
                fetch_clocks_range,
                B,
                (Ctx#context.filter)#filter.key_range,
                (Ctx#context.filter)#filter.segment_filter,
                (Ctx#context.filter)#filter.date_range
            };
        {range_action, repl_keys, B} ->
            {
                repl_keys_range,
                B,
                (Ctx#context.filter)#filter.key_range,
                (Ctx#context.filter)#filter.date_range,
                Ctx#context.repl_queue
            };
        {range_action, repair_keys, B} ->
            {
                repair_keys_range,
                B,
                (Ctx#context.filter)#filter.key_range,
                (Ctx#context.filter)#filter.date_range,
                all
            };
        {range_action, DelAction, B} when
            DelAction == erase_keys; DelAction == reap_tombs
        ->
            {
                DelAction,
                B,
                (Ctx#context.filter)#filter.key_range,
                (Ctx#context.filter)#filter.segment_filter,
                (Ctx#context.filter)#filter.date_range,
                (Ctx#context.filter)#filter.change_method
            };
        {range_query, object_stats, B} ->
            {
                object_stats,
                B,
                (Ctx#context.filter)#filter.key_range,
                (Ctx#context.filter)#filter.date_range
            };
        {range_query, find_tombs, B} ->
            {
                find_tombs,
                B,
                (Ctx#context.filter)#filter.key_range,
                (Ctx#context.filter)#filter.segment_filter,
                (Ctx#context.filter)#filter.date_range
            };
        {find_keys, B, FindType} ->
            {
                find_keys,
                B,
                (Ctx#context.filter)#filter.key_range,
                (Ctx#context.filter)#filter.date_range,
                FindType
            };
        {bucket_list, N} when is_integer(N), N > 0 ->
            {list_buckets, N};
        _ ->
            invalid
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

check_valid_routes_test() ->
    R1 = [<<"cachedtrees">>, <<"nvals">>, <<"3">>, <<"root">>],
    {ok, _, Ctx1} = match_route('GET', <<>>, R1),
    ?assertMatch({n_val, root, 3}, Ctx1#context.fold_type),
    R2 = [<<"cachedtrees">>, <<"nvals">>, <<"5">>, <<"branch">>],
    {ok, _, Ctx2} = match_route('GET', <<>>, R2),
    ?assertMatch({n_val, branch, 5}, Ctx2#context.fold_type),
    R3 = [<<"cachedtrees">>, <<"nvals">>, <<"3">>, <<"keysclocks">>],
    {ok, _, Ctx3} = match_route('GET', <<>>, R3),
    ?assertMatch({n_val, clocks, 3}, Ctx3#context.fold_type),
    R4 = [<<"rangetrees">>, <<"buckets">>, <<"B">>, <<"trees">>, <<"large">>],
    {ok, _, Ctx4} = match_route('GET', <<>>, R4),
    ?assertMatch({range_action, merge_tree, <<"B">>}, Ctx4#context.fold_type),
    ?assertMatch(large, Ctx4#context.tree_size),
    R5 =
        [
            <<"rangetrees">>,
            <<"types">>,
            <<"T">>,
            <<"buckets">>,
            <<"B">>,
            <<"trees">>,
            <<"small">>
        ],
    {ok, _, Ctx5} = match_route('GET', <<>>, R5),
    ?assertMatch(
        {range_action, merge_tree, {<<"T">>, <<"B">>}},
        Ctx5#context.fold_type
    ),
    ?assertMatch(small, Ctx5#context.tree_size),
    R6 = [<<"rangetrees">>, <<"buckets">>, <<"B">>, <<"keysclocks">>],
    {ok, _, Ctx6} = match_route('GET', <<>>, R6),
    ?assertMatch({range_action, clocks, <<"B">>}, Ctx6#context.fold_type),
    R7 =
        [<<"rangerepl">>, <<"buckets">>, <<"B">>, <<"queuename">>, <<"test">>],
    {ok, _, Ctx7} = match_route('GET', <<>>, R7),
    ?assertMatch({range_action, repl_keys, <<"B">>}, Ctx7#context.fold_type),
    R8 = [<<"rangerepair">>, <<"buckets">>, <<"B">>],
    {ok, _, Ctx8} = match_route('GET', <<>>, R8),
    ?assertMatch({range_action, repair_keys, <<"B">>}, Ctx8#context.fold_type),
    R9 = [<<"siblings">>, <<"buckets">>, <<"B">>, <<"counts">>, <<"2">>],
    {ok, _, Ctx9} = match_route('GET', <<>>, R9),
    ?assertMatch(
        {find_keys, <<"B">>, {sibling_count, 2}},
        Ctx9#context.fold_type
    ),
    R10 =
        [<<"objectsizes">>, <<"buckets">>, <<"B">>, <<"sizes">>, <<"10000">>],
    {ok, _, Ctx10} = match_route('GET', <<>>, R10),
    ?assertMatch(
        {find_keys, <<"B">>, {object_size, 10000}},
        Ctx10#context.fold_type
    ),
    R11 = [<<"tombs">>, <<"buckets">>, <<"B">>],
    {ok, _, Ctx11} = match_route('GET', <<>>, R11),
    ?assertMatch({range_query, find_tombs, <<"B">>}, Ctx11#context.fold_type),
    R12 = [<<"reap">>, <<"buckets">>, <<"B">>],
    {ok, _, Ctx12} = match_route('GET', <<>>, R12),
    ?assertMatch({range_action, reap_tombs, <<"B">>}, Ctx12#context.fold_type),
    R13 = [<<"erase">>, <<"buckets">>, <<"B">>],
    {ok, _, Ctx13} = match_route('GET', <<>>, R13),
    ?assertMatch({range_action, erase_keys, <<"B">>}, Ctx13#context.fold_type),
    R14 = [<<"aaebucketlist">>],
    {ok, _, Ctx14} = match_route('GET', <<>>, R14),
    ?assertMatch({bucket_list, undefined}, Ctx14#context.fold_type).

nomatch_test() ->
    nomatch([<<"bucket_list">>]),
    nomatch([<<"cachedtrees">>, <<"nvals">>, <<"0">>, <<"root">>]),
    nomatch([<<"cachedtrees">>, <<"nvals">>, <<"A">>, <<"branch">>]),
    nomatch([<<"cachedtrees">>, <<"nvals">>, <<"3">>, <<"tree">>]),
    nomatch([<<"siblings">>, <<"buckets">>, <<"B">>, <<"counts">>, <<"0">>]),
    nomatch([<<"siblings">>, <<"buckets">>, <<"B">>, <<"counts">>, <<"A">>]),
    nomatch([<<"objectstats">>, <<"counts">>]),
    nomatch([<<"objectsizes">>, <<"buckets">>, <<"B">>, <<"sizes">>, <<"0">>]),
    nomatch([<<"objectsizes">>, <<"buckets">>, <<"B">>, <<"sizes">>, <<"A">>]),
    nomatch(
        [<<"objectsizes">>, <<"buckets">>, <<"B">>, <<"counts">>, <<"1000">>]
    ),
    nomatch([<<"rangetrees">>, <<"buckets">>, <<"B">>, <<"trees">>, <<"xl">>]),
    nomatch([<<"rangetrees">>, <<"buckets">>, <<"B">>, <<"t">>, <<"large">>]),
    nomatch([<<"siblings">>, <<"buckets">>, <<"B">>, <<"sizes">>, <<"1">>]),
    nomatch([<<"reap">>, <<"tipes">>, <<"T">>, <<"buckets">>, <<"B">>]),
    nomatch([<<"tombs">>, <<"tipes">>, <<"T">>, <<"buckets">>, <<"B">>]),
    nomatch([<<"erase">>, <<"types">>, <<"T">>, <<"bickets">>, <<"B">>]),
    nomatch([<<"rangerepair">>]),
    nomatch([<<"rangerepl">>, <<"B">>]),
    nomatch([]).

notallowed_test() ->
    notallowed([<<"aaebucketlist">>]),
    notallowed([<<"tombs">>, <<"types">>, <<"T">>, <<"buckets">>, <<"B">>]).

nomatch(SP) ->
    ?assertMatch(nomatch, match_route('GET', <<>>, SP)).

notallowed(SP) ->
    ?assertMatch({method_not_allowed, ['GET']}, match_route('PUT', <<>>, SP)).

valid_filter() ->
    #{
        <<"segment_filter">> =>
            #{
                <<"tree_size">> => <<"large">>,
                <<"segments">> => [1, 2, 3, 5]
            },
        <<"key_range">> =>
            #{
                <<"start">> => <<"Key00001">>,
                <<"end">> => <<"Key00099">>
            },
        <<"date_range">> =>
            #{
                <<"start">> => 300000,
                <<"end">> => 400000
            },
        <<"hash_iv">> => <<"pre_hash">>,
        <<"change_method">> => <<"count">>
    }.

check_valid_filter_test() ->
    AssertionFun =
        fun(Ctx) ->
            ?assertMatch(
                {segments, [1, 2, 3, 5], large},
                (Ctx#context.filter)#filter.segment_filter
            ),
            ?assertMatch(
                {date, 300000, 400000},
                (Ctx#context.filter)#filter.date_range
            ),
            ?assertMatch(
                {<<"Key00001">>, <<"Key00099">>},
                (Ctx#context.filter)#filter.key_range
            ),
            ?assertMatch(
                pre_hash,
                (Ctx#context.filter)#filter.hash_method
            ),
            ?assertMatch(
                count,
                (Ctx#context.filter)#filter.change_method
            )
        end,
    check_valid_filter_tester(valid_filter(), AssertionFun),
    Filter1 = maps:remove(change_method, valid_filter()),
    %% Change method is default so erasing it should change nothing
    check_valid_filter_tester(Filter1, AssertionFun),
    Filter2 = maps:put(change_method, <<"local">>, Filter1),
    check_valid_filter_tester(
        Filter2,
        fun(Ctx) ->
            ?assertMatch(
                local,
                (Ctx#context.filter)#filter.change_method
            )
        end
    ),
    Filter3 = maps:put(change_method, #{<<"job_id">> => 1}, Filter1),
    check_valid_filter_tester(
        Filter3,
        fun(Ctx) ->
            ?assertMatch(
                {job, 1},
                (Ctx#context.filter)#filter.change_method
            )
        end
    ),
    Filter4 = maps:put(hash_iv, 99999, Filter1),
    check_valid_filter_tester(
        Filter4,
        fun(Ctx) ->
            ?assertMatch(
                {rehash, 99999},
                (Ctx#context.filter)#filter.hash_method
            )
        end
    ).

empty_filter_test() ->
    check_valid_filter_tester(
        #{},
        fun(Ctx) ->
            ?assertMatch(#filter{}, Ctx#context.filter)
        end
    ).

filter_all_test() ->
    AllFilter =
        #{
            <<"segment_filter">> => <<"all">>,
            <<"key_range">> => <<"all">>,
            <<"date_range">> => all
        },
    AssertionFun =
        fun(Ctx) ->
            ?assertMatch(all, (Ctx#context.filter)#filter.segment_filter),
            ?assertMatch(all, (Ctx#context.filter)#filter.date_range),
            ?assertMatch(all, (Ctx#context.filter)#filter.key_range)
        end,
    check_valid_filter_tester(AllFilter, AssertionFun).

invalid_segment_filter_test() ->
    InvalidTreeSize =
        maps:put(
            <<"segment_filter">>,
            maps:put(
                <<"tree_size">>,
                <<"supersize">>,
                maps:get(<<"segment_filter">>, valid_filter())
            ),
            valid_filter()
        ),
    check_invalid_filter_tester(
        InvalidTreeSize,
        <<"Segment filter has invalid tree_size">>
    ),
    InvalidSegmentList = <<"all">>,
    InvalidSegment = [1, 2, 3, 5, <<"a">>],
    SetISFun =
        fun(ISL) ->
            maps:put(
                <<"segment_filter">>,
                maps:put(
                    <<"segments">>,
                    ISL,
                    maps:get(<<"segment_filter">>, valid_filter())
                ),
                valid_filter()
            )
        end,
    check_invalid_filter_tester(
        SetISFun(InvalidSegmentList),
        <<"Segment filter has invalid segment list">>
    ),
    check_invalid_filter_tester(
        SetISFun(InvalidSegment),
        <<"Segment filter non-integer segment">>
    ),
    DelISFun =
        fun(Key) ->
            maps:put(
                <<"segment_filter">>,
                maps:remove(
                    Key,
                    maps:get(<<"segment_filter">>, valid_filter())
                ),
                valid_filter()
            )
        end,
    check_invalid_filter_tester(
        DelISFun(<<"segments">>),
        <<"Segment filter has no segment list">>
    ),
    check_invalid_filter_tester(
        DelISFun(<<"tree_size">>),
        <<"Segment filter has no tree_size">>
    ).

invalid_date_range_test() ->
    InvalidStartDate =
        maps:put(
            <<"date_range">>,
            maps:put(
                <<"start">>,
                <<"today">>,
                maps:get(<<"date_range">>, valid_filter())
            ),
            valid_filter()
        ),
    NoStartDate =
        maps:put(
            <<"date_range">>,
            maps:remove(
                <<"start">>,
                maps:get(<<"date_range">>, valid_filter())
            ),
            valid_filter()
        ),
    check_invalid_filter_tester(
        InvalidStartDate,
        <<"Date range does not contain both a valid start and end">>
    ),
    check_invalid_filter_tester(
        NoStartDate,
        <<"Date range does not contain both a valid start and end">>
    ).

invalid_key_range_test() ->
    InvalidEndKey =
        maps:put(
            <<"key_range">>,
            #{<<"start">> => <<"K00001">>, <<"end">> => <<"K00000">>},
            valid_filter()
        ),
    check_invalid_filter_tester(
        InvalidEndKey,
        <<"Key range does not contain both a valid start and end">>
    ).

check_valid_filter_tester(Filter, AssertionFun) ->
    B64Json = base64:encode(iolist_to_binary(riak_kv_wm_json:encode(Filter))),
    QPS = iolist_to_binary(io_lib:format(<<"filter=~s">>, [B64Json])),
    QPD = uri_string:dissect_query(QPS),
    {ok, Ctx} = parse_query_params(QPD, #context{filter_expected = true}),
    AssertionFun(Ctx).

check_invalid_filter_tester(Filter, ExpectedErrMsg) ->
    B64Json = base64:encode(iolist_to_binary(riak_kv_wm_json:encode(Filter))),
    QPS = iolist_to_binary(io_lib:format(<<"filter=~s">>, [B64Json])),
    QPD = uri_string:dissect_query(QPS),
    {halt, 400, [?TXT_HEADER], HaltMsg, []} =
        parse_query_params(QPD, #context{filter_expected = true}),
    ?assertMatch(ExpectedErrMsg, HaltMsg).

decode_error_test() ->
    B64Json =
        base64:encode(
            iolist_to_binary(riak_kv_wm_json:encode(valid_filter())),
            #{mode => urlsafe, padding => false}
        ),
    QPS1 = iolist_to_binary(io_lib:format(<<"filter=~s">>, [B64Json])),
    QPD1 = uri_string:dissect_query(QPS1),
    {halt, 400, [?TXT_HEADER], HaltMsg1, []} =
        parse_query_params(QPD1, #context{filter_expected = true}),
    ?assertMatch(
        <<"Exception decoding filter">>,
        HaltMsg1
    ),
    BadJson = base64:encode(<<"[Key, Value]">>),
    QPS2 = iolist_to_binary(io_lib:format(<<"filter=~s">>, [BadJson])),
    QPD2 = uri_string:dissect_query(QPS2),
    {halt, 400, [?TXT_HEADER], HaltMsg2, []} =
        parse_query_params(QPD2, #context{filter_expected = true}),
    io:format("~0p~n", [HaltMsg2]),
    ?assertMatch(
        <<"Exception decoding filter">>,
        HaltMsg2
    ).

ignore_filter_if_unexpected_test() ->
    Filter = valid_filter(),
    EmptyFilter = #filter{},
    AssertionFun =
        fun(C) -> ?assertNotMatch(EmptyFilter, C#context.filter) end,
    check_valid_filter_tester(Filter, AssertionFun),
    B64Json = base64:encode(iolist_to_binary(riak_kv_wm_json:encode(Filter))),
    QPS = iolist_to_binary(io_lib:format(<<"filter=~s">>, [B64Json])),
    QPD = uri_string:dissect_query(QPS),
    {ok, Ctx} = parse_query_params(QPD, #context{filter_expected = false}),
    ?assertMatch(EmptyFilter, Ctx#context.filter),
    QPE = uri_string:dissect_query(<<>>),
    {ok, CtxE} = parse_query_params(QPE, #context{filter_expected = true}),
    ?assertMatch(EmptyFilter, CtxE#context.filter).

bucket_list_qparam_test() ->
    {ok, _, Ctx} = match_route('GET', <<>>, [<<"aaebucketlist">>]),
    {halt, 400, _, <<"Invalid nval in query params ~0p">>, [<<"A">>]} =
        parse_query_params(uri_string:dissect_query(<<"nval=A">>), Ctx),
    {halt, 400, _, <<"Missing nval in query params">>, []} =
        parse_query_params(uri_string:dissect_query(<<"filter=A">>), Ctx),
    {ok, Ctx1} =
        parse_query_params(uri_string:dissect_query(<<"nval=3">>), Ctx),
    ?assertMatch({bucket_list, 3}, Ctx1#context.fold_type).

segment_list_on_branch_test() ->
    {ok, _, Ctx} =
        match_route(
            'GET',
            <<>>,
            [<<"cachedtrees">>, <<"nvals">>, <<"3">>, <<"branch">>]
        ),
    F = <<<<"filter=">>/binary, (base64:encode(<<"[1, 2, 3, 5]">>))/binary>>,
    {ok, Ctx1} = parse_query_params(uri_string:dissect_query(F), Ctx),
    ?assertMatch(
        {segments, [1, 2, 3, 5], n_val},
        (Ctx1#context.filter)#filter.segment_filter
    ),
    BadF = <<<<"filter=">>/binary, (base64:encode(<<"[1, 2, A]">>))/binary>>,
    ?assertMatch(
        {halt, 400, _, <<"Exception decoding filter">>, _},
        parse_query_params(uri_string:dissect_query(BadF), Ctx)
    ).

valid_query_conversion_test() ->
    VF =
        base64:encode(
            iolist_to_binary(
                riak_kv_wm_json:encode(valid_filter())
            )
        ),
    VSL1 = base64:encode(<<"[1, 2, 3, 5]">>),
    SegDateFilter =
        #{
            <<"segment_filter">> =>
                #{
                    <<"tree_size">> => <<"large">>,
                    <<"segments">> => [1, 2, 3, 5]
                },
            <<"date_range">> =>
                #{
                    <<"start">> => 300000,
                    <<"end">> => 400000
                }
        },
    VSL2 =
        base64:encode(
            iolist_to_binary(
                riak_kv_wm_json:encode(SegDateFilter)
            )
        ),
    check_uri(<<"/aaebucketlist">>, 3),
    check_uri(<<"/erase/types/Type/buckets/Bucket">>, VF),
    check_uri(<<"/reap/types/Type/buckets/Bucket">>, VF),
    check_uri(<<"/tombs/types/Type/buckets/Bucket">>, VF),
    check_uri(<<"/objectstats/types/Type/buckets/Bucket">>, VF),
    check_uri(<<"/objectstats/buckets/Bucket">>, VF),
    check_uri(<<"/objectsizes/buckets/Bucket/sizes/10000">>, VF),
    check_uri(<<"/siblings/buckets/Bucket/counts/2">>, VF),
    check_uri(<<"/rangerepair/types/Type/buckets/Bucket">>, VF),
    check_uri(<<"/rangerepl/buckets/Bucket/queuename/queue">>, VF),
    check_uri(<<"/rangetrees/types/Type/buckets/Bucket/keysclocks">>, VF),
    check_uri(<<"/rangetrees/types/Type/buckets/Bucket/trees/large">>, VF),
    check_uri(<<"/cachedtrees/nvals/3/root">>, none),
    check_uri(<<"/cachedtrees/nvals/3/branch">>, VSL1),
    check_uri(<<"/cachedtrees/nvals/3/keysclocks">>, VSL1),
    check_uri(<<"/cachedtrees/nvals/3/keysclocks">>, VSL2).

create_uri(Path, none) ->
    Path;
create_uri(Path, NVal) when is_integer(NVal) ->
    iolist_to_binary([Path, <<"?nval=">>, integer_to_binary(NVal)]);
create_uri(Path, Filter) when is_binary(Filter) ->
    iolist_to_binary([Path, <<"?filter=">>, Filter]).

check_uri(Path, Filter) ->
    URI = create_uri(Path, Filter),
    {Route, QS} =
        case binary:split(URI, <<"?">>) of
            [R, Q] ->
                {R, Q};
            [R] ->
                {R, <<"">>}
        end,
    {ok, _, Ctx} =
        match_route(
            'GET',
            Route,
            binary:split(Route, <<"/">>, [global, trim_all])
        ),
    QP = uri_string:dissect_query(QS),
    {ok, Ctx1} = parse_query_params(QP, Ctx),
    ?assertNotMatch(invalid, convert_to_query(Ctx1)).

-endif.
