%% -------------------------------------------------------------------
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
%% @doc common functions used by web API handlers

-module(riak_kv_web_common).

-include_lib("kernel/include/logger.hrl").
-include("riak_kv_web.hrl").

-export(
    [
        check_permissions/5,
        set_bucket/2,
        check_type_exists/1,
        count_fold/3,
        boolean_fold/3,
        decode_clock/1,
        normalise_boolean_param/1,
        type_match/2,
        add_routes/0,
        get_version_vector/1,
        get_timeout/1,
        filter_options/1
    ]
).

-define(BAD_COUNT_PARAM_TEXT, <<
    "~0p query parameter must be an integer or "
    "one of the following words: 'one', 'quorum' or 'all'"
>>).

-define(BAD_BOOLEAN_PARAM_TEXT, <<
    "~0p query parameter must be true or false"
>>).

-spec filter_options(#{atom() => term()}) -> list({atom(), term()}).
filter_options(Options) ->
    maps:to_list(
        maps:filter(
            fun(_K, V) -> V =/= undefined andalso V =/= default end,
            Options
        )
    ).

-spec add_routes() -> ok.
add_routes() ->
    Routes =
        [
            {5, riak_kv_web_object_read},
            {10, riak_kv_web_object_store},
            {15, riak_kv_web_object_delete},
            {30, riak_kv_web_index},
            {80, riak_kv_web_stats},
            {90, riak_kv_web_aaefold}
        ],
    riak_api_web:add_routes(Routes).

-spec check_permissions(
    riak_api_web_headers:headers(),
    riak_api_web_socket:scheme(),
    riak_api_web_handler:peer_ip(),
    riak_object:bucket() | undefined,
    atom()
) ->
    true | riak_api_web_acceptor:halt_response().
check_permissions(ReqHeaders, Scheme, Peer, Bucket, PermissionRequired) ->
    Authorised =
        riak_api_web_security:is_authorised(
            riak_core_security:is_enabled(),
            Scheme,
            ReqHeaders,
            Peer
        ),
    case Authorised of
        {ok, undefined} ->
            true;
        {ok, _SecContext} when PermissionRequired == undefined ->
            true;
        {ok, SecContext} ->
            PermissionGranted =
                riak_core_security:check_permission(
                    {PermissionRequired, Bucket},
                    SecContext
                ),
            case PermissionGranted of
                {true, _SecContext} ->
                    true;
                {false, Error, _SecContext} when is_binary(Error) ->
                    {halt, 403, [?TXT_HEADER], Error, []}
            end;
        {halt, RC, RH, RB, RS} ->
            {halt, RC, RH, RB, RS}
    end.

-spec set_bucket(binary(), binary()) -> riak_object:bucket().
set_bucket(<<"default">>, Bucket) ->
    Bucket;
set_bucket(BucketType, Bucket) ->
    {BucketType, Bucket}.

-spec check_type_exists(riak_object:bucket()) -> boolean().
check_type_exists({Type, _Bucket}) ->
    case riak_core_bucket_type:get(Type) of
        undefined ->
            % Not that this fetches the properties for the type from
            % the metadata - which may be an unnecessary cost.
            false;
        _ ->
            true
    end;
check_type_exists(Bucket) when is_binary(Bucket) ->
    true.

-spec count_fold(
    riak_api_web_handler:query_params(),
    list(binary()),
    #{atom() => term()}
) ->
    #{atom() => term()} | riak_api_web_acceptor:halt_response().
count_fold([], _CountKeys, Opts) ->
    Opts;
count_fold([{PK, PV} | Rest], CountKeys, Opts) when is_map(Opts) ->
    case lists:member(PK, CountKeys) of
        false ->
            count_fold(Rest, CountKeys, Opts);
        true ->
            case normalise_rw_param(PV) of
                bad_param ->
                    {halt, 400, [?TXT_HEADER], ?BAD_COUNT_PARAM_TEXT, [PK]};
                V ->
                    count_fold(
                        Rest,
                        CountKeys,
                        maps:put(binary_to_existing_atom(PK), V, Opts)
                    )
            end
    end.

-spec boolean_fold(
    riak_api_web_handler:query_params(),
    list(binary()),
    #{atom() => term()}
) ->
    #{atom() => term()} | riak_api_web_acceptor:halt_response().
boolean_fold([], _BoolKeys, Opts) ->
    Opts;
boolean_fold([{PK, PV} | Rest], BoolKeys, Opts) when is_map(Opts) ->
    case lists:member(PK, BoolKeys) of
        false ->
            boolean_fold(Rest, BoolKeys, Opts);
        true ->
            case normalise_boolean_param(PV) of
                bad_param ->
                    {halt, 400, [?TXT_HEADER], ?BAD_BOOLEAN_PARAM_TEXT, [PK]};
                V ->
                    boolean_fold(
                        Rest,
                        BoolKeys,
                        maps:put(binary_to_existing_atom(PK), V, Opts)
                    )
            end
    end.

normalise_rw_param(<<"default">>) ->
    default;
normalise_rw_param(<<"one">>) ->
    1;
normalise_rw_param(<<"quorum">>) ->
    quorum;
normalise_rw_param(<<"all">>) ->
    all;
normalise_rw_param(V) when is_binary(V) ->
    try
        case binary_to_integer(V) of
            I when I >= 0 ->
                I
        end
    catch
        _:_ ->
            bad_param
    end;
normalise_rw_param(_) ->
    bad_param.

normalise_boolean_param(true) ->
    true;
normalise_boolean_param(V) when is_binary(V) ->
    case string:casefold(V) of
        <<"true">> ->
            true;
        <<"false">> ->
            false;
        <<"default">> ->
            default;
        _ ->
            bad_param
    end.

-spec decode_clock(unicode:chardata()) -> vclock:vclock() | error.
decode_clock(EncodedClock) ->
    try
        riak_object:decode_vclock(base64:decode(EncodedClock))
    catch
        _:Error ->
            ?LOG_WARNING(
                "Unexpected error decoding clock ~0p",
                [Error]
            ),
            error
    end.

-spec type_match(
    binary(),
    list(binary()) | binary() | all
) ->
    {boolean(), binary()}.
type_match(ContentType, all) ->
    {true, ContentType};
type_match(ContentType, AcceptedType) when is_binary(AcceptedType) ->
    type_match(ContentType, [AcceptedType]);
type_match(ContentType, AcceptedTypes) ->
    case lists:member(<<"*/*">>, AcceptedTypes) of
        true ->
            {true, ContentType};
        false ->
            case split_type(ContentType) of
                {Type, SubType} ->
                    type_match(Type, SubType, ContentType, AcceptedTypes);
                error ->
                    type_match(
                        <<"application">>,
                        <<"octet-stream">>,
                        <<"application/octet-stream">>,
                        AcceptedTypes
                    )
            end
    end.

type_match(_Type, _SubType, BinType, []) ->
    {false, BinType};
type_match(Type, SubType, BinType, [AcceptedType | Rest]) ->
    case split_type(AcceptedType) of
        {Type, SubType} ->
            {true, BinType};
        {Type, <<"*">>} ->
            {true, BinType};
        _ ->
            type_match(Type, SubType, BinType, Rest)
    end.

-spec split_type(binary()) -> {binary(), binary()} | error.
split_type(BinType) ->
    [PrimaryTypeInfo | _Rest] = string:split(BinType, <<";">>, leading),
    case binary:split(PrimaryTypeInfo, <<"/">>, []) of
        [Type, SubType] when is_binary(Type), is_binary(SubType) ->
            {Type, SubType};
        _NotSplitAsExpected ->
            error
    end.

-spec get_version_vector(
    riak_api_web_headers:headers()
) ->
    {ok, vclock:vclock()} | {ok, none} | riak_api_web_acceptor:halt_response().
get_version_vector(ReqHeaders) ->
    ClockHeader =
        riak_api_web_headers:lookup(?HEAD_VCLOCK_CASEFOLD, ReqHeaders, true),
    case ClockHeader of
        undefined ->
            {ok, none};
        {_OrigKey, [EncodedClock]} ->
            case riak_kv_web_common:decode_clock(EncodedClock) of
                error ->
                    ErrorRsp =
                        <<
                            "Error decoding vector clock in "
                            "x-riak-vclock header"
                        >>,
                    {halt, 400, [?TXT_HEADER], ErrorRsp, []};
                DecodedClock ->
                    {ok, DecodedClock}
            end;
        {_OrigKey, _MultipleClocks} ->
            ErrorRsp =
                <<
                    "Only one x-riak-vclock may be specified"
                >>,
            {halt, 400, [?TXT_HEADER], ErrorRsp, []}
    end.

-spec get_timeout(
    riak_api_web_handler:query_params()
) ->
    {ok, non_neg_integer()}
    | {ok, none}
    | riak_api_web_acceptor:halt_response().
get_timeout(Params) ->
    case lists:keyfind(<<"timeout">>, 1, Params) of
        false ->
            {ok, none};
        {<<"timeout">>, TO} when is_binary(TO) ->
            try
                IntTO = binary_to_integer(TO),
                true = IntTO >= 0,
                {ok, IntTO}
            catch
                _:_ ->
                    ErrMsg = <<"Bad timeout value ~0p">>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, [TO]}
            end
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

split_path(RequestLine) ->
    {ok, {http_request, Method, {abs_path, Path}, _Version}, _Rest} =
        erlang:decode_packet(http_bin, RequestLine, []),
    URIMap = uri_string:normalize(Path, [return_map]),
    NormalisedPath = maps:get(path, URIMap, <<"">>),
    case string:split(NormalisedPath, <<"/">>, all) of
        [<<>> | Rest] ->
            {ok, Method, Rest, NormalisedPath};
        PathList when is_list(PathList) ->
            {ok, Method, PathList, NormalisedPath}
    end.

check_path(RequestLine) ->
    {ok, Method, SplitPath, AbsPath} = split_path(RequestLine),
    riak_api_web:get_route(Method, AbsPath, SplitPath).

routing_test() ->
    add_routes(),
    ?assertMatch(
        {ok, riak_kv_web_object_read, _, _},
        check_path(<<"GET /types/T/buckets/B/keys/K HTTP/1.1\r\n">>)
    ),
    ?assertMatch(
        {ok, riak_kv_web_object_read, _, _},
        check_path(<<"HEAD /types/T/buckets/B/keys/K HTTP/1.1\r\n">>)
    ),
    ?assertMatch(
        {ok, riak_kv_web_object_read, _, _},
        check_path(<<"GET /buckets/B/keys/K HTTP/1.1\r\n">>)
    ),
    ?assertMatch(
        {halt, 405, [{'Allow', _}], <<>>, []},
        check_path(<<"OPTIONS /types/T/buckets/B/keys/K HTTP/1.1\r\n">>)
    ),
    ?assertMatch(
        {ok, riak_kv_web_object_store, _, _},
        check_path(<<"POST /types/T/buckets/B/keys HTTP/1.1\r\n">>)
    ),
    ?assertMatch(
        {halt, 405, [{'Allow', _}], <<>>, []},
        check_path(<<"PUT /types/T/buckets/B/keys HTTP/1.1\r\n">>)
    ),
    ?assertMatch(
        {ok, riak_kv_web_object_store, _, _},
        check_path(<<"POST /buckets/B/keys HTTP/1.1\r\n">>)
    ),
    ?assertMatch(
        {ok, riak_kv_web_object_delete, _, _},
        check_path(<<"DELETE /types/T/buckets/B/keys/K HTTP/1.1\r\n">>)
    ),
    ?assertMatch(
        {ok, riak_kv_web_object_delete, _, _},
        check_path(<<"DELETE /buckets/B/keys/K HTTP/1.1\r\n">>)
    ),
    ?assertMatch(
        {ok, riak_kv_web_index, _, _},
        check_path(
            <<"GET /types/T/buckets/B/index/field1_bin/exact HTTP/1.1\r\n">>
        )
    ),
    ?assertMatch(
        {ok, riak_kv_web_index, _, _},
        check_path(
            <<"GET /types/T/buckets/B/index/field1_bin/s/e HTTP/1.1\r\n">>
        )
    ),
    ?assertMatch(
        {ok, riak_kv_web_index, _, _},
        check_path(
            <<"GET /buckets/B/index/field1_bin/exact HTTP/1.1\r\n">>
        )
    ),
    ?assertMatch(
        {ok, riak_kv_web_index, _, _},
        check_path(
            <<"GET /buckets/B/index/field1_bin/s/e HTTP/1.1\r\n">>
        )
    ),
    ?assertMatch(
        {ok, riak_kv_web_index, _, _},
        check_path(
            <<"GET /buckets/B/index/field1_int/1/10 HTTP/1.1\r\n">>
        )
    ),
    ?assertMatch(
        {halt, 405, [{'Allow', <<"GET">>}], <<>>, []},
        check_path(
            <<"HEAD /types/T/buckets/B/index/field1_bin/s/e HTTP/1.1\r\n">>
        )
    ),
    ?assertMatch(
        {ok, riak_kv_web_stats, _, _},
        check_path(
            <<"GET /stats HTTP/1.1\r\n">>
        )
    ),
    ?assertMatch(
        {halt, 404, [], <<>>, []},
        check_path(
            <<"GET /other HTTP/1.1\r\n">>
        )
    ).

-endif.
