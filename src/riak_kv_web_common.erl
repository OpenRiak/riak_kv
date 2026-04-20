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
        type_preference/2,
        type_match/2,
        add_routes/0,
        get_version_vector/1,
        get_timeout/1,
        filter_options/1,
        make_clock_etag/1,
        compile_splitters/0
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

-spec type_preference(
    binary(),
    list(binary()) | binary()
) ->
    {boolean(), float()}.
type_preference(ContentType, AcceptedTypes) ->
    AcceptedTypeList =
        case is_list(AcceptedTypes) of
            true ->
                AcceptedTypes;
            false ->
                [AcceptedTypes]
        end,
    type_preference(split_type(ContentType), AcceptedTypeList, false, 0.0).

type_preference(error, _AcceptedTypes, Match, BestQ) ->
    {Match, BestQ};
type_preference(_, [], Match, BestQ) ->
    {Match, BestQ};
type_preference({PMT, SMT}, [ThisType|Rest], Match, BestQ) ->
    {TypeInfo, MaybeQ} =
        case binary:split(ThisType, get_subsplitter()) of
            [PlainType] when is_binary(PlainType) ->
                {PlainType, <<>>};
            [PlainType, Suffix] when is_binary(Suffix) ->
                {PlainType, Suffix}
        end,
    case match_type(TypeInfo, {PMT, SMT}) of
        true ->
            QV =
                case string:trim(MaybeQ, both) of
                    <<"q=", BF/binary >> ->
                        try
                            binary_to_float(BF)
                        catch
                            _ : _ ->
                                +0.0
                        end;
                    _ ->
                        1.0
                end,
            type_preference({PMT, SMT}, Rest, true, max(QV, BestQ));
        false ->
            type_preference({PMT, SMT}, Rest, Match, BestQ)
    end.

-spec type_match(
    binary() | {binary(), binary()} | error,
    list(binary()) | binary()
) ->
    boolean() | error.
type_match(CType, AcceptedTypes) when is_binary(AcceptedTypes) ->
    type_match(CType, [AcceptedTypes]);
type_match(CType, AcceptedTypes) when is_binary(CType) ->
    type_match(split_type(CType), AcceptedTypes);
type_match({_PMT, _SMT}, []) ->
    false;
type_match(error, _AcceptedTypes) ->
    error;
type_match({PMT, SMT}, [AcceptedType|Rest]) ->
    case match_type(AcceptedType, {PMT, SMT}) of
        true ->
            true;
        false ->
            type_match({PMT, SMT}, Rest)
    end.

match_type(AcceptedType, {PMT, SMT}) ->
    case {split_type(AcceptedType), {PMT, SMT}} of
        {{PMT, SMT}, {PMT, SMT}} ->
            true;
        {{PMT, <<"*">>}, {PMT, _}} ->
            true;
        {{<<"*">>, <<"*">>}, _} ->
            true;
        _ ->
            false
    end.

%% @doc Call this function when initialising API
-spec compile_splitters() -> ok.
compile_splitters() ->
    CP = binary:compile_pattern([<<";">>, <<" ;">>]),
    persistent_term:put({?MODULE, compile_patterns}, CP).

-spec get_subsplitter() -> list(binary()) | binary:cp().
get_subsplitter() ->
    persistent_term:get({?MODULE, compile_patterns}, [<<";">>, <<" ;">>]).

-spec split_type(binary()) -> {binary(), binary()} | error.
split_type(BinType) ->
    [PrimaryTypeInfo | _Rest] = binary:split(BinType, get_subsplitter()),
    case binary:split(PrimaryTypeInfo, <<"/">>) of
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

-spec make_clock_etag(vclock:vclock()) -> binary().
make_clock_etag(Vclock) ->
    <<ETag:128/integer>> = crypto:hash(md5, term_to_binary(Vclock)),
    list_to_binary(riak_core_util:integer_to_list(ETag, 62)).

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

type_preference_test() ->
    Accept1 = [<<"multipart/mixed">>, <<"*/*;q=0.9">>],
    Accept2 = [<<"*/*;q=0.9">>, <<"multipart/mixed">>],
    Accept3 =
        [
            <<"text/plain;q=0.9 ">>,
            <<"multipart_mixed;q=0.8">>,
            <<"*/*; q=0.7 ">>
        ],
    ?assertMatch({true, 1.0}, type_preference(<<"multipart/mixed">>, Accept1)),
    ?assertMatch({true, 0.9}, type_preference(<<"text/plain">>, Accept1)),
    ?assertMatch({true, 1.0}, type_preference(<<"multipart/mixed">>, Accept2)),
    ?assertMatch({true, 0.9}, type_preference(<<"text/plain">>, Accept2)),
    ?assertMatch({true, 0.7}, type_preference(<<"multipart/mixed">>, Accept3)),
    ?assertMatch({true, 0.9}, type_preference(<<"text/plain">>, Accept3)),
    ?assertMatch(
        {true, 0.7},
        type_preference(<<"application/json">>, Accept3)
    ),
    Accept4 =
        [
            <<"text/plain; q=0.9 ">>,
            <<"application/json">>,
            <<"multipart/* ; q=0.7 ">>
        ],
    ?assertMatch({true, 0.7}, type_preference(<<"multipart/mixed">>, Accept4)),
    ?assertMatch({true, 0.9}, type_preference(<<"text/plain">>, Accept4)),
    ?assertMatch(
        {true, 1.0},
        type_preference(<<"application/json">>, Accept4)
    ),
    ?assertMatch({false, +0.0}, type_preference(<<"text/xml">>, Accept4)),

    ?assertMatch(true, type_match(<<"multipart/mixed">>, Accept1)),
    ?assertMatch(true, type_match(<<"text/plain">>, Accept1)),
    ?assertMatch(true, type_match(<<"multipart/mixed">>, Accept4)),
    ?assertMatch(true, type_match(<<"text/plain">>, Accept4)),
    ?assertMatch(false, type_match(<<"text/xml">>, Accept4)),

    Accept5 =
        [
            <<"text/plain; q=0.9 ">>,
            <<"application-json">>,
            <<"multipart/* ; q=A ">>
        ],
    ?assertMatch({true, 0.9}, type_preference(<<"text/plain">>, Accept5)),
    ?assertMatch({true, +0.0}, type_preference(<<"multipart/mixed">>, Accept5)),
    ?assertMatch(
        {false, +0.0},
        type_preference(<<"application/json">>, Accept5)
    ).
    

-endif.
