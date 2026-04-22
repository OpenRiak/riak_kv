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

-module(riak_kv_web_queue).
-include_lib("riak_kv/include/riak_kv_web.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

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

-record(context, {
    client = riak_client:new(node(), self()) :: riak_client:riak_client(),
    request_type :: fetch_request | repl_request | membership_request,
    queue_name :: atom() | undefined,
    object_format = internal :: internal | internal_aaehash
}).

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
match_route('GET', _Path, [<<"queuename">>, Queue]) ->
    case riak_kv_web_common:check_queuename(Queue) of
        undefined ->
            nomatch;
        QueueA ->
            {
                ok,
                {16, 2048, 0},
                #context{request_type = fetch_request, queue_name = QueueA}
            }
    end;
match_route('POST', _Path, [<<"queuename">>, Queue]) ->
    case riak_kv_web_common:check_queuename(Queue) of
        undefined ->
            nomatch;
        QueueA ->
            {
                ok,
                {16, 2048, 8 * 1024 * 1024},
                #context{request_type = repl_request, queue_name = QueueA}
            }
    end;
match_route(_Method, _Path, [<<"queuename">>, _Queue]) ->
    {method_not_allowed, ['GET', 'POST']};
match_route('GET', _Path, [<<"membership_request">>]) ->
    {ok, {16, 2048, 0}, #context{request_type = membership_request}};
match_route(_Method, _Path, [<<"membership_request">>]) ->
    {method_not_allowed, ['GET']};
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
parse_query_params(Params, #context{request_type = fetch_request} = Ctx) ->
    case lists:keyfind(<<"object_format">>, 1, Params) of
        {<<"object_format">>, <<"internal">>} ->
            {ok, Ctx#context{object_format = internal}};
        {<<"object_format">>, <<"internal_aaehash">>} ->
            {ok, Ctx#context{object_format = internal_aaehash}};
        {<<"object_format">>, Unexpected} ->
            {
                halt,
                400,
                [?TXT_HEADER],
                <<"Format ~0p no defined">>,
                [Unexpected]
            };
        _ ->
            {ok, Ctx#context{object_format = internal}}
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
                all;
            SingleType when is_binary(SingleType) ->
                [SingleType];
            MultipleTypes when is_list(MultipleTypes) ->
                MultipleTypes
        end,
    case AcceptedTypes of
        all ->
            {ok, Ctx};
        L when is_list(L) ->
            CTypeRequired =
                case Ctx#context.request_type of
                    fetch_request ->
                        <<"application/octet-stream">>;
                    membership_request ->
                        <<"application/json">>
                end,
            case riak_kv_web_common:type_match(CTypeRequired, AcceptedTypes) of
                true ->
                    {ok, Ctx};
                false ->
                    {
                        halt,
                        406,
                        [?TXT_HEADER],
                        <<"~s must be accepted">>,
                        [CTypeRequired]
                    }
            end
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
process_request(none, #context{request_type = fetch_request} = Ctx) ->
    R = riak_client:fetch(Ctx#context.queue_name, Ctx#context.client),
    case format_response(Ctx#context.object_format, R) of
        {ok, Bin} ->
            {ok, {200, [?BIN_HEADER], Bin, true, none}, Ctx};
        {error, Bin} ->
            {halt, 500, [?TXT_HEADER], Bin, []}
    end;
process_request(none, #context{request_type = membership_request} = Ctx) ->
    case riak_client:membership_request(http) of
        AddrL when is_list(AddrL) ->
            RMap =
                #{
                    <<"up_nodes">> =>
                        lists:map(
                            fun({IP, Port}) ->
                                #{
                                    <<"ip">> => list_to_binary(IP),
                                    <<"port">> => integer_to_binary(Port)
                                }
                            end,
                            AddrL
                        )
                },
            Body = iolist_to_binary(riak_kv_wm_json:encode(RMap)),
            {ok, {200, [?BIN_HEADER], Body, true, none}, Ctx}
    end;
process_request(RqBdy, #context{request_type = repl_request} = Ctx) ->
    case riak_api_web_body:get_body(RqBdy, all, 60000) of
        {EncodedKeyList, UpdRqBdy} when is_binary(EncodedKeyList) ->
            case decode_keylist(EncodedKeyList) of
                false ->
                    ErrMsg = <<"Malformed Keyclock list">>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, []};
                KCL ->
                    QN = Ctx#context.queue_name,
                    ok = riak_kv_replrtq_src:replrtq_ttaaefs(QN, KCL),
                    RBin =
                        case riak_kv_replrtq_src:length_rtq(QN) of
                            {_, {FL, FSL, RTL}} ->
                                iolist_to_binary(
                                    io_lib:format(
                                        "Queue ~w: ~w ~w ~w",
                                        [QN, FL, FSL, RTL]
                                    )
                                );
                            _ ->
                                iolist_to_binary(
                                    io_lib:format(
                                        "No queue ~w",
                                        [QN]
                                    )
                                )
                        end,
                    {ok, {200, [?TXT_HEADER], RBin, true, UpdRqBdy}, Ctx}
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
%% Internal Functions
%% ===================================================================

decode_keylist(EncodedKeyList) ->
    case riak_kv_wm_json:decode(EncodedKeyList) of
        #{<<"keys-clocks">> := KCL} when is_list(KCL) ->
            KeyClockList =
                lists:foldl(fun decode_bucketkeyclock/2, [], KCL),
            case {length(KeyClockList), length(KCL)} of
                {N, N} ->
                    lists:reverse(KeyClockList);
                {N, M} ->
                    ?LOG_INFO(
                        "Malformed requests ~w within push of ~w",
                        [M - N, M]
                    ),
                    false
            end;
        _ ->
            false
    end.

decode_bucketkeyclock(
    #{
        <<"bucket-type">> := T,
        <<"bucket">> := B,
        <<"key">> := K,
        <<"clock">> := C
    },
    Acc
) ->
    case decode_clock(C) of
        false ->
            Acc;
        DecodedClock ->
            [{{T, B}, K, DecodedClock, to_fetch} | Acc]
    end;
decode_bucketkeyclock(
    #{
        <<"bucket">> := B,
        <<"key">> := K,
        <<"clock">> := C
    },
    Acc
) ->
    case decode_clock(C) of
        false ->
            Acc;
        DecodedClock ->
            [{B, K, DecodedClock, to_fetch} | Acc]
    end;
decode_bucketkeyclock(_, Acc) ->
    Acc.

-spec decode_clock(list()) -> vclock:vclock() | false.
decode_clock(EncodedClock) ->
    try
        riak_object:decode_vclock(base64:decode(EncodedClock))
    catch
        _:_ ->
            false
    end.

-type fetch_result() ::
    riak_object:riak_object()
    | queue_empty
    | {deleted, vclock:vclock(), riak_object:riak_object()}
    | {
        reap,
        {
            riak_object:bucket(),
            riak_object:key(),
            vclock:vclock(),
            erlang:timestamp()
        }
    }.

-spec format_response(
    internal | internal_aaehash,
    {ok, fetch_result()} | {error, Err :: term()}
) ->
    {ok, binary()} | {error, binary()}.
format_response(_, {ok, queue_empty}) ->
    {ok, <<0:8/integer>>};
format_response(_, {error, Reason}) ->
    ?LOG_WARNING("Fetch error ~w", [Reason]),
    {error, iolist_to_binary(io_lib:format("~0p", [Reason]))};
format_response(internal_aaehash, {ok, {reap, {B, K, TC, LMD}}}) ->
    BK = make_binarykey(B, K),
    {SegmentID, SegmentHash} =
        leveled_tictac:tictac_hash(BK, lists:sort(TC)),
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<0:8/integer>>,
    ObjBin = encode_riakobject({reap, {B, K, TC, LMD}}),
    {
        ok,
        <<
            SuccessMark/binary,
            IsTombstone/binary,
            SegmentID:32/integer,
            SegmentHash:32/integer,
            ObjBin/binary
        >>
    };
format_response(internal_aaehash, {ok, {deleted, TombClock, RObj}}) ->
    BK = make_binarykey(riak_object:bucket(RObj), riak_object:key(RObj)),
    {SegmentID, SegmentHash} =
        leveled_tictac:tictac_hash(BK, lists:sort(TombClock)),
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<1:8/integer>>,
    ObjBin = encode_riakobject(RObj),
    TombClockBin = term_to_binary(TombClock),
    TCL = byte_size(TombClockBin),
    {
        ok,
        <<
            SuccessMark/binary,
            IsTombstone/binary,
            SegmentID:32/integer,
            SegmentHash:32/integer,
            TCL:32/integer,
            TombClockBin/binary,
            ObjBin/binary
        >>
    };
format_response(internal_aaehash, {ok, RObj}) ->
    BK = make_binarykey(riak_object:bucket(RObj), riak_object:key(RObj)),
    {SegmentID, SegmentHash} =
        leveled_tictac:tictac_hash(BK, lists:sort(riak_object:vclock(RObj))),
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<0:8/integer>>,
    ObjBin = encode_riakobject(RObj),
    {
        ok,
        <<
            SuccessMark/binary,
            IsTombstone/binary,
            SegmentID:32/integer,
            SegmentHash:32/integer,
            ObjBin/binary
        >>
    };
format_response(internal, {ok, {reap, {B, K, TC, LMD}}}) ->
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<0:8/integer>>,
    ObjBin = encode_riakobject({reap, {B, K, TC, LMD}}),
    {
        ok,
        <<
            SuccessMark/binary,
            IsTombstone/binary,
            ObjBin/binary
        >>
    };
format_response(internal, {ok, {deleted, TombClock, RObj}}) ->
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<1:8/integer>>,
    ObjBin = encode_riakobject(RObj),
    TombClockBin = term_to_binary(TombClock),
    TCL = byte_size(TombClockBin),
    {
        ok,
        <<
            SuccessMark/binary,
            IsTombstone/binary,
            TCL:32/integer,
            TombClockBin/binary,
            ObjBin/binary
        >>
    };
format_response(internal, {ok, RObj}) ->
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<0:8/integer>>,
    ObjBin = encode_riakobject(RObj),
    {
        ok,
        <<
            SuccessMark/binary,
            IsTombstone/binary,
            ObjBin/binary
        >>
    }.

-spec encode_riakobject(
    riak_object:riak_object() | riak_object:repl_ref()
) ->
    binary().
encode_riakobject(RObj) ->
    ToCompress = app_helper:get_env(riak_kv, replrtq_compressonwire, false),
    FullObjBin = riak_object:nextgenrepl_encode(repl_v1, RObj, ToCompress),
    CRC = erlang:crc32(FullObjBin),
    <<CRC:32/integer, FullObjBin/binary>>.

-spec make_binarykey(riak_object:bucket(), riak_object:key()) -> binary().
%% @doc
%% Convert Bucket and Key into a single binary
make_binarykey(
    {Type, Bucket}, Key
) when
    is_binary(Type), is_binary(Bucket), is_binary(Key)
->
    <<Type/binary, Bucket/binary, Key/binary>>;
make_binarykey(Bucket, Key) when is_binary(Bucket), is_binary(Key) ->
    <<Bucket/binary, Key/binary>>.

%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

test_kcl() ->
    A = vclock:fresh(),
    B = vclock:fresh(),
    A1 = vclock:increment(a, A),
    B1 = vclock:increment(b, B),
    E1 = {<<"B1">>, <<"K1">>, A1},
    E2 = {{<<"T">>, <<"B2">>}, <<"K2">>, B1},
    [E1, E2].

encode_keys_and_clocks(KeysNClocks) ->
    Keys =
        {
            struct,
            [
                {
                    <<"keys-clocks">>,
                    [
                        {struct, encode_key_and_clock(Bucket, Key, Clock)}
                     || {Bucket, Key, Clock} <- KeysNClocks
                    ]
                }
            ]
        },
    mochijson2:encode(Keys).

encode_key_and_clock({Type, Bucket}, Key, C) ->
    [
        {<<"bucket-type">>, Type},
        {<<"bucket">>, Bucket},
        {<<"key">>, Key},
        {<<"clock">>, base64:encode_to_string(riak_object:encode_vclock(C))}
    ];
encode_key_and_clock(Bucket, Key, C) ->
    [
        {<<"bucket">>, Bucket},
        {<<"key">>, Key},
        {<<"clock">>, base64:encode_to_string(riak_object:encode_vclock(C))}
    ].

keylist_decode_test() ->
    KCL = test_kcl(),
    KeyJson =
        iolist_to_binary(
            encode_keys_and_clocks(KCL)
        ),
    ActualBKCL = decode_keylist(KeyJson),
    ExpectedBKCL =
        lists:map(fun({B, K, C}) -> {B, K, C, to_fetch} end, KCL),
    ?assertMatch(ExpectedBKCL, ActualBKCL).

-endif.
