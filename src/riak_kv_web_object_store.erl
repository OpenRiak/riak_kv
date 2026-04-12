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
%% @doc Handler for HTTP API requests to store an object ('PUT' or 'POST'
%% requests)

-module(riak_kv_web_object_store).
-include("riak_object.hrl").
-include("riak_kv_web.hrl").

-if(?OTP_RELEASE == 26).
-feature(maybe_expr, enable).
-endif.

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

-define(PUT_DEFAULTS, #{
    w => default,
    dw => default,
    pw => default,
    node_confirms => default,
    sync_on_write => backend,
    n_val => default,
    asis => false,
    returnbody => false,
    timeout => undefined
}).

-type put_option_key() ::
    w
    | dw
    | pw
    | node_confirms
    | sync_on_write
    | n_val
    | asis
    | returnbody
    | timeout.

-type put_options() ::
    #{
        w => pos_integer() | default | quorum | all,
        dw => non_neg_integer() | default | quorum | all,
        pw => non_neg_integer() | default | quorum | all,
        node_confirms => non_neg_integer() | default | quorum | all,
        sync_on_write => default | backend | one | all,
        n_val => pos_integer() | default,
        asis => boolean(),
        returnbody => boolean(),
        timeout => pos_integer() | undefined
    }.

-record(context, {
    client = riak_client:new(node(), self()) :: riak_client:riak_client(),
    method :: 'PUT' | 'POST',
    bucket :: riak_object:bucket(),
    key :: riak_object:key(),
    put_options = ?PUT_DEFAULTS :: put_options(),
    object :: riak_object:riak_object() | undefined,
    if_not_modified :: vclock:vclock() | undefined,
    if_none_match = false :: boolean()
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
match_route(
    Method,
    _Path,
    [<<"types">>, BucketType, <<"buckets">>, Bucket, <<"keys">>, Key]
) when is_binary(Key) ->
    case Method of
        Method when Method == 'PUT'; Method == 'POST' ->
            Context =
                #context{
                    method = Method,
                    bucket = riak_kv_web_common:set_bucket(BucketType, Bucket),
                    key = Key
                },
            {ok, size_limits(), Context};
        Method when Method == 'GET'; Method == 'HEAD'; Method == 'DELETE' ->
            nomatch;
        _OtherMethod ->
            {method_not_allowed, ['GET', 'HEAD', 'PUT', 'POST', 'DELETE']}
    end;
match_route(
    Method,
    Path,
    [<<"buckets">>, Bucket, <<"keys">>, Key]
) when is_binary(Key) ->
    match_route(
        Method,
        Path,
        [<<"types">>, <<"default">>, <<"buckets">>, Bucket, <<"keys">>, Key]
    );
match_route(
    Method,
    Path,
    [<<"types">>, BucketType, <<"buckets">>, Bucket, <<"keys">>]
) ->
    case Method of
        'POST' ->
            K = iolist_to_binary(riak_core_util:unique_id_62()),
            match_route(
                Method,
                Path,
                [<<"types">>, BucketType, <<"buckets">>, Bucket, <<"keys">>, K]
            );
        _ ->
            {method_not_allowed, ['POST']}
    end;
match_route(
    Method,
    Path,
    [<<"buckets">>, Bucket, <<"keys">>]
) ->
    match_route(
        Method,
        Path,
        [<<"types">>, <<"default">>, <<"buckets">>, Bucket, <<"keys">>]
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
            riak_kv_put
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
parse_query_params([], Ctx) ->
    % Typically we expect no options - so shortcut the validation in this case
    {ok, Ctx};
parse_query_params(Params, Ctx) ->
    maybe
        {ok, Ctx0} ?= validate_timeout(Params, Ctx),
        {ok, Ctx1} ?= validate_counts(Params, Ctx0),
        {ok, Ctx2} ?= validate_booleans(Params, Ctx1),
        validate_synconwrite(Params, Ctx2)
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
    maybe
        {ok, Ctx0} ?= validate_conditional_request(ReqHeaders, Ctx),
        Obj = riak_object:new(Ctx#context.bucket, Ctx#context.key, <<>>),
        {ok, Obj1} ?= set_version_vector(ReqHeaders, Obj),
        {ok, MD0} ?= set_index_specs(ReqHeaders, riak_object:metadata_new()),
        {ok, MD1} ?= set_user_metadata(ReqHeaders, MD0),
        {ok, MD2} ?= set_content_type_and_encoding(ReqHeaders, MD1),
        {ok, Ctx0#context{object = riak_object:update_metadata(Obj1, MD2)}}
    else
        HaltResponse ->
            HaltResponse
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
process_request(RqBdy, Context) ->
    case riak_api_web_body:get_body(RqBdy, all, 60000) of
        {error, content_too_large} ->
            {halt, 413, [], <<>>, []};
        {ObjBody, UpdRqBody} when is_binary(ObjBody) ->
            PutRsp =
                do_put(
                    riak_object:update_value(Context#context.object, ObjBody),
                    Context
                ),
            case PutRsp of
                {error, Reason} ->
                    handle_error(Reason, Context);
                ok ->
                    {ok, {200, [], <<>>, false}, UpdRqBody};
                {ok, Obj} ->
                    {ok, riak_kv_web_object_read:produce_response(Obj)}
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

-spec validate_timeout(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
validate_timeout(Params, Ctx) ->
    case lists:keyfind(<<"timeout">>, 1, Params) of
        false ->
            {ok, Ctx};
        {<<"timeout">>, TO} when is_binary(TO) ->
            try
                IntTO = binary_to_integer(TO),
                true = IntTO >= 0,
                {ok, set_option(timeout, IntTO, Ctx)}
            catch
                _:_ ->
                    ErrMsg = <<"Bad timeout value ~0p">>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, [TO]}
            end
    end.

-spec validate_counts(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
validate_counts(Params, Context) ->
    FoldResult =
        riak_kv_web_common:count_fold(
            Params,
            [<<"w">>, <<"dw">>, <<"pw">>, <<"n_val">>, <<"node_confirms">>],
            Context#context.put_options
        ),
    case FoldResult of
        UpdOpts when is_map(UpdOpts) ->
            {ok, Context#context{put_options = UpdOpts}};
        HaltResponse ->
            HaltResponse
    end.

-spec validate_booleans(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
validate_booleans(Params, Context) ->
    FoldResult =
        riak_kv_web_common:boolean_fold(
            Params,
            [<<"returnbody">>, <<"basic_quorum">>, <<"asis">>],
            Context#context.put_options
        ),
    case FoldResult of
        UpdOpts when is_map(UpdOpts) ->
            {ok, Context#context{put_options = UpdOpts}};
        HaltResponse ->
            HaltResponse
    end.

-spec validate_synconwrite(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()}.
validate_synconwrite(QueryParams, Context) ->
    case lists:keyfind(<<"sync_on_write">>, 1, QueryParams) of
        false ->
            {ok, Context};
        {<<"sync_on_write">>, Valid} when
            Valid == <<"default">>;
            Valid == <<"backend">>;
            Valid == <<"one">>;
            Valid == <<"all">>
        ->
            {ok, set_option(sync_on_write, binary_to_atom(Valid), Context)};
        _Invalid ->
            ErrorText =
                <<"~w query parameter must be one of the following words: ~0p">>,
            {
                halt,
                400,
                [?TXT_HEADER],
                ErrorText,
                [sync_on_write, [default, backend, one, all]]
            }
    end.

-spec validate_conditional_request(
    riak_api_web_headers:headers(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
validate_conditional_request(ReqHeaders, Ctx) ->
    Ctx0 =
        case riak_api_web_headers:get_value('If-None-Match', ReqHeaders) of
            undefined ->
                Ctx;
            _ ->
                Ctx#context{if_none_match = true}
        end,
    IfNotModClock =
        riak_api_web_headers:lookup(?HEAD_IFNOTMOD_CASEFOLD, ReqHeaders, true),
    case IfNotModClock of
        undefined ->
            {ok, Ctx0};
        {_OrigKey, [EncodedClock]} ->
            case riak_kv_web_common:decode_clock(EncodedClock) of
                error ->
                    ErrorRsp =
                        <<
                            "Error decoding vector clock in "
                            "x-riak-if-not-modified header"
                        >>,
                    {halt, 400, [?TXT_HEADER], ErrorRsp, []};
                DecodedClock ->
                    {ok, Ctx0#context{if_not_modified = DecodedClock}}
            end;
        {_OrigKey, _MultipleClocks} ->
            ErrorRsp =
                <<
                    "Only one value may be set "
                    "for x-riak-if-not-modified header"
                >>,
            {halt, 400, [?TXT_HEADER], ErrorRsp, []}
    end.

%% ===================================================================
%% Build object
%% ===================================================================

-spec set_version_vector(
    riak_api_web_headers:headers(),
    riak_object:riak_object()
) ->
    {ok, riak_object:riak_object()} | riak_api_web_acceptor:halt_response().
set_version_vector(ReqHeaders, Obj) ->
    ClockHeader =
        riak_api_web_headers:lookup(?HEAD_VCLOCK_CASEFOLD, ReqHeaders, true),
    case ClockHeader of
        undefined ->
            {ok, Obj};
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
                    {ok, riak_object:set_vclock(Obj, DecodedClock)}
            end;
        {_OrigKey, _MultipleClocks} ->
            ErrorRsp =
                <<
                    "Only one x-riak-vclock may be specified"
                >>,
            {halt, 400, [?TXT_HEADER], ErrorRsp, []}
    end.

-spec set_index_specs(
    riak_api_web_headers:headers(),
    riak_object:riak_object_meta()
) ->
    {ok, riak_object:riak_object_meta()}.
set_index_specs(ReqHeaders, MD) ->
    IndexHeaders =
        riak_api_web_headers:prefix_fold(
            ?HEAD_INDEX_CASEFOLD,
            ReqHeaders,
            true
        ),
    IndexSpecs =
        lists:foldl(
            fun({Fld, Terms}, IdxAcc) ->
                lists:map(fun(T) -> {Fld, T} end, Terms) ++ IdxAcc
            end,
            [],
            IndexHeaders
        ),
    {ok, riak_object:metadata_store(?MD_INDEX, IndexSpecs, MD)}.

-spec set_user_metadata(
    riak_api_web_headers:headers(),
    riak_object:riak_object_meta()
) ->
    {ok, riak_object:riak_object_meta()}.
set_user_metadata(ReqHeaders, MD) ->
    MetaHeaders =
        riak_api_web_headers:prefix_fold(
            ?HEAD_USERMETA_CASEFOLD,
            ReqHeaders,
            true
        ),
    MetaSpecs =
        lists:foldl(
            fun({K, VL}, MetaAcc) ->
                lists:map(fun(V) -> {K, V} end, VL) ++ MetaAcc
            end,
            [],
            MetaHeaders
        ),
    {ok, riak_object:metadata_store(?MD_USERMETA, MetaSpecs, MD)}.

-spec set_content_type_and_encoding(
    riak_api_web_headers:headers(),
    riak_object:riak_object_meta()
) ->
    {ok, riak_object:riak_object_meta()}.
set_content_type_and_encoding(ReqHeaders, MD) ->
    ContentEncodingHeader =
        riak_api_web_headers:get_value('Content-Encoding', ReqHeaders),
    MD1 =
        case ContentEncodingHeader of
            undefined ->
                MD;
            EncodingList when is_list(EncodingList) ->
                Encoding =
                    lists:flatten(
                        lists:join(
                            ", ",
                            lists:map(fun binary_to_list/1, EncodingList)
                        )
                    ),
                riak_object:metadata_store(
                    ?MD_ENCODING,
                    Encoding,
                    MD
                );
            Encoding ->
                riak_object:metadata_store(
                    ?MD_ENCODING,
                    binary_to_list(Encoding),
                    MD
                )
        end,
    ContentTypeHeader =
        riak_api_web_headers:get_unique_value('Content-Type', ReqHeaders),
    case ContentTypeHeader of
        undefined ->
            {
                halt,
                415,
                [?TXT_HEADER, {<<"Accept-Post">>, <<"*/*">>}],
                <<"Missing Content-Type request header">>,
                []
            };
        {error, multiple_values} ->
            {
                halt,
                415,
                [?TXT_HEADER, {<<"Accept-Post">>, <<"*/*">>}],
                <<"Only one Content-Type may be specified">>,
                []
            };
        ContentType when is_binary(ContentType) ->
            [CType | RawParams] = string:lexemes(ContentType, "; "),
            case take_first_encoding(RawParams) of
                undefined ->
                    {ok, riak_object:metadata_store(?MD_CTYPE, CType, MD1)};
                Charset ->
                    {
                        ok,
                        riak_object:metadata_store(
                            ?MD_CTYPE,
                            binary_to_list(CType),
                            % list for backwards compatibility
                            riak_object:metadata_store(
                                ?MD_CHARSET,
                                binary_to_list(Charset),
                                MD1
                            )
                        )
                    }
            end
    end.

take_first_encoding([]) ->
    undefined;
take_first_encoding([Param | Rest]) ->
    case string:split(Param, "=") of
        [<<"charset">>, Charset] ->
            Charset;
        _ ->
            take_first_encoding(Rest)
    end.

-spec do_put(
    riak_object:riak_object(),
    context()
) ->
    ok | {ok, riak_object:riak_object()} | {error, term()}.
do_put(Object, Ctx) ->
    CondPutMode =
        application:get_env(riak_kv, conditional_put_mode, api_only),
    {CondPutOptions, SessionToken} =
        case
            {
                Ctx#context.if_not_modified,
                Ctx#context.if_none_match,
                CondPutMode =/= api_only
            }
        of
            {undefined, false, _} ->
                {[], none};
            {NotMod, NoneMatch, true} ->
                TokenResult =
                    riak_kv_token_session:session_request_retry(
                        {Ctx#context.bucket, Ctx#context.key}
                    ),
                case TokenResult of
                    {true, Token} ->
                        GetOpts =
                            [
                                {basic_quorum, true},
                                {return_body, false},
                                {deleted_vclock, true}
                            ],
                        Condition =
                            case NotMod of
                                undefined ->
                                    {undefined, true, GetOpts};
                                InClock ->
                                    {{true, InClock}, undefined, GetOpts}
                            end,
                        {[{condition_check, Condition}], Token};
                    _ ->
                        case {NotMod, NoneMatch} of
                            {_, true} ->
                                {[{if_none_match, true}], none};
                            {InClock, _} ->
                                {[{if_not_modified, InClock}], none}
                        end
                end;
            {NotMod, NoneMatch, false} ->
                case {NotMod, NoneMatch} of
                    {_, true} ->
                        {[{if_none_match, true}], none};
                    {InClock, _} ->
                        {[{if_not_modified, InClock}], none}
                end
        end,
    case SessionToken of
        none ->
            riak_client:put(
                Object,
                CondPutOptions ++ maps:to_list(Ctx#context.put_options),
                Ctx#context.client
            );
        _ ->
            riak_kv_token_session:session_use(
                SessionToken,
                put,
                [
                    Object,
                    CondPutOptions ++ maps:to_list(Ctx#context.put_options)
                ]
            )
    end.

%% ===================================================================
%% Internal Functions
%% ===================================================================

-spec handle_error(term(), context()) -> riak_api_web_acceptor:halt_response().
handle_error(precommit_fail, Ctx) ->
    Msg =
        iolist_to_binary(
            io_lib:format(
                <<"~w aborted by pre-commit hook.">>,
                [Ctx#context.method]
            )
        ),
    handle_error({precommit_fail, Msg}, Ctx);
handle_error({precommit_fail, Msg}, _Ctx) ->
    case is_binary(Msg) of
        true ->
            {halt, 403, [?TXT_HEADER], Msg, []};
        false ->
            {halt, 403, [?TXT_HEADER], iolist_to_binary(Msg), []}
    end;
handle_error(too_many_fails, _Ctx) ->
    Msg = <<"Too Many write failures to satisfy W/DW">>,
    {halt, 503, [?TXT_HEADER], Msg, []};
handle_error(timeout, _Ctx) ->
    {halt, 503, [?TXT_HEADER], <<"request timed out">>, []};
handle_error({n_val_violation, N}, _Ctx) ->
    Msg =
        <<
            "Specified w/dw/pw/node_confirms"
            " values invalid for bucket n value of ~p"
        >>,
    {halt, 400, [?TXT_HEADER], Msg, [N]};
handle_error({dw_val_unsatisfied, DW, NumDW}, _Ctx) ->
    {halt, 503, [?TXT_HEADER], <<"DW-value unsatisfied: ~p/~p">>, [DW, NumDW]};
handle_error({pw_val_unsatisfied, PW, NumPW}, _Ctx) ->
    {halt, 503, [?TXT_HEADER], <<"PW-value unsatisfied: ~p/~p">>, [PW, NumPW]};
handle_error({node_confirms_val_unsatisfied, NC, NumNC}, _Ctx) ->
    Msg = <<"node_confirms-value unsatisfied: ~p/~p">>,
    {halt, 503, [?TXT_HEADER], Msg, [NC, NumNC]};
handle_error(failed, _Ctx) ->
    {halt, 412, [], <<>>, []};
handle_error("match_found", _Ctx) ->
    {halt, 412, [], <<>>, []};
handle_error("modified", _Ctx) ->
    {halt, 409, [], <<>>, []};
handle_error(OtherError, _Ctx) ->
    {halt, 500, [?TXT_HEADER], <<"Error:~n~p">>, [OtherError]}.

-spec set_option(
    put_option_key(),
    non_neg_integer() | quorum | all | backend | one | boolean(),
    context()
) ->
    context().
set_option(Option, Value, Context) ->
    Context#context{
        put_options = maps:put(Option, Value, Context#context.put_options)
    }.

size_limits() ->
    {
        1024,
        2048,
        application:get_env(riak_kv, max_object_size)
    }.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").

request_headers_test() ->
    Vc =
        base64:encode(
            riak_object:encode_vclock(
                vclock:increment('node1@127.0.0.1', vclock:fresh())
            )
        ),
    TestHeaders =
        [
            {<<"X-riak-Index-date_bin">>, <<"date1, date2">>},
            {<<"x-riak-index-date_bin">>, <<"date3">>},
            {<<"x-riak-index-name_bin">>, <<"name1">>},
            {<<"X-Riak-Meta-postcode">>, <<"postcode1">>},
            {<<"X-Riak-Meta-postcode">>, <<"postcode2">>},
            {'If-None-Match', <<"*">>},
            {'Content-Type', <<"application/json; charset=utf8">>},
            {'Content-Encoding', <<"gzip, deflate">>},
            {<<"X-Riak-vclock">>, Vc}
        ],
    InitCtx =
        #context{
            method = 'PUT',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>,
            client = dummy
        },
    TestHeaderObj = riak_api_web_headers:make(TestHeaders),
    {ok, CtxOut} = parse_request_headers(TestHeaderObj, InitCtx),
    MD =
        riak_object:get_metadata(
            riak_object:apply_updates(CtxOut#context.object)
        ),
    ?assertMatch(
        [
            {<<"date_bin">>, <<"date1">>},
            {<<"date_bin">>, <<"date2">>},
            {<<"date_bin">>, <<"date3">>},
            {<<"name_bin">>, <<"name1">>}
        ],
        lists:sort(riak_object:metadata_fetch(?MD_INDEX, MD))
    ),
    ?assertMatch(
        [
            {<<"postcode">>, <<"postcode1">>},
            {<<"postcode">>, <<"postcode2">>}
        ],
        lists:sort(riak_object:metadata_fetch(?MD_USERMETA, MD))
    ),
    ?assertMatch(
        "application/json",
        riak_object:metadata_fetch(?MD_CTYPE, MD)
    ),
    ?assertMatch(
        "utf8",
        riak_object:metadata_fetch(?MD_CHARSET, MD)
    ),
    ?assertMatch(
        "gzip, deflate",
        riak_object:metadata_fetch(?MD_ENCODING, MD)
    ),
    ?assert(CtxOut#context.if_none_match).

headers_clock_error1_test() ->
    Vc =
        base64:encode(
            riak_object:encode_vclock(
                vclock:increment('node1@127.0.0.1', vclock:fresh())
            ),
            #{mode => urlsafe}
        ),
    TestHeaders =
        [
            {<<"X-riak-Index-date_bin">>, <<"date1, date2">>},
            {'Content-Type', <<"application/json; charset=utf8">>},
            {'Content-Encoding', <<"gzip, deflate">>},
            {<<"X-Riak-vclock">>, Vc}
        ],
    InitCtx =
        #context{
            method = 'PUT',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>,
            client = dummy
        },
    TestHeaderObj = riak_api_web_headers:make(TestHeaders),
    {halt, 400, [?TXT_HEADER], Msg, _Subs} =
        parse_request_headers(TestHeaderObj, InitCtx),
    ?assertMatch(
        <<"Error decoding vector clock in x-riak-vclock header">>,
        Msg
    ).

headers_clock_error2_test() ->
    VcB =
        base64:encode(
            riak_object:encode_vclock(
                vclock:increment('node1@127.0.0.1', vclock:fresh())
            ),
            #{mode => urlsafe}
        ),
    VcG =
        base64:encode(
            riak_object:encode_vclock(
                vclock:increment('node1@127.0.0.1', vclock:fresh())
            )
        ),
    TestHeaders =
        [
            {<<"X-riak-if-not-modified">>, VcB},
            {'Content-Type', <<"application/json; charset=utf8">>},
            {'Content-Encoding', <<"gzip, deflate">>},
            {<<"X-Riak-vclock">>, VcG}
        ],
    InitCtx =
        #context{
            method = 'PUT',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>,
            client = dummy
        },
    TestHeaderObj = riak_api_web_headers:make(TestHeaders),
    {halt, 400, [?TXT_HEADER], Msg, _Subs} =
        parse_request_headers(TestHeaderObj, InitCtx),
    ?assertMatch(
        <<"Error decoding vector clock in x-riak-if-not-modified header">>,
        Msg
    ).

headers_single_encoding_test() ->
    VcE = vclock:increment('node1@127.0.0.1', vclock:fresh()),
    Vc = base64:encode(riak_object:encode_vclock(VcE)),
    TestHeaders =
        [
            {'Content-Type', <<"application/json; charset=utf8">>},
            {'Content-Encoding', <<"gzip">>},
            {<<"X-riak-if-not-modified">>, Vc},
            {<<"X-Riak-vclock">>, Vc}
        ],
    InitCtx =
        #context{
            method = 'PUT',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>,
            client = dummy
        },
    TestHeaderObj = riak_api_web_headers:make(TestHeaders),
    {ok, CtxOut} = parse_request_headers(TestHeaderObj, InitCtx),
    MD =
        riak_object:get_metadata(
            riak_object:apply_updates(CtxOut#context.object)
        ),
    ?assertMatch("gzip", riak_object:metadata_fetch(?MD_ENCODING, MD)),
    ?assertMatch(VcE, riak_object:vclock(CtxOut#context.object)).

headers_multiple_value_error1_test() ->
    Vc0 = vclock:increment('node1@127.0.0.1', vclock:fresh()),
    Vc1 = vclock:increment('node1@127.0.0.1', Vc0),
    TestHeaders =
        [
            {'Content-Type', <<"application/json; charset=utf8">>},
            {'Content-Encoding', <<"gzip">>},
            {
                <<"X-riak-if-not-modified">>,
                base64:encode(riak_object:encode_vclock(Vc0))
            },
            {
                <<"X-riak-if-not-modified">>,
                base64:encode(riak_object:encode_vclock(Vc1))
            },
            {
                <<"X-Riak-vclock">>,
                base64:encode(riak_object:encode_vclock(Vc0))
            }
        ],
    InitCtx =
        #context{
            method = 'PUT',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>,
            client = dummy
        },
    TestHeaderObj = riak_api_web_headers:make(TestHeaders),
    {halt, 400, [?TXT_HEADER], Msg, []} =
        parse_request_headers(TestHeaderObj, InitCtx),
    ?assertMatch(
        <<
            "Only one value may be set "
            "for x-riak-if-not-modified header"
        >>,
        Msg
    ),
    MultiClockHeaders =
        [
            {'Content-Type', <<"application/json">>},
            {
                <<"X-Riak-vclock">>,
                base64:encode(riak_object:encode_vclock(Vc0))
            },
            {
                <<"X-Riak-vclock">>,
                base64:encode(riak_object:encode_vclock(Vc1))
            }
        ],
    TestHeaderObj2 = riak_api_web_headers:make(MultiClockHeaders),
    {halt, 400, [?TXT_HEADER], Msg2, []} =
        parse_request_headers(TestHeaderObj2, InitCtx),
    ?assertMatch(
        <<
            "Only one x-riak-vclock may be specified"
        >>,
        Msg2
    ).

headers_multiple_value_error2_test() ->
    Vc = vclock:increment('node1@127.0.0.1', vclock:fresh()),
    TestHeaders =
        [
            {
                'Content-Type',
                <<"application/json; charset=utf8, application/octet-stream">>
            },
            {'Content-Encoding', <<"gzip">>},
            {
                <<"X-Riak-vclock">>,
                base64:encode(riak_object:encode_vclock(Vc))
            }
        ],
    InitCtx =
        #context{
            method = 'PUT',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>,
            client = dummy
        },
    TestHeaderObj = riak_api_web_headers:make(TestHeaders),
    {halt, 415, Hdrs, Msg, []} =
        parse_request_headers(TestHeaderObj, InitCtx),
    ?assertMatch(
        <<"Only one Content-Type may be specified">>,
        Msg
    ),
    ?assertMatch(
        [{'Content-Type', <<"text/plain">>}, {<<"Accept-Post">>, <<"*/*">>}],
        lists:sort(Hdrs)
    ),
    MissingHeader =
        [
            {'Content-Encoding', <<"gzip">>},
            {
                <<"X-Riak-vclock">>,
                base64:encode(riak_object:encode_vclock(Vc))
            }
        ],
    {halt, 415, Hdrs, MissingMsg, []} =
        parse_request_headers(
            riak_api_web_headers:make(MissingHeader),
            InitCtx
        ),
    ?assertMatch(<<"Missing Content-Type request header">>, MissingMsg).

query_params_positive_test() ->
    Ctx =
        #context{
            client = dummy,
            method = 'POST',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>
        },
    URI1 =
        <<
            "types/T/buckets/B/keys/K?timeout=10"
            "&asis&returnbody=true&basic_quorum=true"
            "&dw=0&pw=1"
            "&sync_on_write=one"
        >>,
    QP1 = extract_params(URI1),
    {ok, CtxUpd} = parse_query_params(QP1, Ctx),
    PutOpts = CtxUpd#context.put_options,
    ?assertMatch(0, maps:get(dw, PutOpts)),
    ?assertMatch(1, maps:get(pw, PutOpts)),
    ?assertMatch(one, maps:get(sync_on_write, PutOpts)),
    ?assertMatch(true, maps:get(returnbody, PutOpts)),
    ?assertMatch(true, maps:get(basic_quorum, PutOpts)),
    ?assertMatch(10, maps:get(timeout, PutOpts)).

extract_params(URI) ->
    uri_string:dissect_query(
        maps:get(
            query,
            uri_string:normalize(URI, [return_map])
        )
    ).

-endif.
