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
%% @doc Handler for HTTP API requests to read an object ('GET' or 'HEAD'
%% requests)

-module(riak_kv_web_object_read).
-include_lib("kernel/include/logger.hrl").
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

-export(
    [
        produce_response/1
    ]
).

-define(GET_DEFAULTS, #{
    r => default,
    pr => default,
    node_confirms => default,
    n_val => default,
    notfound_ok => default,
    deletedvclock => true,
    basic_quorum => default,
    sloppy_quorum => default,
    return_body => true,
    timeout => undefined
}).

-define(NOT_FOUND(Headers, Body, Ctx), {
    ok,
    {404, Headers, <<"not found">>, true, UpdBody},
    Context
}).

-type get_options() ::
    #{
        r => pos_integer() | default | quorum | all,
        pr => non_neg_integer() | default | quorum | all,
        node_confirms => non_neg_integer() | default | quorum | all,
        n_val => pos_integer() | default,
        notfound_ok => boolean() | default,
        deletedvclock => true,
        basic_quorum => boolean() | default,
        sloppy_quorum => boolean() | default,
        return_body => boolean(),
        timeout => pos_integer() | undefined
    }.

-type get_option_key() ::
    r
    | pr
    | node_confirms
    | n_val
    | notfound_ok
    | basic_quorum
    | sloppy_quorum
    | timeout.

-record(context, {
    client = riak_kv_web_common:get_client() ::
        riak_client:riak_client() | dummy,
    method :: 'GET' | 'HEAD',
    bucket :: riak_object:bucket(),
    key :: riak_object:key(),
    get_options = ?GET_DEFAULTS :: get_options(),
    vtag :: binary() | undefined,
    accepted_types = all :: list(binary()) | all,
    multipart_accepted = false :: boolean()
}).

-type context() :: #context{}.

-export_type([get_options/0]).

%% ===================================================================
%% Callback functions
%% ===================================================================

-spec match_route(
    riak_api_web_acceptor:method(),
    unicode:chardata(),
    list(unicode:chardata())
) ->
    no_match
    | {method_not_allowed, list(riak_api_web_acceptor:method())}
    | {ok, riak_api_web_handler:limits(), context()}.
match_route(
    Method,
    _Path,
    [<<"types">>, BucketType, <<"buckets">>, Bucket, <<"keys">>, Key]
) when is_binary(Key) ->
    case Method of
        Method when Method == 'GET'; Method == 'HEAD' ->
            Context =
                #context{
                    method = Method,
                    bucket = riak_kv_web_common:set_bucket(BucketType, Bucket),
                    key = Key
                },
            {ok, size_limits(), Context};
        Method when Method == 'PUT'; Method == 'POST'; Method == 'DELETE' ->
            no_match;
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
match_route(_Method, _Path, _SplitPath) ->
    no_match.

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
            riak_kv_get
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
        maybe_set_vtag(Params, Ctx2)
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
        <<"multipart/mixed">> ->
            {ok, Ctx#context{multipart_accepted = true}};
        <<"*/*">> ->
            {ok, Ctx};
        CTL when is_list(CTL) ->
            {ok, Ctx#context{accepted_types = CTL}};
        CT when is_binary(CT) ->
            {ok, Ctx#context{accepted_types = [CT]}};
        undefined ->
            {ok, Ctx}
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
    case riak_kv_web_common:confirm_empty_body(RqBdy) of
        {ok, UpdBody} ->
            GetResponse =
                riak_client:get(
                    Context#context.bucket,
                    Context#context.key,
                    maps:to_list(Context#context.get_options),
                    Context#context.client
                ),
            case GetResponse of
                {ok, RObj} ->
                    {Code, Headers, Body} =
                        produce_response(RObj, Context),
                    {ok, {Code, Headers, Body, true, UpdBody}, Context};
                {error, notfound} ->
                    %% Not a halt response - as connection may keepalive
                    ?NOT_FOUND([?TXT_HEADER], UpdBody, Context);
                {error, {deleted, VClock}} ->
                    Headers =
                        [
                            ?TXT_HEADER,
                            {?HEAD_DELETED, <<"true">>},
                            {
                                ?HEAD_VCLOCK,
                                base64:encode(
                                    riak_object:encode_vclock(VClock)
                                )
                            }
                        ],
                    ?NOT_FOUND(Headers, UpdBody, Context);
                Error ->
                    halt_error(Error)
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

-spec maybe_set_vtag(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()}.
maybe_set_vtag(QueryParams, Context) ->
    case lists:keyfind(<<"vtag">>, 1, QueryParams) of
        false ->
            {ok, Context};
        {<<"vtag">>, VTag} when is_binary(VTag) ->
            {ok, Context#context{vtag = VTag}}
    end.

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
            [<<"r">>, <<"pr">>, <<"n_val">>, <<"node_confirms">>],
            Context#context.get_options
        ),
    case FoldResult of
        UpdOpts when is_map(UpdOpts) ->
            {ok, Context#context{get_options = UpdOpts}};
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
            [<<"basic_quorum">>, <<"notfound_ok">>],
            Context#context.get_options
        ),
    case FoldResult of
        UpdOpts when is_map(UpdOpts) ->
            {ok, Context#context{get_options = UpdOpts}};
        HaltResponse ->
            HaltResponse
    end.

%% ===================================================================
%% Produce Response
%% ===================================================================

-spec halt_error({error, term()}) -> riak_api_web_acceptor:halt_response().
halt_error({error, timeout}) ->
    {halt, 503, [?TXT_HEADER], <<"request timed out">>, []};
halt_error({error, {n_val_violation, N}}) ->
    Msg =
        <<
            "Specified w/dw/pw/node_confirms values invalid"
            " for bucket n value of ~0p"
        >>,
    {halt, 400, [?TXT_HEADER], Msg, [N]};
halt_error({error, {r_val_unsatisfied, Requested, Returned}}) ->
    Msg = <<"R-value unsatisfied: ~p/~p">>,
    {halt, 503, [?TXT_HEADER], Msg, [Requested, Returned]};
halt_error({error, {pr_val_unsatisfied, Requested, Returned}}) ->
    Msg = <<"PR-value unsatisfied: ~p/~p">>,
    {halt, 503, [?TXT_HEADER], Msg, [Requested, Returned]};
halt_error({error, UnexpectedError}) ->
    {halt, 500, [?TXT_HEADER], <<"Error:~n~p~n">>, [UnexpectedError]}.

-spec produce_response(
    riak_object:riak_object()
) ->
    {200 | 300 | 400 | 406, riak_api_web_headers:header_list(), binary()}.
produce_response(Object) ->
    produce_response(
        Object,
        #context{
            client = dummy,
            method = 'GET',
            bucket = riak_object:bucket(Object),
            key = riak_object:key(Object)
        }
    ).

-spec produce_response(
    riak_object:riak_object(),
    context()
) ->
    {200 | 300 | 400 | 406, riak_api_web_headers:header_list(), binary()}.
produce_response(RObj, Ctx) ->
    VcHdr =
        {
            ?HEAD_VCLOCK,
            base64:encode(
                riak_object:encode_vclock(riak_object:vclock(RObj))
            )
        },
    case riak_object:get_contents(RObj) of
        [SingletonObject] ->
            handle_singleton_object(SingletonObject, VcHdr, Ctx);
        Siblings ->
            handle_multiple_objects(Siblings, VcHdr, Ctx)
    end.

-spec handle_singleton_object(
    {riak_object:riak_object_meta(), riak_object:value()},
    {binary(), binary()},
    context()
) ->
    {200 | 400 | 406, riak_api_web_headers:header_list(), binary()}.
handle_singleton_object({MD0, Value0}, VcHdr, Ctx) ->
    {MD, Value} =
        encode_value(MD0, Value0, Ctx#context.method == 'GET'),
    EtHdr =
        {
            'Etag',
            uri_string:quote(riak_object:metadata_fetch(?MD_VTAG, MD))
        },
    LmdHdr =
        {
            'Last-Modified',
            riak_api_web:rfc1123_date(
                riak_object:metadata_fetch(?MD_LASTMOD, MD)
            )
        },
    InitHdrs = [LmdHdr, VcHdr, EtHdr],
    case produce_response_headers(MD, InitHdrs, type, Ctx) of
        Hdrs when is_list(Hdrs) ->
            {200, Hdrs, Value};
        {RspCode, Hdrs, Body} ->
            {RspCode, Hdrs, Body}
    end.

-spec handle_multiple_objects(
    list({riak_object:riak_object_meta(), riak_object:value()}),
    {binary(), binary()},
    context()
) ->
    {200 | 400 | 406, riak_api_web_headers:header_list(), binary()}.
handle_multiple_objects(
    Siblings,
    VcHdr,
    Ctx = #context{multipart_accepted = MAcc, vtag = VTag}
) when MAcc == true, VTag == undefined ->
    Boundary = produce_boundary(),
    EncodedSibs =
        lists:map(
            fun({MD0, V0}) ->
                {MD, V} =
                    encode_value(MD0, V0, Ctx#context.method == 'GET'),
                multipart_body_part(Boundary, MD, V, Ctx)
            end,
            Siblings
        ),
    Terminator = iolist_to_binary([<<"\r\n--">>, Boundary, <<"--\r\n">>]),
    SibValue = iolist_to_binary(EncodedSibs),
    ObjHeaders =
        [
            VcHdr,
            last_modified_header(Siblings),
            {
                'Content-Type',
                iolist_to_binary([<<"multipart/mixed; boundary=">>, Boundary])
            }
        ],
    {200, ObjHeaders, <<SibValue/binary, Terminator/binary>>};
handle_multiple_objects(
    Siblings,
    VcHdr,
    _Ctx = #context{multipart_accepted = MAcc, vtag = VTag}
) when MAcc == false, VTag == undefined ->
    VTags =
        lists:map(
            fun({M, _V}) -> riak_object:metadata_fetch(?MD_VTAG, M) end,
            Siblings
        ),
    LmdHdr = last_modified_header(Siblings),
    Body = [<<"Siblings:\n">>, [[V, <<"\n">>] || V <- VTags]],
    {300, [VcHdr, LmdHdr], Body};
handle_multiple_objects(
    Siblings,
    VcHdr,
    Ctx = #context{vtag = VTag}
) when is_binary(VTag) ->
    ChosenSibs =
        lists:filter(
            fun({MD, _V}) ->
                riak_object:metadata_fetch(?MD_VTAG, MD) == VTag
            end,
            Siblings
        ),
    case ChosenSibs of
        [{MD0, Value0}] ->
            {MD, Value} =
                encode_value(MD0, Value0, Ctx#context.method == 'GET'),
            EtHdr = {'Etag', VTag},
            LmHdr =
                {
                    'Last-Modified',
                    riak_api_web:rfc1123_date(
                        riak_object:metadata_fetch(
                            ?MD_LASTMOD,
                            MD
                        )
                    )
                },
            SibHeaders =
                produce_response_headers(MD, [VcHdr, EtHdr, LmHdr], type, Ctx),
            case SibHeaders of
                Hdrs when is_list(Hdrs) ->
                    {200, Hdrs, Value};
                {RspCode, Hdrs, Body} ->
                    {RspCode, Hdrs, Body}
            end;
        _ ->
            {
                400,
                [?TXT_HEADER],
                iolist_to_binary(
                    io_lib:format(
                        <<"VTag ~s failed to match an individual sibling">>,
                        [VTag]
                    )
                )
            }
    end.

-spec last_modified_header(
    list({riak_object:riak_object_meta(), riak_object:value()})
) ->
    {'Last-Modified', binary()}.
last_modified_header(Siblings) when is_list(Siblings) ->
    LMDs =
        lists:map(
            fun({M, _V}) ->
                riak_object:metadata_fetch(?MD_LASTMOD, M)
            end,
            Siblings
        ),
    LastLMD = lists:last(lists:sort(LMDs)),
    {'Last-Modified', riak_api_web:rfc1123_date(LastLMD)}.

-spec multipart_body_part(
    binary(),
    riak_object:riak_object_meta(),
    riak_object:value(),
    context()
) ->
    binary().
multipart_body_part(Boundary, MD, Val, Ctx) ->
    EtHdr =
        {
            'Etag',
            uri_string:quote(riak_object:metadata_fetch(?MD_VTAG, MD))
        },
    LmdHdr =
        {
            'Last-Modified',
            riak_api_web:rfc1123_date(
                riak_object:metadata_fetch(?MD_LASTMOD, MD)
            )
        },
    Hdrs =
        produce_response_headers(
            MD,
            [EtHdr, LmdHdr],
            type,
            Ctx#context{accepted_types = all}
        ),
    case Hdrs of
        Hdrs when is_list(Hdrs) ->
            BodyL =
                [
                    <<"\r\n--">>,
                    Boundary,
                    <<"\r\n">>,
                    riak_api_web_headers:output_response_block(
                        riak_api_web_headers:make_rsp_header(Hdrs)
                    ),
                    <<"\r\n">>,
                    Val
                ],
            iolist_to_binary(BodyL);
        _ ->
            <<>>
    end.

produce_boundary() ->
    <<I:160/integer>> =
        crypto:hash(sha, term_to_binary({self(), os:timestamp()})),
    integer_to_binary(I, 36).

-spec produce_response_headers(
    riak_object:riak_object_meta(),
    riak_api_web_headers:header_list(),
    type | encoding | meta | index,
    context()
) ->
    riak_api_web_headers:header_list()
    | {200 | 300 | 406, riak_api_web_headers:header_list(), binary()}.
produce_response_headers(MD, Hdrs, type, Context) ->
    ContentType = get_ctype(MD),
    case type_match(ContentType, Context#context.accepted_types) of
        {true, CType} ->
            ExtendedCType =
                case riak_object:metadata_find(?MD_CHARSET, MD) of
                    {ok, CS} when is_binary(CS); is_list(CS); is_atom(CS) ->
                        iolist_to_binary(
                            [CType, <<"; charset=">>, ensure_binary(CS)]
                        );
                    error ->
                        CType
                end,
            produce_response_headers(
                MD,
                [{'Content-Type', ExtendedCType} | Hdrs],
                encoding,
                Context
            );
        {false, _} ->
            {406, [?TXT_HEADER], <<>>}
    end;
produce_response_headers(MD, Hdrs, encoding, Context) ->
    case riak_object:metadata_find(?MD_ENCODING, MD) of
        {ok, Enc} when is_binary(Enc); is_list(Enc); is_atom(Enc) ->
            produce_response_headers(
                MD,
                [{'Content-Encoding', ensure_binary(Enc)} | Hdrs],
                meta,
                Context
            );
        error ->
            produce_response_headers(MD, Hdrs, meta, Context)
    end;
produce_response_headers(MD, Hdrs, meta, Context) ->
    UpdHdrs =
        binary_header_fold(MD, Hdrs, ?MD_USERMETA, ?HEAD_USERMETA_PREFIX),
    produce_response_headers(MD, UpdHdrs, index, Context);
produce_response_headers(MD, Hdrs, index, _Context) ->
    binary_header_fold(MD, Hdrs, ?MD_INDEX, ?HEAD_INDEX_PREFIX).

binary_header_fold(MD, Hdrs, MetaKey, Prefix) ->
    UserMeta =
        case riak_object:metadata_find(MetaKey, MD) of
            {ok, KVL} when is_list(KVL) ->
                KVL;
            _ ->
                []
        end,
    lists:foldl(
        fun({K, V}, Acc) ->
            [
                {
                    <<
                        Prefix/binary,
                        (ensure_binary(K))/binary
                    >>,
                    ensure_binary(V)
                }
                | Acc
            ]
        end,
        Hdrs,
        UserMeta
    ).

-spec get_ctype(riak_object:riak_object_meta()) -> binary().
%% @doc Work out the content type for this object - use the metadata if provided
get_ctype(MD) ->
    case riak_object:metadata_find(?MD_CTYPE, MD) of
        {ok, SingleType} when is_binary(SingleType) ->
            SingleType;
        {ok, TypeAsCharData} when is_list(TypeAsCharData) ->
            ensure_binary(TypeAsCharData);
        error ->
            <<"application/octet-stream">>
    end.

-spec type_match(
    binary(),
    list(binary()) | all
) ->
    {boolean(), binary()}.
type_match(ContentType, all) ->
    {true, ContentType};
type_match(ContentType, AcceptedTypes) ->
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
    case string:split(PrimaryTypeInfo, <<"/">>, leading) of
        [Type, SubType] when is_binary(Type), is_binary(SubType) ->
            {Type, SubType};
        _NotSplitAsExpected ->
            error
    end.

-spec ensure_binary(atom() | binary() | list()) -> binary().
ensure_binary(B) when is_binary(B) ->
    B;
ensure_binary(A) when is_atom(A) ->
    atom_to_binary(A);
ensure_binary(List) when is_list(List) ->
    iolist_to_binary(List).

%% @doc
%% Need to handle values stored not as binaries but as Erlang terms through
%% the direct Erlang API.  If it is not a binary, we assume it is a term and
%% set the content type as such regardless.
%% Also the context may not be interested in the body, so we cna replace with
%% an empty binary in this case (e.g. 'HEAD' request).
-spec encode_value(
    riak_object:riak_object_meta(),
    binary() | term(),
    boolean()
) ->
    {riak_object:riak_object_meta(), binary()}.
encode_value(MD, Value, true) when is_binary(Value) ->
    {MD, Value};
encode_value(MD, Value, false) when is_binary(Value) ->
    {MD, <<>>};
encode_value(MD, Value, IsGet) ->
    MD0 =
        riak_object:metadata_store(
            ?MD_CTYPE,
            <<"application/x-erlang-binary">>,
            MD
        ),
    case IsGet of
        true ->
            {MD0, term_to_binary(Value)};
        false ->
            {MD0, <<>>}
    end.

%% ===================================================================
%% Internal Functions
%% ===================================================================

-spec set_option(
    get_option_key(),
    non_neg_integer() | quorum | all | boolean(),
    context()
) ->
    context().
set_option(Option, Value, Context) ->
    Context#context{
        get_options = maps:put(Option, Value, Context#context.get_options)
    }.

size_limits() ->
    {
        1024,
        2048,
        % A GET/HEAD can never have a request body
        0
    }.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

accept_filter_test() ->
    Accept1 =
        [
            {
                'Accept',
                [
                    <<"text/html">>,
                    <<"application/xhtml+xml">>,
                    <<"application/xml;q=0.9">>,
                    <<"image/*">>
                ]
            }
        ],
    Headers1 = riak_api_web_headers:make(Accept1),
    DummyCtx =
        #context{
            client = dummy,
            bucket = {<<"Type">>, <<"B">>},
            key = <<"K">>,
            method = 'GET'
        },
    {ok, UpdCtx} = parse_request_headers(Headers1, DummyCtx),
    AcceptedTypes1 = UpdCtx#context.accepted_types,
    ?assert(is_list(AcceptedTypes1)),
    ?assertMatch(
        {true, <<"text/html">>},
        type_match(ensure_binary("text/html"), AcceptedTypes1)
    ),
    ?assertMatch(
        {false, <<"application/json">>},
        type_match(ensure_binary("application/json"), AcceptedTypes1)
    ),
    ?assertMatch(
        {true, <<"image/jpeg">>},
        type_match(ensure_binary("image/jpeg"), AcceptedTypes1)
    ),
    ?assertMatch(
        {false, <<"application/xhtml">>},
        type_match(ensure_binary("application/xhtml"), AcceptedTypes1)
    ),
    ?assertMatch(
        {true, <<"application/xhtml+xml">>},
        type_match(ensure_binary("application/xhtml+xml"), AcceptedTypes1)
    ),
    ?assertMatch(
        {true, <<"application/xml">>},
        type_match(ensure_binary("application/xml"), AcceptedTypes1)
    ),

    HdrList1 =
        produce_response_headers(
            #{<<"content-type">> => "application/xml"}, [], type, UpdCtx
        ),
    ?assertMatch([{'Content-Type', <<"application/xml">>}], HdrList1),
    ?assertMatch(
        {406, [?TXT_HEADER], <<>>},
        produce_response_headers(
            #{<<"content-type">> => "application+badly-formatted"},
            [],
            type,
            UpdCtx
        )
    ),
    ?assertMatch(
        {406, [?TXT_HEADER], <<>>},
        produce_response_headers(
            #{<<"content-type">> => "application/json"}, [], type, UpdCtx
        )
    ).

metadata_format_test() ->
    UserMeta =
        [
            {<<"postCode">>, <<"LS1 4BT">>},
            {<<"postCode">>, <<"LS11_0ES">>},
            {<<"familyName">>, <<"ROBERTS">>}
        ],
    IndexSpecs =
        [
            {<<"pc_bin">>, <<"LS1_4BT|ROBERTS">>},
            {<<"pc_bin">>, <<"LS11_0ES|ROBERTS">>},
            {<<"family_bin">>, <<"ROBERTS|LS1_4BT.LS11_0ES">>}
        ],
    VTag = base64:encode(term_to_binary({'node1', os:timestamp()})),
    LMD = os:timestamp(),
    MetaData =
        maps:from_list(
            [
                {?MD_USERMETA, UserMeta},
                {?MD_INDEX, IndexSpecs},
                {?MD_VTAG, VTag},
                {?MD_LASTMOD, LMD},
                {?MD_CTYPE, "application/json"}
            ]
        ),
    O = riak_object:new({<<"BT">>, <<"B">>}, <<"K">>, <<"ObjVal">>),
    O1 = riak_object:update_metadata(O, MetaData),
    O2 = riak_object:apply_updates(O1),
    Ctx =
        #context{
            client = dummy,
            method = 'GET',
            bucket = {<<"BT">>, <<"B">>},
            key = <<"K">>
        },
    {200, HdrList, <<"ObjVal">>} = produce_response(O2, Ctx),
    ?assertMatch(10, length(HdrList)),
    HeaderMap =
        riak_api_web_headers:enter_from_list(
            HdrList,
            riak_api_web_headers:make_rsp_header(
                [{'Server', <<"RiakEUnit/4.0 SilverMachine">>}]
            )
        ),
    HeaderBin = riak_api_web_headers:output_response_block(HeaderMap),
    % confirm multiple index entries folded into one line
    ?assertNotMatch(
        nomatch,
        string:find(
            HeaderBin,
            <<"X-Riak-Index-pc_bin: LS1_4BT\|ROBERTS, LS11_0ES|ROBERTS">>
        )
    ),
    % likewise, multiple metadata with the same key folded into list
    ?assertNotMatch(
        nomatch,
        string:find(
            HeaderBin,
            <<"X-Riak-Meta-postCode: LS1 4BT, LS11_0ES">>
        )
    ),
    ErlLMD =
        iolist_to_binary(
            io_lib:format(
                <<"Last-Modified: ~s">>,
                [httpd_util:rfc1123_date(calendar:now_to_local_time(LMD))]
            )
        ),
    ?assertNotMatch(
        nomatch,
        string:find(
            HeaderBin,
            ErlLMD
        )
    ),
    SWs = os:system_time(microsecond),
    lists:map(
        fun(_I) ->
            {200, HdrList, <<"ObjVal">>} = produce_response(O2, Ctx),
            HeaderMap =
                riak_api_web_headers:enter_from_list(
                    HdrList,
                    riak_api_web_headers:make_rsp_header(
                        [{'Server', <<"RiakEUnit/4.0 SilverMachine">>}]
                    )
                )
        end,
        lists:seq(1, 1000)
    ),
    SWe = os:system_time(microsecond),
    io:format(
        user,
        "1000 response header blocks in ~w microseconds~n",
        [SWe - SWs]
    ).

validate_timeout_test() ->
    Ctx =
        #context{
            client = dummy,
            method = 'GET',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>
        },
    QP1 = extract_params(<<"types/T/buckets/B/keys/K?timeout=10">>),
    {ok, Ctx1} = validate_timeout(QP1, Ctx),
    ?assertMatch(10, maps:get(timeout, Ctx1#context.get_options)),
    QP2 = extract_params(<<"types/T/buckets/B/keys/K?timeout=-2">>),
    ?assertMatch(
        {halt, 400, [?TXT_HEADER], <<"Bad timeout value ~0p">>, [<<"-2">>]},
        validate_timeout(QP2, Ctx)
    ),
    QP3 = extract_params(<<"types/T/buckets/B/keys/K?timeout=XC">>),
    ?assertMatch(
        {halt, 400, [?TXT_HEADER], <<"Bad timeout value ~0p">>, [<<"XC">>]},
        validate_timeout(QP3, Ctx)
    ),
    QP4 = extract_params(<<"types/T/buckets/B/keys/K?timoeut=100">>),
    ?assertMatch(
        % Not timeout extracted - misspelling
        {ok, Ctx},
        validate_timeout(QP4, Ctx)
    ).

validate_counts_test() ->
    Ctx =
        #context{
            client = dummy,
            method = 'GET',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>
        },
    QP1 =
        extract_params(
            <<"types/T/buckets/B/keys/K?r=all&pr=2&node_confirms=1&n_val=default">>
        ),
    io:format("Params ~0p~n", [QP1]),
    {ok, Ctx1} = validate_counts(QP1, Ctx),
    ?assertMatch(all, maps:get(r, Ctx1#context.get_options)),
    ?assertMatch(2, maps:get(pr, Ctx1#context.get_options)),
    ?assertMatch(1, maps:get(node_confirms, Ctx1#context.get_options)),
    ?assertMatch(default, maps:get(n_val, Ctx1#context.get_options)),
    QP2 =
        extract_params(
            <<"types/T/buckets/B/keys/K?r=all&pr=2&node_confirms=1&nval=-1">>
        ),
    {ok, Ctx2} = validate_counts(QP2, Ctx),
    % n_val still default due to misspelling
    ?assertMatch(Ctx1, Ctx2),
    QP3 =
        extract_params(
            <<"types/T/buckets/B/keys/K?r=-1&pr=2&node_confirms=1">>
        ),
    {halt, 400, _, _, [<<"r">>]} = validate_counts(QP3, Ctx),
    {ok, Ctx3} = parse_query_params(QP2, Ctx),
    ?assertMatch(Ctx2, Ctx3).

validate_bools_test() ->
    Ctx =
        #context{
            client = dummy,
            method = 'GET',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>
        },
    QP1 =
        extract_params(
            <<"types/T/buckets/B/keys/K?basic_quorum=true&notfound_ok=false">>
        ),
    {ok, Ctx1} = validate_booleans(QP1, Ctx),
    ?assertMatch(false, maps:get(notfound_ok, Ctx1#context.get_options)),
    ?assertMatch(true, maps:get(basic_quorum, Ctx1#context.get_options)),
    QP2 =
        extract_params(
            <<"types/T/buckets/B/keys/K?basic_quorum=true&notfound_ok=flase">>
        ),
    {halt, 400, _, _, [<<"notfound_ok">>]} = validate_booleans(QP2, Ctx),
    QP3 =
        extract_params(
            <<"types/T/buckets/B/keys/K?basic_quorum=true&notfoundok=false">>
        ),
    {ok, Ctx3} = validate_booleans(QP3, Ctx),
    ?assertMatch(default, maps:get(notfound_ok, Ctx3#context.get_options)),
    % Misspell notfound_ok
    ?assertMatch(true, maps:get(basic_quorum, Ctx3#context.get_options)).

extract_params(URI) ->
    uri_string:dissect_query(
        maps:get(
            query,
            uri_string:normalize(URI, [return_map])
        )
    ).

with_bucket_prop_test_() ->
    {
        setup,
        fun() ->
            meck:new(riak_core_bucket),
            meck:expect(riak_core_bucket, get_bucket, fun(_) -> [] end)
        end,
        fun(_) -> meck:unload(riak_core_bucket) end,
        [
            {"Singleton response", fun singleton_response/0},
            {"Sibling response", fun sibling_response/0}
        ]
    }.

singleton_response() ->
    LastMod = os:timestamp(),
    ET1 = <<"a123456zz">>,
    O0 = riak_object:new({<<"T">>, <<"B">>}, <<"K">>, <<"willy">>),
    MD0 = get_metadata(LastMod, ET1),
    O1 =
        riak_object:increment_vclock(
            riak_object:update_metadata(
                riak_object:update_value(
                    O0,
                    <<"gnonto">>
                ),
                MD0
            ),
            x
        ),
    OM1 = riak_object:syntactic_merge(O0, O1),
    Ctx =
        #context{
            client = dummy,
            method = 'GET',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>
        },
    {200, HdrList1, <<"gnonto">>} = produce_response(OM1, Ctx),
    ?assertMatch({'Etag', ET1}, lists:keyfind('Etag', 1, HdrList1)),
    ErlTerm = maps:put(name, <<"gnonto">>, maps:new()),
    O2 =
        riak_object:increment_vclock(
            riak_object:update_value(
                O1,
                ErlTerm
            ),
            y
        ),
    OM2 = riak_object:syntactic_merge(OM1, O2),
    {200, HdrList2, ConvertedVal2} =
        produce_response(OM2, Ctx),
    ?assertMatch(ErlTerm, binary_to_term(ConvertedVal2)),
    ?assertMatch(
        {'Content-Type', <<"application/x-erlang-binary">>},
        lists:keyfind('Content-Type', 1, HdrList2)
    ),
    ?assertMatch({'Etag', ET1}, lists:keyfind('Etag', 1, HdrList2)),
    PlainTextValue = "gnonto",
    O3 =
        riak_object:increment_vclock(
            riak_object:update_value(
                O2,
                PlainTextValue
            ),
            z
        ),
    OM3 = riak_object:syntactic_merge(OM2, O3),
    {200, HdrList3, ConvertedVal3} =
        produce_response(OM3, Ctx),
    ?assertMatch("gnonto", binary_to_term(ConvertedVal3)),
    ?assertMatch({'Etag', ET1}, lists:keyfind('Etag', 1, HdrList3)),
    CtxHead =
        #context{
            client = dummy,
            method = 'HEAD',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>
        },
    {200, HdrList4, <<>>} =
        produce_response(OM3, CtxHead),
    ?assertMatch({'Etag', ET1}, lists:keyfind('Etag', 1, HdrList4)).

sibling_response() ->
    LastMod1 = os:timestamp(),
    LastMod2 = setelement(2, LastMod1, element(2, LastMod1) + 1),
    ET1 = <<"a123456zz">>,
    ET2 = <<"b123456zz">>,
    O0 = riak_object:new({<<"T">>, <<"B">>}, <<"K">>, <<"willy">>),
    O1 =
        riak_object:increment_vclock(
            riak_object:update_metadata(
                riak_object:update_value(
                    O0,
                    <<"gnonto">>
                ),
                get_metadata(LastMod1, ET1)
            ),
            x
        ),
    O2 =
        riak_object:increment_vclock(
            riak_object:update_metadata(
                riak_object:update_value(
                    O0,
                    <<"gnonto mk2">>
                ),
                get_metadata(LastMod2, ET2)
            ),
            y
        ),

    OM1 = riak_object:reconcile([O1, O2], true),
    Ctx1 =
        #context{
            client = test,
            method = 'GET',
            bucket = {<<"T">>, <<"B">>},
            key = <<"K">>,
            vtag = ET1
        },
    {200, HdrList1, <<"gnonto">>} = produce_response(OM1, Ctx1),
    ?assertMatch({'Etag', ET1}, lists:keyfind('Etag', 1, HdrList1)),
    {'Last-Modified', FormattedDate1} =
        lists:keyfind('Last-Modified', 1, HdrList1),
    ?assertMatch(
        FormattedDate1,
        iolist_to_binary(
            httpd_util:rfc1123_date(
                calendar:now_to_local_time(LastMod1)
            )
        )
    ),
    ?assertMatch(
        {200, HdrList1, <<>>},
        produce_response(OM1, Ctx1#context{method = 'HEAD'})
    ),
    Ctx2 = Ctx1#context{multipart_accepted = false, vtag = undefined},
    {300, HdrList2, SibBody2} = produce_response(OM1, Ctx2),
    ?assert(
        lists:keyfind(?HEAD_VCLOCK, 1, HdrList2) ==
            lists:keyfind(?HEAD_VCLOCK, 1, HdrList1)
    ),
    ?assertNotMatch(
        nomatch,
        string:find(SibBody2, ET1)
    ),
    ?assertNotMatch(
        nomatch,
        string:find(SibBody2, ET2)
    ),
    %% Last Modfied Date should be later of two dates (from sibling 2)
    {'Last-Modified', FormattedDate2} =
        lists:keyfind('Last-Modified', 1, HdrList2),
    ?assertMatch(
        FormattedDate2,
        iolist_to_binary(
            httpd_util:rfc1123_date(
                calendar:now_to_local_time(LastMod2)
            )
        )
    ),

    %% Return multipart body
    Ctx3 = Ctx1#context{multipart_accepted = true, vtag = undefined},
    {200, HdrList3, MultiBody3} = produce_response(OM1, Ctx3),
    {'Last-Modified', FormattedDate3} =
        lists:keyfind('Last-Modified', 1, HdrList3),
    ?assertMatch(
        FormattedDate3,
        iolist_to_binary(
            httpd_util:rfc1123_date(
                calendar:now_to_local_time(LastMod2)
            )
        )
    ),
    ?assert(
        lists:keyfind(?HEAD_VCLOCK, 1, HdrList3) ==
            lists:keyfind(?HEAD_VCLOCK, 1, HdrList1)
    ),
    {'Content-Type', MultipartHeader} =
        lists:keyfind('Content-Type', 1, HdrList3),
    <<
        "multipart/mixed; boundary=",
        Boundary/binary
    >> = MultipartHeader,
    [<<"\r\n--">>, SS1] = string:split(MultiBody3, Boundary),
    [B1, SS2] = string:split(SS1, Boundary),
    [B2, <<"--\r\n">>] = string:split(SS2, Boundary),
    ?assertNotMatch(
        nomatch,
        string:find(
            B1,
            <<<<"Etag: ">>/binary, ET1/binary>>
        )
    ),
    ?assertMatch(
        nomatch,
        string:find(
            B1,
            <<<<"Etag: ">>/binary, ET2/binary>>
        )
    ),
    ?assertNotMatch(
        nomatch,
        string:find(
            B2,
            <<<<"Etag: ">>/binary, ET2/binary>>
        )
    ),
    ?assertMatch(
        nomatch,
        string:find(
            B2,
            <<<<"Etag: ">>/binary, ET1/binary>>
        )
    ),
    ?assertNotMatch(
        nomatch,
        string:find(
            B1,
            <<"Content-Type: application/octet-stream">>
        )
    ),
    ?assertNotMatch(
        nomatch,
        string:find(
            B2,
            <<"Content-Type: application/octet-stream">>
        )
    ),
    ?assertNotMatch(
        nomatch,
        string:find(
            B1,
            <<"\r\ngnonto\r\n">>
        )
    ),
    ?assertNotMatch(
        nomatch,
        string:find(
            B2,
            <<"\r\ngnonto mk2\r\n">>
        )
    ),

    % Non-unique vtags
    O3 =
        riak_object:increment_vclock(
            riak_object:update_metadata(
                riak_object:update_value(
                    O0,
                    <<"gnonto mk3">>
                ),
                get_metadata(LastMod1, ET1)
            ),
            z
        ),
    OM2 = riak_object:reconcile([O1, O2, O3], true),
    {400, _, <<"VTag a123456zz failed to match an individual sibling">>} =
        produce_response(OM2, Ctx1).

get_metadata(LastMod, ETag) ->
    riak_object:metadata_fromlist(
        [
            {?MD_CTYPE, <<"application/octet-stream">>},
            {
                ?MD_INDEX,
                [
                    {<<"familyname_bin">>, <<"gnonto">>},
                    {<<"dob_bin">>, <<"20031105">>}
                ]
            },
            {?MD_VTAG, ETag},
            {?MD_LASTMOD, LastMod}
        ]
    ).

-endif.
