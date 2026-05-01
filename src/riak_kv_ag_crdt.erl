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
%% @doc Handler for HTTP CRDT requests (legacy)

-module(riak_kv_ag_crdt).

-if(?OTP_RELEASE == 26).
-feature(maybe_expr, enable).
-endif.

-include("riak_kv_web.hrl").
-include("riak_kv_types.hrl").

-behaviour(riak_api_web_handler).

-export(
    [
        match_route/3,
        check_permissions/5,
        parse_query_params/2,
        parse_request_headers/2,
        process_request/2,
        record_request/3
    ]
).

-define(OPTION_DEFAULTS, #{
    r => default,
    w => default,
    dw => default,
    pw => default,
    pr => default,
    node_confirms => default,
    n_val => default,
    basic_quorum => default,
    sloppy_quorum => default,
    returnbody => false,
    notfound_ok => default,
    include_context => true,
    timeout => undefined
}).

-record(context, {
    client = riak_client:new(node(), undefined) :: riak_client:riak_client(),
    method :: 'GET' | 'HEAD' | 'POST',
    bucket :: riak_object:bucket(),
    key :: riak_object:key() | {generated, riak_object:key()},
    crdt_mod :: module() | undefined,
    crdt_type :: riak_kv_crdt_json:toplevel_type() | undefined,
    crdt_options = ?OPTION_DEFAULTS :: crdt_options()
}).

-type crdt_option_key() ::
    r
    | w
    | dw
    | pw
    | pr
    | node_confirms
    | n_val
    | basic_quorum
    | sloppy_quorum
    | notfound_ok
    | returnbody
    | include_context
    | timeout.

-type crdt_options() ::
    #{
        r => pos_integer() | default | quorum | all,
        w => pos_integer() | default | quorum | all,
        dw => non_neg_integer() | default | quorum | all,
        pw => non_neg_integer() | default | quorum | all,
        pr => non_neg_integer() | default | quorum | all,
        n_val => pos_integer() | default,
        node_confirms => non_neg_integer() | default | quorum | all,
        basic_quorum => boolean() | default,
        sloppy_quorum => boolean() | default,
        returnbody => boolean(),
        include_context => true,
        notfound_ok => boolean() | default,
        timeout => pos_integer() | undefined
    }.

-type context() :: #context{}.

%% ===================================================================
%% Callback functions
%% ===================================================================

-spec match_route(
    riak_api_web_acceptor:method(),
    unicode:chardata(),
    list(unicode:chardata() | {generated, binary()})
) ->
    nomatch
    | {method_not_allowed, list(riak_api_web_acceptor:method())}
    | {ok, riak_api_web_handler:limits(), context()}.
match_route(
    Method,
    _,
    [<<"types">>, BucketType, <<"buckets">>, Bucket, <<"datatypes">>, Key]
) when
    BucketType =/= <<"default">>
->
    case Method of
        Method when Method == 'POST' ->
            MaxUpdateSize =
                application:get_env(
                    riak_kv,
                    max_crdt_update_size,
                    4 * 1024 * 1024
                ),
            {
                ok,
                {32, 2048, MaxUpdateSize},
                #context{
                    bucket = riak_kv_web_common:set_bucket(BucketType, Bucket),
                    method = 'POST',
                    key = Key
                }
            };
        Method when Method == 'GET'; Method == 'HEAD' ->
            {
                ok,
                {32, 2048, 0},
                #context{
                    bucket = riak_kv_web_common:set_bucket(BucketType, Bucket),
                    method = Method,
                    key = Key
                }
            };
        _ ->
            {method_not_allowed, ['GET', 'HEAD', 'POST']}
    end;
match_route(
    'POST',
    Path,
    [<<"types">>, BucketType, <<"buckets">>, Bucket, <<"datatypes">>]
) ->
    K = {generated, iolist_to_binary(riak_core_util:unique_id_62())},
    match_route(
        'POST',
        Path,
        [<<"types">>, BucketType, <<"buckets">>, Bucket, <<"datatypes">>, K]
    );
match_route(_, _, _) ->
    nomatch.

%% @doc check_permissions for using this module or route
-spec check_permissions(
    riak_api_web_headers:headers(),
    riak_api_web_socket:scheme(),
    riak_api_web_handler:peer_ip(),
    riak_api_web_handler:peer_cert(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
check_permissions(ReqHeaders, Scheme, Peer, _Cert, Ctx) ->
    GrantSought =
        case Ctx#context.method of
            'POST' ->
                "riak_kv.put";
            _ ->
                "riak_kv.get"
        end,
    Check =
        riak_kv_web_common:check_permissions(
            ReqHeaders,
            Scheme,
            Peer,
            Ctx#context.bucket,
            GrantSought
        ),
    case Check of
        true ->
            check_crdt_type(Ctx);
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
        {ok, Ctx2}
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
    case riak_kv_web_common:accept_json_only(ReqHeaders) of
        ok ->
            {ok, Ctx};
        HaltResponse ->
            HaltResponse
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
process_request(none, #context{method = Method, crdt_mod = Mod} = Ctx) when
    Method == 'GET'; Method == 'HEAD'
->
    GetOptions =
        riak_kv_web_common:filter_options(
            Ctx#context.crdt_options
        ),
    GetResult =
        riak_client:get(
            Ctx#context.bucket,
            Ctx#context.key,
            [{crdt_op, Mod} | GetOptions],
            Ctx#context.client
        ),
    case GetResult of
        {ok, RObj} ->
            JsonBody = iolist_to_binary(produce_json(RObj, Ctx, Mod)),
            case Method of
                'GET' ->
                    {ok, {200, [?JSN_HEADER], JsonBody, true, none}, Ctx};
                'HEAD' ->
                    RspHdrs =
                        [?JSN_HEADER, {'Content-Length', byte_size(JsonBody)}],
                    {ok, {200, RspHdrs, <<>>, true, none}, Ctx}
            end;
        {error, Reason} ->
            handle_common_error(Reason, Ctx)
    end;
process_request(RqBdy, #context{method = 'POST', crdt_mod = Mod} = Ctx) ->
    case riak_api_web_body:get_body(RqBdy, all, 60000) of
        {error, content_too_large} ->
            {halt, 413, [], <<>>, []};
        {ObjBody, UpdRqBdy} when is_binary(ObjBody) ->
            case check_post_body(ObjBody, Ctx) of
                {ok, {_UpdType, UpdOp, UpdOpCtx}} ->
                    {Type, Bucket} = Ctx#context.bucket,
                    {Key, LocHdr} =
                        case Ctx#context.key of
                            {generated, K} ->
                                Location = set_location(Type, Bucket, K),
                                {K, [{'Location', Location}]};
                            K when is_binary(K) ->
                                {K, []}
                        end,
                    O = riak_kv_crdt:new({Type, Bucket}, Key, Mod),
                    PutOptions =
                        riak_kv_web_common:filter_options(
                            Ctx#context.crdt_options
                        ),
                    CrdtOp = #crdt_op{mod = Mod, op = UpdOp, ctx = UpdOpCtx},
                    Options =
                        [
                            {crdt_op, CrdtOp},
                            {retry_put_coordinator_failure, false}
                        ] ++
                            PutOptions,
                    case riak_client:put(O, Options, Ctx#context.client) of
                        ok ->
                            {ok, {204, LocHdr, <<>>, true, UpdRqBdy}, Ctx};
                        {ok, RObj} ->
                            JsonBody =
                                iolist_to_binary(produce_json(RObj, Ctx, Mod)),
                            {
                                ok,
                                {
                                    200,
                                    [?JSN_HEADER | LocHdr],
                                    JsonBody,
                                    true,
                                    UpdRqBdy
                                },
                                Ctx
                            };
                        {error, Reason} ->
                            handle_common_error(Reason, Ctx)
                    end;
                HaltResponse ->
                    HaltResponse
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
%% Validation Functions
%% ===================================================================

-spec validate_timeout(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
validate_timeout(Params, Ctx) ->
    case riak_kv_web_common:get_timeout(Params) of
        {ok, none} ->
            {ok, Ctx};
        {ok, Timeout} ->
            {ok, set_option(timeout, Timeout, Ctx)};
        HaltResponse ->
            HaltResponse
    end.

-spec validate_counts(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
validate_counts(Params, Context) ->
    % List of allowed integer parameters based on riak_kv_pb_crdt
    ParamList =
        case Context#context.method of
            'POST' ->
                [
                    <<"w">>,
                    <<"dw">>,
                    <<"pw">>,
                    <<"n_val">>,
                    <<"node_confirms">>
                ];
            _ ->
                [<<"r">>, <<"pr">>, <<"n_Val">>]
        end,
    FoldResult =
        riak_kv_web_common:count_fold(
            Params,
            ParamList,
            Context#context.crdt_options
        ),
    case FoldResult of
        UpdOpts when is_map(UpdOpts) ->
            {ok, Context#context{crdt_options = UpdOpts}};
        HaltResponse ->
            HaltResponse
    end.

-spec validate_booleans(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
validate_booleans(Params, Context) ->
    % List of allowed boolean parameters based on riak_kv_pb_crdt
    ParamList =
        case Context#context.method of
            'POST' ->
                [<<"sloppy_quorum">>, <<"returnbody">>];
            _ ->
                [<<"sloppy_quorum">>, <<"basic_quorum">>, <<"notfound_ok">>]
        end,
    FoldResult =
        riak_kv_web_common:boolean_fold(
            Params,
            ParamList,
            Context#context.crdt_options
        ),
    case FoldResult of
        UpdOpts when is_map(UpdOpts) ->
            {ok, Context#context{crdt_options = UpdOpts}};
        HaltResponse ->
            HaltResponse
    end.

%% ===================================================================
%% Internal Functions
%% ===================================================================

handle_common_error(Reason, Ctx) ->
    case Reason of
        too_many_fails ->
            ErrMsg = <<"Too many write failures to satisfy W/DW">>,
            {halt, 503, [?TXT_HEADER], ErrMsg, []};
        timeout ->
            {halt, 503, [?TXT_HEADER], <<"request timed out">>, []};
        notfound ->
            NotFndMsg =
                #{
                    <<"type">> =>
                        atom_to_binary(
                            riak_kv_crdt:from_mod(Ctx#context.crdt_mod),
                            utf8
                        ),
                    <<"error">> => <<"notfound">>
                },
            {halt, 404, [?JSN_HEADER], riak_kv_wm_json:encode(NotFndMsg), []};
        {deleted, _VClock} ->
            RspHdrs = [{?HEAD_DELETED, <<"true">>}, ?JSN_HEADER],
            NotFndMsg =
                #{
                    <<"type">> =>
                        atom_to_binary(
                            riak_kv_crdt:from_mod(Ctx#context.crdt_mod),
                            utf8
                        ),
                    <<"error">> => <<"notfound">>
                },
            {halt, 404, RspHdrs, riak_kv_wm_json:encode(NotFndMsg), []};
        {n_val_violation, N} ->
            ErrMsg =
                <<
                    "Specified w/dw/pw/node_confirms values invalid for"
                    " bucket n value of ~0p"
                >>,
            {halt, 400, [?TXT_HEADER], ErrMsg, [N]};
        {r_val_unsatisfied, Requested, Returned} ->
            ErrMsg = "R-value unsatisfied: ~p/~p",
            {halt, 503, [?TXT_HEADER], ErrMsg, [Returned, Requested]};
        {dw_val_unsatisfied, Requested, Returned} ->
            ErrMsg = <<"DW-value unsatisfied: ~p/~p">>,
            {halt, 503, [?TXT_HEADER], ErrMsg, [Returned, Requested]};
        {pr_val_unsatisfied, Requested, Returned} ->
            ErrMsg = <<"PR-value unsatisfied: ~p/~p">>,
            {halt, 503, [?TXT_HEADER], ErrMsg, [Returned, Requested]};
        {pw_val_unsatisfied, Requested, Returned} ->
            ErrMsg = <<"PW-value unsatisfied: ~p/~p">>,
            {halt, 503, [?TXT_HEADER], ErrMsg, [Returned, Requested]};
        {node_confirms_val_unsatisfied, Requested, Returned} ->
            ErrMsg = <<"node_confirms-value unsatisfied: ~p/~p">>,
            {halt, 503, [?TXT_HEADER], ErrMsg, [Returned, Requested]};
        Err ->
            {halt, 500, [?TXT_HEADER], <<"Error:~n~0p~n">>, [Err]}
    end.

-spec produce_json(riak_object:riak_object(), context(), module()) -> binary().
produce_json(RObj, Ctx, Mod) ->
    IncludeContext =
        maps:get(include_context, (Ctx#context.crdt_options), true),
    Type = riak_kv_crdt:from_mod(Mod),
    {{RespCtx, Value}, Stats} = riak_kv_crdt:value(RObj, Mod),
    _ = [ok = riak_kv_stat:update(S) || S <- Stats],
    Body =
        riak_kv_crdt_json:fetch_response_to_json(
            Type,
            Value,
            case IncludeContext of
                true ->
                    RespCtx;
                _ ->
                    undefined
            end,
            riak_kv_crdt:mod_map(Type)
        ),
    mochijson2:encode(Body).

-spec check_post_body(
    binary(),
    context()
) ->
    {ok, riak_kv_crdt_json:update()} | riak_api_web_acceptor:halt_response().
check_post_body(ReqBody, #context{crdt_type = CRDTType}) ->
    try
        JSON = mochijson2:decode(ReqBody),
        Update =
            {CRDTType, _Op, _Context} =
            riak_kv_crdt_json:update_request_from_json(
                CRDTType,
                JSON,
                riak_kv_crdt:mod_map(CRDTType)
            ),
        {ok, Update}
    catch
        throw:{invalid_operation, {BadType, BadOp}} ->
            {
                halt,
                400,
                [?TXT_HEADER],
                <<"Invalid operation on datatype '~s': ~s">>,
                [BadType, mochijson2:encode(BadOp)]
            };
        throw:{invalid_field_name, Field} ->
            {
                halt,
                400,
                [?TXT_HEADER],
                <<"Invalid map field name '~s'">>,
                [Field]
            };
        throw:invalid_utf8 ->
            ErrMsg = <<"Malformed JSON submitted, invalid UTF-8">>,
            {halt, 400, [?TXT_HEADER], ErrMsg, []};
        _Other:Reason ->
            ErrMsg = <<"Couldn't decode JSON: ~p">>,
            {halt, 400, [?TXT_HEADER], ErrMsg, [Reason]}
    end.

-spec check_crdt_type(
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
check_crdt_type(Context) ->
    {Type, Bucket} = Context#context.bucket,
    case riak_core_bucket:get_bucket({Type, Bucket}) of
        BProps when is_list(BProps) ->
            DataType = proplists:get_value(datatype, BProps),
            AllowMult = proplists:get_value(allow_mult, BProps),
            Mod = riak_kv_crdt:to_mod(DataType),
            case {AllowMult, riak_kv_crdt:supported(Mod)} of
                {false, _} ->
                    ErrMsg = <<"Bucket must be allow_mult=true">>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, []};
                {_, false} ->
                    ErrMsg =
                        <<"Bucket datatype '~s' is not a supported type">>,
                    {halt, 400, [?TXT_HEADER], ErrMsg, [DataType]};
                _ ->
                    {ok, Context#context{crdt_type = DataType, crdt_mod = Mod}}
            end;
        {error, no_type} ->
            {halt, 404, [?TXT_HEADER], <<"Unknown bucket type: ~s">>, [Type]}
    end.

-spec set_option(
    crdt_option_key(),
    non_neg_integer() | quorum | all | backend | one | boolean(),
    context()
) ->
    context().
set_option(Option, Value, Context) ->
    Context#context{
        crdt_options = maps:put(Option, Value, Context#context.crdt_options)
    }.

set_location(Type, Bucket, Key) ->
    iolist_to_binary(
        io_lib:format(
            "/types/~s/buckets/~s/datatypes/~s",
            [Type, Bucket, Key]
        )
    ).

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.
