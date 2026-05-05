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
%% @doc Handler for HTTP API requests to 'DELETE' an object

-module(riak_kv_ag_object_delete).

-if(?OTP_RELEASE == 26).
-feature(maybe_expr, enable).
-endif.

-include("riak_kv_web.hrl").

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

-define(DEL_DEFAULTS, #{
    w => default,
    dw => default,
    pw => default,
    r => default,
    pr => default,
    rw => default,
    n_val => default,
    sloppy_quorum => default,
    timeout => undefined
}).

-type del_option_key() ::
    w
    | dw
    | pw
    | r
    | pr
    | rw
    | n_val
    | sloppy_quorum
    | timeout.

-type del_options() ::
    #{
        w => pos_integer() | default | quorum | all,
        dw => non_neg_integer() | default | quorum | all,
        pw => non_neg_integer() | default | quorum | all,
        r => non_neg_integer() | default | quorum | all,
        pr => non_neg_integer() | default | quorum | all,
        rw => non_neg_integer() | default | quorum | all,
        n_val => non_neg_integer() | default | quorum | all,
        sloppy_quorum => boolean() | default,
        timeout => pos_integer() | undefined
    }.

-record(context, {
    client = riak_client:new(node(), undefined) :: riak_client:riak_client(),
    bucket :: riak_object:bucket(),
    key :: riak_object:key(),
    del_options = ?DEL_DEFAULTS :: del_options(),
    vclock :: vclock:vclock() | undefined
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
        Method when Method == 'DELETE' ->
            Context =
                #context{
                    bucket = riak_kv_web_common:set_bucket(BucketType, Bucket),
                    key = Key
                },
            {ok, size_limits(), Context};
        _OtherMethod ->
            {method_not_allowed, ['DELETE']}
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
) when Method == 'POST' ->
    K = iolist_to_binary(riak_core_util:unique_id_62()),
    match_route(
        Method,
        Path,
        [<<"types">>, BucketType, <<"buckets">>, Bucket, <<"keys">>, K]
    );
match_route(_Method, _Path, _SplitPath) ->
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
    Check =
        riak_kv_web_common:check_permissions(
            ReqHeaders,
            Scheme,
            Peer,
            Ctx#context.bucket,
            "riak_kv.delete"
        ),
    case Check of
        true ->
            % The PB API doesn't check type exists, however, the FSM will crash
            % if it does not exist - so better to give a sensible error here.
            % Note this requires the fetching (and discarding) of the type
            % properties.
            case riak_kv_web_common:check_type_exists(Ctx#context.bucket) of
                ok ->
                    {ok, Ctx};
                HaltResponse ->
                    HaltResponse
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
    maybe
        {ok, Ctx0} ?= set_version_vector(ReqHeaders, Ctx),
        {ok, Ctx0}
    else
        HaltResponse ->
            HaltResponse
    end.

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
process_request(none, Context) ->
    DelOptions =
        riak_kv_web_common:filter_options(Context#context.del_options),
    Result =
        case Context#context.vclock of
            undefined ->
                riak_client:delete(
                    Context#context.bucket,
                    Context#context.key,
                    DelOptions,
                    Context#context.client
                );
            DecodedClock ->
                riak_client:delete_vclock(
                    Context#context.bucket,
                    Context#context.key,
                    DecodedClock,
                    DelOptions,
                    Context#context.client
                )
        end,
    case Result of
        ok ->
            {ok, {204, [], <<>>, true, none}, Context};
        {error, notfound} ->
            {ok, {404, [], <<>>, true, none}, Context};
        {error, Reason} ->
            handle_error(Reason, Context)
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
    FoldResult =
        riak_kv_web_common:count_fold(
            Params,
            [
                <<"w">>,
                <<"dw">>,
                <<"pw">>,
                <<"r">>,
                <<"pr">>,
                <<"rw">>,
                <<"n_val">>
            ],
            Context#context.del_options
        ),
    case FoldResult of
        UpdOpts when is_map(UpdOpts) ->
            {ok, Context#context{del_options = UpdOpts}};
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
            [<<"sloppy_quorum">>],
            Context#context.del_options
        ),
    case FoldResult of
        UpdOpts when is_map(UpdOpts) ->
            {ok, Context#context{del_options = UpdOpts}};
        HaltResponse ->
            HaltResponse
    end.

-spec set_version_vector(
    riak_api_web_headers:headers(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
set_version_vector(ReqHeaders, Ctx) ->
    case riak_kv_web_common:get_version_vector(ReqHeaders) of
        {ok, none} ->
            {ok, Ctx};
        {ok, DecodedClock} ->
            {ok, Ctx#context{vclock = DecodedClock}};
        HaltResponse ->
            HaltResponse
    end.

-spec handle_error(term(), context()) -> riak_api_web_acceptor:halt_response().
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
    {halt, 503, [?TXT_HEADER], <<"DW-value unsatisfied: ~p/~p">>, [NumDW, DW]};
handle_error({pw_val_unsatisfied, PW, NumPW}, _Ctx) ->
    {halt, 503, [?TXT_HEADER], <<"PW-value unsatisfied: ~p/~p">>, [NumPW, PW]};
handle_error({pr_val_unsatisfied, PR, NumPR}, _Ctx) ->
    Msg = <<"PR-value unsatisfied: ~p/~p">>,
    {halt, 503, [?TXT_HEADER], Msg, [NumPR, PR]};
handle_error({r_val_unsatisfied, R, NumR}, _Ctx) ->
    Msg = <<"R-value unsatisfied: ~p/~p">>,
    {halt, 503, [?TXT_HEADER], Msg, [NumR, R]};
handle_error(OtherError, _Ctx) ->
    {halt, 500, [?TXT_HEADER], <<"Error:~n~p">>, [OtherError]}.

%% ===================================================================
%% Internal Functions
%% ===================================================================

-spec set_option(
    del_option_key(),
    non_neg_integer() | quorum | all | boolean(),
    context()
) ->
    context().
set_option(Option, Value, Context) ->
    Context#context{
        del_options = maps:put(Option, Value, Context#context.del_options)
    }.

size_limits() ->
    {
        1024,
        2048,
        % A DELETE can never have a request body
        0
    }.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

extract_params(URI) ->
    uri_string:dissect_query(
        maps:get(
            query,
            uri_string:normalize(URI, [return_map])
        )
    ).

parameter_validation_test() ->
    InitCtx = #context{bucket = {<<"T">>, <<"B">>}, key = <<"K">>},
    {ok, Ctx1} =
        parse_query_params(
            extract_params(
                <<"/types/T/buckets/B/keys/K?timeout=10">>
            ),
            InitCtx
        ),
    ?assertMatch(10, maps:get(timeout, Ctx1#context.del_options)),
    ?assertMatch(
        {halt, 400, _, _, _},
        parse_query_params(
            extract_params(<<"/types/T/buckets/B/keys/K?timeout=*">>),
            InitCtx
        )
    ),
    {ok, Ctx2} =
        parse_query_params(
            extract_params(
                <<"/types/T/buckets/B/keys/K?w=1&rw=1&sloppy_quorum=true">>
            ),
            InitCtx
        ),
    ?assertMatch(1, maps:get(w, Ctx2#context.del_options)),
    ?assertMatch(1, maps:get(rw, Ctx2#context.del_options)),
    ?assertMatch(true, maps:get(sloppy_quorum, Ctx2#context.del_options)),

    ?assertMatch(
        {halt, 400, _, _, _},
        parse_query_params(
            extract_params(
                <<"/types/T/buckets/B/keys/K?w=A&rw=1&sloppy_quorum=true">>
            ),
            InitCtx
        )
    ),
    ?assertMatch(
        {halt, 400, _, _, _},
        parse_query_params(
            extract_params(
                <<"/types/T/buckets/B/keys/K?w=1&rw=1&sloppy_quorum=1">>
            ),
            InitCtx
        )
    ).

header_validation_test() ->
    Vc0 = vclock:increment('node1@127.0.0.1', vclock:fresh()),
    Vc1 = vclock:increment('node1@127.0.0.1', Vc0),
    Hdr0 =
        {
            <<"X-Riak-vclock">>,
            base64:encode(riak_object:encode_vclock(Vc0))
        },
    Hdr1 =
        {
            <<"X-Riak-vclock">>,
            base64:encode(riak_object:encode_vclock(Vc1))
        },
    InitCtx = #context{bucket = {<<"T">>, <<"B">>}, key = <<"K">>},
    {ok, Ctx1} =
        parse_request_headers(
            riak_api_web_headers:make([Hdr1]),
            InitCtx
        ),
    ?assertMatch(Vc1, Ctx1#context.vclock),
    ?assertMatch(
        {halt, 400, _, _, _},
        parse_request_headers(
            riak_api_web_headers:make([Hdr0, Hdr1]),
            InitCtx
        )
    ).

-endif.
