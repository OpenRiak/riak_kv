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
%% @doc Handler for HTTP keylist API (v2 or higher)

-module(riak_kv_web_keylist).

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

-record(context, {
    client = riak_client:new(node(), self()) :: riak_client:riak_client(),
    bucket :: riak_object:bucket(),
    stream = false :: boolean(),
    timeout :: pos_integer() | undefined
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
match_route('GET', _, [<<"types">>, T, <<"buckets">>, B, <<"keys">>]) ->
    {
        ok,
        {32, 2048, 0},
        #context{bucket = riak_kv_web_common:set_bucket(T, B)}
    };
match_route(Method, _, [<<"types">>, _T, <<"buckets">>, _B, <<"keys">>]) when
    Method =/= 'GET', Method =/= 'POST'
->
    {method_not_allowed, ['GET', 'POST']};
match_route(Method, Path, [<<"buckets">>, B, <<"keys">>]) ->
    match_route(
        Method,
        Path,
        [<<"types">>, <<"default">>, <<"buckets">>, B, <<"keys">>]
    );
match_route(_, _, _) ->
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
    case application:get_env(riak_kv, permit_insecure_http_ops, false) of
        true ->
            {ok, Ctx};
        false ->
            Check =
                riak_kv_web_common:check_permissions(
                    ReqHeaders,
                    Scheme,
                    Peer,
                    Ctx#context.bucket,
                    "riak_kv.list_keys"
                ),
            case Check of
                true ->
                    {ok, Ctx};
                HaltResponse ->
                    HaltResponse
            end
    end.

%% @doc parse and validate query params, passed as a map
-spec parse_query_params(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
parse_query_params(Params, Ctx) ->
    Ctx1 =
        case lists:keyfind(<<"keys">>, 1, Params) of
            {<<"keys">>, <<"stream">>} ->
                Ctx#context{stream = true};
            _ ->
                Ctx
        end,
    validate_timeout(Params, Ctx1).

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
process_request(none, #context{bucket = B, client = C} = Ctx) ->
    case Ctx#context.stream of
        true ->
            {ok, ReqId} =
                riak_client:stream_list_keys(B, Ctx#context.timeout, C),

            {
                ok,
                {
                    200,
                    [?JSN_HEADER],
                    {stream, key_stream_fun(ReqId)},
                    true,
                    none
                },
                Ctx
            };
        false ->
            case riak_client:list_keys(B, Ctx#context.timeout, C) of
                {ok, KeyList} ->
                    JsonResults =
                        riak_kv_web_index:encode_results(
                            false,
                            KeyList,
                            undefined
                        ),
                    {
                        ok,
                        {
                            200,
                            [?JSN_HEADER],
                            iolist_to_binary(JsonResults),
                            true,
                            none
                        },
                        Ctx
                    };
                {error, timeout} ->
                    ErrMsg =
                        riak_kv_wm_json:encode(
                            #{<<"error">> => <<"Request timed out">>}
                        ),
                    {halt, 503, [?JSN_HEADER], iolist_to_binary(ErrMsg), []};
                {error, Reason} ->
                    ErrMsg =
                        riak_kv_wm_json:encode(
                            #{
                                <<"error">> =>
                                    iolist_to_binary(
                                        io_lib:format("~0p", [Reason])
                                    )
                            }
                        ),
                    {halt, 503, [?JSN_HEADER], iolist_to_binary(ErrMsg), []}
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
%% Internal Functions
%% ===================================================================

-spec key_stream_fun(non_neg_integer()) -> stream_fun().
key_stream_fun(ReqId) ->
    fun() ->
        receive
            {ReqId, From, {keys, Keys}} ->
                _ = riak_kv_keys_fsm:ack_keys(From),
                JsonResults =
                    riak_kv_web_index:encode_results(
                        false,
                        Keys,
                        undefined
                    ),
                {
                    iolist_to_binary(JsonResults),
                    key_stream_fun(ReqId)
                };
            {ReqId, {keys, Keys}} ->
                JsonResults =
                    riak_kv_web_index:encode_results(
                        false,
                        Keys,
                        undefined
                    ),
                {
                    iolist_to_binary(JsonResults),
                    key_stream_fun(ReqId)
                };
            {ReqId, done} ->
                {<<>>, fun() -> done end};
            {ReqId, {error, timeout}} ->
                JsonError =
                    riak_kv_wm_json:encode(
                        #{<<"error">> => <<"Request timed out">>}
                    ),
                {
                    iolist_to_binary(JsonError),
                    fun() -> done end
                }
        end
    end.

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
            {ok, Ctx#context{timeout = Timeout}};
        HaltResponse ->
            HaltResponse
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

parse_test() ->
    InitCtx = #context{bucket = {<<"T">>, <<"B">>}},
    {ok, Ctx1} =
        parse_query_params(
            [{<<"keys">>, <<"stream">>}, {<<"timeout">>, <<"1000">>}],
            InitCtx
        ),
    ?assertMatch(true, Ctx1#context.stream),
    ?assertMatch(1000, Ctx1#context.timeout),
    ?assertMatch(
        {halt, 400, _, _, _},
        parse_query_params(
            [{<<"keys">>, <<"stream">>}, {<<"timeout">>, <<"A">>}],
            InitCtx
        )
    ),
    ReqHeaders = riak_api_web_headers:make([{'Accept', <<"*/*">>}]),
    ?assertMatch({ok, _}, parse_request_headers(ReqHeaders, Ctx1)),
    ReqHeadersNoAccept = riak_api_web_headers:make([]),
    ?assertMatch({ok, _}, parse_request_headers(ReqHeadersNoAccept, Ctx1)),
    ReqHeadersPlain = riak_api_web_headers:make([{'Accept', <<"text/plain">>}]),
    ?assertMatch(
        {halt, 406, _, _, _},
        parse_request_headers(ReqHeadersPlain, Ctx1)
    ).

-endif.
