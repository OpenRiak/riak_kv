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
%% @doc Handler for HTTP API requests for node statistics

-module(riak_kv_web_stats).

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
    timeout = 30000 :: pos_integer(),
    content_type = json :: content_type()
}).

-type content_type() :: json | plain.
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
match_route(Method, Path, _) ->
    ExpectedStatsPath =
        iolist_to_binary(
            application:get_env(riak_kv, stats_urlpath, "stats")
        ),
    case {string:trim(Path, both, "/"), Method} of
        {ExpectedStatsPath, 'GET'} ->
            {ok, size_limits(), #context{}};
        {ExpectedStatsPath, _OtherMethod} ->
            {method_not_allowed, ['GET']};
        {_OtherPath, _} ->
            nomatch
    end.

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
                    undefined,
                    undefined
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
parse_query_params([], Ctx) ->
    % Typically we expect no options - so shortcut the validation in this case
    {ok, Ctx};
parse_query_params(Params, Ctx) ->
    case riak_kv_web_common:get_timeout(Params) of
        {ok, none} ->
            {ok, Ctx};
        {ok, Timeout} ->
            {ok, Ctx#context{timeout = Timeout}};
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
        undefined ->
            {ok, Ctx#context{content_type = json}};
        AcceptType ->
            {JsonOK, JsonScore} =
                riak_kv_web_common:type_preference(
                    <<"application/json">>,
                    AcceptType
                ),
            {TextOK, TextScore} =
                riak_kv_web_common:type_preference(
                    <<"text/plain">>,
                    AcceptType
                ),
            case {JsonOK, JsonScore >= TextScore, TextOK} of
                {true, true, _} ->
                    {ok, Ctx#context{content_type = json}};
                {false, false, true} ->
                    {ok, Ctx#context{content_type = plain}};
                _ ->
                    {halt, 406, [], <<>>, []}
            end
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
process_request(none, Ctx) ->
    try
        Stats = riak_kv_http_cache:get_stats(Ctx#context.timeout),
        {Rsp, CType} =
            produce_response(Stats, Ctx#context.content_type),
        {ok, {200, [{'Content-Type', CType}], Rsp, true, none}, Ctx}
    catch
        exit:{timeout, _} ->
            ErrMsg = <<"Request timed out after ~w ms">>,
            {halt, 503, [?TXT_HEADER], ErrMsg, Ctx#context.timeout}
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

-if(?OTP_RELEASE >= 28).

produce_response(Stats, json) ->
    {
        iolist_to_binary(json:encode(maps:from_list(Stats))),
        <<"application/json">>
    };
produce_response(Stats, plain) ->
    % No pretty-print available
    {
        iolist_to_binary(json:format(maps:from_list(Stats))),
        <<"text/plain">>
    }.

-else.
produce_response(Stats, json) ->
    {
        iolist_to_binary(riak_kv_wm_json:encode(maps:from_list(Stats))),
        <<"application/json">>
    };
produce_response(Stats, plain) ->
    % No pretty-print available in riak_kv_wm_json
    {
        iolist_to_binary(riak_kv_wm_json:encode(maps:from_list(Stats))),
        <<"text/plain">>
    }.
-endif.

size_limits() ->
    {
        32,
        1024,
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
    {ok, Ctx1} =
        parse_query_params(
            extract_params(<<"/stats?timeout=10">>),
            #context{}
        ),
    ?assertMatch(10, Ctx1#context.timeout),
    {ok, Ctx2} =
        parse_query_params(
            extract_params(<<"/stats?undefined_param">>),
            #context{}
        ),
    ?assertMatch(30000, Ctx2#context.timeout),
    ?assertMatch(
        {halt, 400, _, _, _},
        parse_query_params(
            extract_params(<<"/stats?timeout=B">>),
            #context{}
        )
    ).

stats_test_() ->
    {
        foreach,
        setup(),
        cleanup(),
        [
            fun check_stats_not_crash/0
        ]
    }.

check_stats_not_crash() ->
    Stats = riak_kv_http_cache:get_stats(30000),
    _DummyRun = timer:tc(fun() -> produce_response(Stats, json) end),
    % Timings for first run are slow ??
    {T0, {JR, <<"application/json">>}} =
        timer:tc(fun() -> produce_response(Stats, json) end),
    {T1, {JP, <<"text/plain">>}} =
        timer:tc(fun() -> produce_response(Stats, plain) end),
    io:format(user, "Stats took ~w json ~w plain~n", [T0, T1]),
    ?assert(is_binary(JR)),
    ?assert(is_binary(JP)).

setup() ->
    riak_kv_test_util:common_setup(?MODULE, fun configure/1).

cleanup() ->
    riak_kv_test_util:common_cleanup(?MODULE, fun configure/1).

configure(load) ->
    application:set_env(riak_core, default_bucket_props, []),
    application:set_env(riak_kv, storage_backend, riak_kv_memory_backend);
configure(_) ->
    ok.

accept_header_test() ->
    InitCtx = #context{},
    Hdr1 = riak_api_web_headers:make([{'Accept', <<"application/json">>}]),
    {ok, Ctx1} = parse_request_headers(Hdr1, InitCtx),
    ?assertMatch(json, Ctx1#context.content_type),
    Hdr2 = riak_api_web_headers:make([{'Accept', <<"application/*">>}]),
    {ok, Ctx2} = parse_request_headers(Hdr2, InitCtx),
    ?assertMatch(json, Ctx2#context.content_type),
    Hdr3 =
        riak_api_web_headers:make(
            [
                {'Accept', <<"text/plain, application/*">>}
            ]
        ),
    {ok, Ctx3} = parse_request_headers(Hdr3, InitCtx),
    ?assertMatch(json, Ctx3#context.content_type),
    Hdr4 =
        riak_api_web_headers:make(
            [
                {'Accept', <<"text/*, application/octet-stream">>}
            ]
        ),
    {ok, Ctx4} = parse_request_headers(Hdr4, InitCtx),
    ?assertMatch(plain, Ctx4#context.content_type),
    Hdr5 =
        riak_api_web_headers:make(
            [
                {'Accept', <<"*/*, application/octet-stream">>}
            ]
        ),
    {ok, Ctx5} = parse_request_headers(Hdr5, InitCtx),
    ?assertMatch(json, Ctx5#context.content_type),
    Hdr6 =
        riak_api_web_headers:make([
            {'Accept', <<"application/octet-stream">>}
        ]),
    ?assertMatch(halt, element(1, parse_request_headers(Hdr6, InitCtx))),
    Hdr7 =
        riak_api_web_headers:make([
            {'Accept', <<"application/octet-stream, text/html">>}
        ]),
    ?assertMatch(halt, element(1, parse_request_headers(Hdr7, InitCtx))),
    Hdr8 = riak_api_web_headers:make([]),
    {ok, Ctx8} = parse_request_headers(Hdr8, InitCtx),
    ?assertMatch(json, Ctx8#context.content_type).

-endif.
