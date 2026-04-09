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

-record(context,
    {
        timeout = 30000 :: pos_integer(),
        content_type = json :: content_type()
    }
).

-type content_type() :: json|plain.
-type context() :: #context{}.

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
match_route(Method, _Path, [StatsPath]) ->
    ExpectedStatsPath =
        iolist_to_binary(application:get_env(riak_kv, stats_urlpath, "stats")),
    case {StatsPath, Method} of
        {ExpectedStatsPath, 'GET'} ->
            {ok, size_limits(), #context{}};
        {ExpectedStatsPath, _OtherMethod} ->
            {method_not_allowed, ['GET']};
        {_OtherPath, _} ->
            no_match
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
    case lists:keyfind(<<"timeout">>, 1, Params) of
        false ->
            {ok, Ctx};
        {<<"timeout">>, TO} when is_binary(TO) ->
            try
                IntTO = binary_to_integer(TO),
                true = IntTO >= 0,
                {ok, Ctx#context{timeout = IntTO}}
            catch
                _:_ ->
                    {halt, 401, [], <<"Bad timeout value ~0p">>, [TO]}
            end
    end.

%% @doc parse and validate the request headers
-spec parse_request_headers(
    riak_api_web_headers:headers(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
parse_request_headers(ReqHeaders, Ctx) ->
    case riak_api_web_headers:get_value('Accept', ReqHeaders) of
        <<"application/json">> ->
            {ok, Ctx#context{content_type = json}};
        <<"text/plain">> ->
            {ok, Ctx#context{content_type = plain}};
        <<"*/*">> ->
            {ok, Ctx#context{content_type = json}};
        CTL when is_list(CTL) ->
            case lists:member(<<"application/json">>, CTL) of
                true ->
                    {ok, Ctx#context{content_type = json}};
                false ->
                    case lists:member(<<"text/plain">>, CTL) of
                        true ->
                            {ok, Ctx#context{content_type = plain}};
                        false ->
                            {halt, 406, [], <<>>, []}
                    end
            end;
        undefined ->
            {ok, Ctx#context{content_type = json}};               
        CT when is_binary(CT) ->
            {halt, 406, [], <<>>, []}
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
process_request(RqBdy, Ctx) ->
    case riak_kv_web_common:confirm_empty_body(RqBdy) of
        {ok, UpdBody} ->
            try 
                Stats = riak_kv_http_cache:get_stats(Ctx#context.timeout),
                {Rsp, CType} =
                    produce_response(Stats, Ctx#context.content_type),
                {ok, {200, [{'Content-Type', CType}], Rsp, true, UpdBody}, Ctx}
            catch
                exit:{timeout, _} ->
                    ErrMsg = <<"Request timed out after ~w ms">>,
                    {halt, 503, [?TXT_HEADER], ErrMsg, Ctx#context.timeout}
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
        1024,
        2048,
        0
    }.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

delete_test_() ->
    %% Execute the test cases
    {foreach,
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
configure(_) -> ok.

-endif.
