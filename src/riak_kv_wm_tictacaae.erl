%% -------------------------------------------------------------------
%%
%% riak_kv_wm_tictacaae: a Webmachine resource providing tictacaae report
%%                       Called by riak_control.
%%
%% Copyright (c) 2026 TI Tokyo.  All Rights Reserved.
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

-module(riak_kv_wm_tictacaae).

-export([init/1,
         options/2,
         service_available/2,
         allowed_methods/2,
         is_authorized/2,
         forbidden/2,
         content_types_provided/2,
         to_json/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("kernel/include/logger.hrl").

-record(context, {security :: undefined | riak_core_security:context()}).

init([]) ->
    {ok, #context{}}.

-spec service_available(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
service_available(RD, Ctx) ->
    {({ok, active} == application:get_env(riak_kv, tictacaae_active)),
     wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx}.

-spec allowed_methods(#wm_reqdata{}, #context{}) -> {[atom()], #wm_reqdata{}, #context{}}.
allowed_methods(RD, Ctx) ->
    {['OPTIONS', 'GET'], wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx}.

-spec options(#wm_reqdata{}, #context{}) -> {[{string(), string()}], #wm_reqdata{}, #context{}}.
options(RD, Ctx) ->
    {riak_kv_wm_utils:cors_headers(), RD, Ctx}.

-spec is_authorized(#wm_reqdata{}, #context{}) ->
          {string()|boolean()|{halt,426}, #wm_reqdata{}, #context{}}.
is_authorized(RD, Ctx) ->
    case wrq:method(RD) of
        'OPTIONS' ->
            {true, wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx};
        _ ->
            is_authorized2(RD, Ctx)
    end.
is_authorized2(RD, Ctx) ->
    case riak_api_web_security:is_authorized(RD) of
        false ->
            {"Basic realm=\"Riak\"", RD, Ctx};
        {true, SecContext} ->
            {true, RD, Ctx#context{security = SecContext}};
        insecure ->
            {{halt, 426}, wrq:append_to_resp_body(<<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>, RD), Ctx}
    end.

-spec forbidden(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
forbidden(RD, Ctx = #context{security = undefined}) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx};
forbidden(RD, Ctx) ->
    case wrq:method(RD) of
        'OPTIONS' ->
            {false, RD, Ctx};
        _ ->
            forbidden2(RD, Ctx)
    end.
forbidden2(RD, Ctx = #context{security = Security}) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false when Security == undefined ->
            RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
            {true, wrq:append_to_resp_body(<<"Riak security not enabled">>, RD1), Ctx};
        false ->
            Res = riak_core_security:check_permission(
                    {"riak_kv.riak_control"}, Security),
            case Res of
                {false, Error, _} ->
                    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
                    {true, wrq:append_to_resp_body(
                             unicode:characters_to_binary(Error, utf8, utf8), RD1), Ctx};
                {true, _} ->
                    {false, RD, Ctx}
            end
    end.


-spec content_types_provided(#wm_reqdata{}, #context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/json", to_json}], RD, Ctx}.


-spec to_json(#wm_reqdata{}, #context{}) -> {binary(), #wm_reqdata{}, #context{}}.
to_json(RD, Context) ->
    WhichNodes =
        case lists:keyfind(
               "nodes", 1, wrq:req_qs(RD)) of
            {"nodes", "all"} ->
                [node() | nodes()];
            {"nodes", NNs} ->
                NN = string:tokens(NNs, ","),
                [N || N <- [list_to_atom(N) || N <- NN], lists:member(N, [node() | nodes()])];
            false ->
                [node()]
        end,
    Report =
        [{N, rpc:call(N, riak_kv_tictacaae_report, produce, [])} || N <- WhichNodes],
    {mochijson2:encode(
       jsonify_report(Report)), RD, Context}.

jsonify_report(Report) ->
    [{N, jsonify_report_items(R)} || {N, R} <- Report].
jsonify_report_items(II) ->
    [lists:map(
       fun({partition, A}) -> {partition, integer_to_binary(A)};
          ({last_rebuild, A = {_, _, _}}) -> {last_rebuild, fmt_ts(A)};
          ({next_rebuild, A = {_, _, _}}) -> {next_rebuild, fmt_ts(A)};
          (A) -> A
       end, I) || I <- II].

fmt_ts({M, S, L}) ->
    list_to_binary(
      calendar:system_time_to_rfc3339(
        M * 1_000_000 * 1_000 + S * 1_000 + L div 1000, [{unit, millisecond}])).
