%% -------------------------------------------------------------------
%%
%% riak_kv_wm_system: simple Webmachine resource returning uptime
%%                    and riak and otp versions.
%%                    Called by riak_control.
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

-module(riak_kv_wm_system).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         allowed_methods/2,
         content_types_provided/2,
         is_authorized/2,
         forbidden/2,
         options/2,
         to_json/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("kernel/include/logger.hrl").

-record(context, {security :: undefined | riak_core_security:context()}).

init([]) ->
    {ok, #context{}}.

-spec service_available(#wm_reqdata{}, undefined) -> {boolean(), #wm_reqdata{}, undefined}.
service_available(RD, Ctx) ->
    {true, wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx}.

-spec allowed_methods(#wm_reqdata{}, undefined) -> {[atom()], #wm_reqdata{}, undefined}.
allowed_methods(RD, Ctx) ->
    {['GET', 'OPTIONS'], wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx}.

-spec options(#wm_reqdata{}, undefined) -> {[{string(), string()}], #wm_reqdata{}, undefined}.
options(RD, Ctx) ->
    {riak_kv_wm_utils:cors_headers(), RD, Ctx}.

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
        {true, _} ->
            {true, RD, Ctx};
        insecure ->
            {{halt, 426}, wrq:append_to_resp_body(<<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>, RD), Ctx}
    end.

-spec forbidden(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
forbidden(RD, Ctx = #context{security = undefined}) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx};
forbidden(RD, Ctx = #context{security = Security}) ->
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

content_types_provided(RD, Ctx) ->
    {[{"application/json", to_json}], RD, Ctx}.

-spec to_json(#wm_reqdata{}, undefined) -> {binary(), #wm_reqdata{}, undefined}.
to_json(RD, Ctx) ->
    A = riak_kv_util:system_info(),
    {mochijson2:encode(A#{http_listeners => get_http_listeners()}), RD, Ctx}.

get_http_listeners() ->
    lists:flatten(
      [begin
           case rpc:call(N, application, get_env, [riak_api, https]) of
               {ok, [{IP, Port}]} ->
                   [{N, iolist_to_binary(["https://", IP, $:, integer_to_binary(Port)])}];
               _ ->
                   []
           end
       end || N <- [node() | nodes()]]
     ).
