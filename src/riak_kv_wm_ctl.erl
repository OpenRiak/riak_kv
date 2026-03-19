%% -------------------------------------------------------------------
%%
%% riak_kv_wm_ctl: a Webmachine resource for riak_control operations.
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

-module(riak_kv_wm_ctl).

-export([init/1,
         options/2,
         service_available/2,
         allowed_methods/2,
         is_authorized/2,
         forbidden/2,
         content_types_provided/2,
         content_types_accepted/2,
         process_post/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("kernel/include/logger.hrl").

-record(context, {security :: undefined | riak_core_security:context()}).

init([]) ->
    {ok, #context{}}.

-spec service_available(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
service_available(RD, Ctx) ->
    {true, wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx}.

-spec allowed_methods(#wm_reqdata{}, #context{}) -> {[atom()], #wm_reqdata{}, #context{}}.
allowed_methods(RD, Ctx) ->
    {['OPTIONS', 'POST'], wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx}.

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
            {{halt, 426}, wrq:append_to_resp_body(
                            <<"Security is enabled and Riak does not accept credentials over HTTP. "
                              "Try HTTPS instead.">>, RD), Ctx}
    end.

-spec forbidden(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
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
    {[{"application/json", nonexistent_to_json}], RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    {[{"application/json", nonexistent_from_json}], RD, Ctx}.


-spec process_post(#wm_reqdata{}, #context{}) ->
          {boolean()|{halt, 400..500}, #wm_reqdata{}, #context{}}.
process_post(RD, Ctx) ->
    try
        Request = #{<<"action">> := Action} =
            riak_kv_wm_json:decode(wrq:req_body(RD)),
        case handler_mod(Action) of
            undefined ->
                {{halt, 400},
                 wrq:append_to_resp_body(
                   riak_kv_wm_json:encode(
                     #{error => iolist_to_binary([<<"Invalid request action ">>, Action])}), RD), Ctx};
            Mod ->
                case Mod:process_request(Request) of
                    {ok, Res} ->
                        {true,
                         wrq:append_to_resp_body(
                           riak_kv_wm_json:encode(#{result => Res}), RD), Ctx};
                    {StatusCode, Err} ->
                        {{halt, StatusCode},
                         wrq:append_to_resp_body(
                           riak_kv_wm_json:encode(#{error => Err}), RD), Ctx}
                end
        end
    catch
        _t:_e:_st ->
            {{halt, 400},
             wrq:append_to_resp_body(
               riak_kv_wm_json:encode(
                 #{error => <<"Malformed request">>}), RD), Ctx}
    end.

handler_mod(<<"ClusterGetStatus">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"ClusterClearPlan">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"ClusterCommitPlan">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"ClusterStageJoin">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"ClusterStageLeave">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"ClusterStageRemove">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"ClusterStageReplace">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"ClusterStageForceReplace">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"ClusterDownNode">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"ClusterStopNode">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"NodeGetAppEnv">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"NodePutAppEnv">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"NodeGetAdvancedConfig">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"NodePutAdvancedConfig">>) -> riak_kv_wm_ctl_cluster;
handler_mod(<<"NodeRestart">>) -> riak_kv_wm_ctl_cluster;

handler_mod(<<"VnodeGetStatus">>) -> riak_kv_wm_ctl_vnode;
handler_mod(<<"TictacaaeGetStatus">>) -> riak_kv_wm_ctl_tictacaae;

handler_mod(<<"SystemGetVersionInfo">>) -> riak_kv_wm_ctl_version_info;

handler_mod(<<"SecurityListUsers">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityCreateUser">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityUpdateUser">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityDeleteUser">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityListGroups">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityCreateGroup">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityUpdateGroup">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityDeleteGroup">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityAddUserGroup">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityDeleteUserGroup">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityAddUserGrant">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityDeleteUserGrant">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityAddGroupGrant">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityDeleteGroupGrant">>) -> riak_kv_wm_ctl_security;
handler_mod(<<"SecurityListPermissions">>) -> riak_kv_wm_ctl_security;

handler_mod(_) -> undefined.
