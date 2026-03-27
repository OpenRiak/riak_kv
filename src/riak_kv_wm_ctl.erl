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
         content_types_provided/2,
         content_types_accepted/2,
         process_post/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("kernel/include/logger.hrl").

-record(context, {request :: undefined | #{},
                  user :: string()}).

init([]) ->
    {ok, #context{}}.

-spec service_available(#wm_reqdata{}, #context{}) ->
          {boolean(), #wm_reqdata{}, #context{}}.
service_available(RD, Ctx) ->
    {riak_control_enabled(),
     wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx}.

riak_control_enabled() ->
    ?LOG_NOTICE("STUB: riak_control_enabled() returns true", []),
    true.

-spec allowed_methods(#wm_reqdata{}, #context{}) ->
          {[atom()], #wm_reqdata{}, #context{}}.
allowed_methods(RD, Ctx) ->
    {['OPTIONS', 'POST'],
     wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx}.

-spec options(#wm_reqdata{}, #context{}) ->
          {[{string(), string()}], #wm_reqdata{}, #context{}}.
options(RD, Ctx) ->
    {riak_kv_wm_utils:cors_headers(), RD, Ctx}.

-spec is_authorized(#wm_reqdata{}, #context{}) ->
          {true, #wm_reqdata{}, #context{}}.
is_authorized(RD, Ctx) ->
    Request = #{<<"action">> := Action} =
        riak_kv_wm_json:decode(wrq:req_body(RD)),
    User = extract_user(RD),
    UserPermissions = get_user_permissions(User),
    ReqPermissions = permissions_for(Action),
    {intersect(UserPermissions, ReqPermissions),
     wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD),
     Ctx#context{request = Request, user = User}}.

extract_user(RD) ->
    wrq:get_req_header("X-Riak-User", RD).
get_user_permissions(User) ->
    ?LOG_NOTICE("STUB: get_user_permissions(~p) returns all permissions", [User]),
    [cluster_observer, cluster_admin, security].
intersect(AA, BB) ->
    lists:any(fun(A) -> lists:member(A, BB) end, AA).


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
process_post(RD, Ctx = #context{request = Request}) ->
    #{<<"action">> := Action} = Request,
    try
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


permissions_for(<<"ClusterGetStatus">>) -> [cluster_observer];
permissions_for(<<"ClusterClearPlan">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"ClusterCommitPlan">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"ClusterStageJoin">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"ClusterStageLeave">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"ClusterStageRemove">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"ClusterStageReplace">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"ClusterStageForceReplace">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"ClusterDownNode">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"ClusterStopNode">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"NodeGetAppEnv">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"NodePutAppEnv">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"NodeGetAdvancedConfig">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"NodePutAdvancedConfig">>) -> [cluster_observer, cluster_admin];
permissions_for(<<"NodeRestart">>) -> [cluster_observer, cluster_admin];

permissions_for(<<"VnodeGetStatus">>) -> [cluster_observer];
permissions_for(<<"TictacaaeGetStatus">>) -> [cluster_observer];

permissions_for(<<"SystemGetVersionInfo">>) -> [];

permissions_for(<<"SecurityListUsers">>) -> [security];
permissions_for(<<"SecurityCreateUser">>) -> [security];
permissions_for(<<"SecurityUpdateUser">>) -> [security];
permissions_for(<<"SecurityDeleteUser">>) -> [security];
permissions_for(<<"SecurityListGroups">>) -> [security];
permissions_for(<<"SecurityCreateGroup">>) -> [security];
permissions_for(<<"SecurityUpdateGroup">>) -> [security];
permissions_for(<<"SecurityDeleteGroup">>) -> [security];
permissions_for(<<"SecurityAddUserGroup">>) -> [security];
permissions_for(<<"SecurityDeleteUserGroup">>) -> [security];
permissions_for(<<"SecurityAddUserGrant">>) -> [security];
permissions_for(<<"SecurityDeleteUserGrant">>) -> [security];
permissions_for(<<"SecurityAddGroupGrant">>) -> [security];
permissions_for(<<"SecurityDeleteGroupGrant">>) -> [security];
permissions_for(<<"SecurityListPermissions">>) -> [security];

permissions_for(_) -> [].
