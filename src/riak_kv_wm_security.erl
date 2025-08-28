%% -------------------------------------------------------------------
%%
%% riak_kv_wm_security: Webmachine resource exposing actions
%%                      to manage users, groups & grants
%%                      Called by riak_control.
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

-module(riak_kv_wm_security).

%% webmachine resource exports
-export([init/1,
         service_available/2,
         allowed_methods/2,
         is_authorized/2,
         forbidden/2,
         options/2,
         content_types_accepted/2,
         content_types_provided/2,
         process_post/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("kernel/include/logger.hrl").

-define(TOMBSTONE, '$deleted').

-record(context, {security :: undefined | riak_core_security:context()}).

init([]) ->
    {ok, #context{}}.

-spec service_available(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
service_available(RD, Ctx) ->
    {true, wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx}.

-spec allowed_methods(#wm_reqdata{}, #context{}) -> {[atom()], #wm_reqdata{}, #context{}}.
allowed_methods(RD, Ctx) ->
    {['POST', 'OPTIONS'],
     wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx}.

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
forbidden(RD, Ctx = #context{security = Security}) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
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
    {[{"application/json", process_post}], RD, Ctx}.

-spec process_post(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
process_post(RD, Context) ->
    try
        case process_req(RD) of
            ok ->
                {true,
                 wrq:append_to_resp_body(
                   mochijson2:encode(#{result => ok}), RD),
                 Context};
            {ok, Res} ->
                {true,
                 wrq:append_to_resp_body(
                   mochijson2:encode(Res), RD),
                 Context};
            {error, notfound} ->
                {{halt, 404}, RD, Context};
            {error, Reason} ->
                ?LOG_WARNING("error processing request: ~p", [Reason]),
                {{halt, 400}, wrq:append_to_resp_body(<<"malformed action (check the riak logs)">>, RD),
                 Context}
        end
    catch
        _t:_e:_st ->
            ?LOG_WARNING("error parsing or processing request: ~p:~p  ~p", [_t, _e, _st]),
            {{halt, 400}, wrq:append_to_resp_body(<<"malformed action">>, RD), Context}
    end.

process_req(RD) ->
    case mochijson2:decode(wrq:req_body(RD), [{format, map}]) of
        #{<<"action">> := <<"ListUsers">>,
          <<"params">> := #{}} ->
            Res = [ begin
                        PasswordOptions = proplists:get_value("password", Options, []),
                        PwdHash = proplists:get_value(hash_pass, PasswordOptions, <<"--">>),
                        Groups = proplists:get_value("groups", Options, []),
                        OtherOptions = maps:from_list([{unicode:characters_to_binary(K, utf8),
                                                        unicode:characters_to_binary(V, utf8)}
                                                       || {K, V} <- Options,
                                                          K /= "password",
                                                          K /= "groups"]),
                        Grants = [#{scope => jsonify_scope(Scope),
                                    permissions => jsonify_permissions(PP)}
                                  || {Scope, PP} <- riak_core_security:get_user_grants(Name)],
                        #{name => Name,
                          password_hash => PwdHash,
                          groups => Groups,
                          options => OtherOptions,
                          grants => Grants}
                    end || {Name, [Options]} <- riak_core_security:get_users() ],
            {ok, Res};
        #{<<"action">> := <<"CreateUser">>,
          <<"params">> := #{<<"name">> := Name,
                            <<"options">> := Options}} ->
            riak_core_security:add_user(
              binary_to_list(Name),
              maps:to_list(deep_binary_to_list(Options)));
        #{<<"action">> := <<"UpdateUser">>,
          <<"params">> := #{<<"name">> := Name,
                            <<"options">> := Options}} ->
            riak_core_security:alter_user(
              binary_to_list(Name),
              maps:to_list(deep_binary_to_list(Options)));
        #{<<"action">> := <<"DeleteUser">>,
          <<"params">> := #{<<"name">> := Name}} ->
            riak_core_security:del_user(binary_to_list(Name));

        #{<<"action">> := <<"ListGroups">>,
          <<"params">> := #{}} ->
            Res = [ begin
                        Grants = [#{scope => jsonify_scope(Scope),
                                    permissions => jsonify_permissions(PP)}
                                  || {Scope, PP} <- riak_core_security:get_group_grants(Name)],
                        OtherOptions = maps:from_list([{unicode:characters_to_binary(K, utf8),
                                                        unicode:characters_to_binary(V, utf8)}
                                                       || {K, V} <- Options]),
                        #{name => Name,
                          grants => Grants,
                          options => OtherOptions}
                    end || {Name, [Options]} <- riak_core_security:get_groups() ],
            {ok, Res};
        #{<<"action">> := <<"CreateGroup">>,
          <<"params">> := #{<<"name">> := Name,
                            <<"options">> := Options}} ->
            riak_core_security:add_group(
              binary_to_list(Name),
              maps:to_list(deep_binary_to_list(Options)));
        #{<<"action">> := <<"UpdateGroup">>,
          <<"params">> := #{<<"name">> := Name,
                            <<"options">> := Options}} ->
            riak_core_security:alter_group(
              binary_to_list(Name),
              maps:to_list(deep_binary_to_list(Options)));
        #{<<"action">> := <<"DeleteGroup">>,
          <<"params">> := #{<<"name">> := Name}} ->
            riak_core_security:del_group(binary_to_list(Name));

        #{<<"action">> := <<"AddUserGroup">>,
          <<"params">> := #{<<"user">> := User,
                            <<"group">> := Group}} ->
            case lists:keyfind(User, 1, riak_core_security:get_users()) of
                {_, [PL|_]} ->
                    GG0 = proplists:get_value("groups", PL),
                    GG9 = string:join([binary_to_list(A) || A <- lists:usort(GG0 ++ [Group])], ","),
                    Options = [{"groups", GG9}],
                    riak_core_security:alter_user(binary_to_list(User), Options);
                _ ->
                    {error, notfound}
            end;
        #{<<"action">> := <<"DeleteUserGroup">>,
          <<"params">> := #{<<"user">> := User,
                            <<"group">> := Group}} ->
            case lists:keyfind(User, 1, riak_core_security:get_users()) of
                {_, [PL|_]} ->
                    GG0 = proplists:get_value("groups", PL),
                    GG9 = string:join([binary_to_list(A) || A <- lists:usort(GG0 -- [Group])], ","),
                    Options = [{"groups", GG9}],
                    riak_core_security:alter_user(binary_to_list(User), Options);
                _ ->
                    {error, notfound}
            end;

        #{<<"action">> := <<"AddUserGrant">>,
          <<"params">> := #{<<"user">> := User,
                            <<"permission">> := Permission,
                            <<"scope">> := Scope}} ->
            case lists:keyfind(User, 1, riak_core_security:get_users()) of
                {_, [_|_]} ->
                    riak_core_security:add_grant(
                      ["user/"++binary_to_list(User)],
                      binary_to_list(Scope),
                      [binary_to_list(Permission)]);
                _ ->
                    {error, notfound}
            end;
        #{<<"action">> := <<"DeleteUserGrant">>,
          <<"params">> := #{<<"user">> := User,
                            <<"permission">> := Permission,
                            <<"scope">> := Scope}} ->
            case lists:keyfind(User, 1, riak_core_security:get_users()) of
                {_, [_|_]} ->
                    riak_core_security:add_revoke(
                      ["user/"++binary_to_list(User)],
                      binary_to_list(Scope),
                      [binary_to_list(Permission)]);
                _ ->
                    {error, notfound}
            end;

        #{<<"action">> := <<"AddGroupGrant">>,
          <<"params">> := #{<<"group">> := Group,
                            <<"permission">> := Permission,
                            <<"scope">> := Scope}} ->
            case lists:keyfind(Group, 1, riak_core_security:get_groups()) of
                {_, [_|_]} ->
                    riak_core_security:add_grant(
                      ["group/"++binary_to_list(Group)],
                      binary_to_list(Scope),
                      [binary_to_list(Permission)]);
                _ ->
                    {error, notfound}
            end;
        #{<<"action">> := <<"DeleteGroupGrant">>,
          <<"params">> := #{<<"group">> := Group,
                            <<"permission">> := Permission,
                            <<"scope">> := Scope}} ->
            case lists:keyfind(Group, 1, riak_core_security:get_groups()) of
                {_, [_|_]} ->
                    riak_core_security:add_revoke(
                      ["group/"++binary_to_list(Group)],
                      binary_to_list(Scope),
                      [binary_to_list(Permission)]);
                _ ->
                    {error, notfound}
            end;

        #{<<"action">> := <<"ListPermissions">>,
          <<"params">> := #{}} ->
            {ok, PP} = application:get_env(riak_core, permissions),
            {ok, lists:flatten(
                   [[iolist_to_binary([atom_to_list(App), $., atom_to_list(K)]) || K <- KK]
                    || {App, KK} <- PP]
                  )}
    end.



jsonify_scope({A}) ->
    list_to_binary(A);
jsonify_scope({A, any}) ->
    iolist_to_binary([A, $:, $*]);
jsonify_scope({A, B}) ->
    iolist_to_binary([A, $:, B]).

jsonify_permissions(PP) ->
    [list_to_binary(P) || P <- PP].

-spec content_types_accepted(#wm_reqdata{}, #context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    {[{"application/json", from_json}], RD, Ctx}.


deep_binary_to_list(A) ->
    maps:fold(
      fun(K, V, Q) when is_binary(V) -> maps:put(binary_to_list(K), binary_to_list(V), Q);
         (K, V, Q) when is_map(V) -> maps:put(binary_to_list(K), deep_binary_to_list(V), Q)
      end, #{}, A).
