%% -------------------------------------------------------------------
%%
%% riak_kv_wm_ctl_security: Riak Control, requests to manage users, groups, grants.
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

-module(riak_kv_wm_ctl_security).

-export([process_request/1]).

-include_lib("kernel/include/logger.hrl").

-spec process_request(#{}) ->
          {ok, binary() | map() | [map()]} | {400..500, binary()}.
process_request(Request) ->
    Res =
        case Request of
            #{<<"action">> := <<"SecurityListUsers">>} ->
                A = [ begin
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
                {ok, A};
            #{<<"action">> := <<"SecurityCreateUser">>,
              <<"params">> := #{<<"name">> := Name,
                                <<"options">> := Options}} ->
                riak_core_security:add_user(
                  binary_to_list(Name),
                  maps:to_list(deep_binary_to_list(Options)));
            #{<<"action">> := <<"SecurityUpdateUser">>,
              <<"params">> := #{<<"name">> := Name,
                                <<"options">> := Options}} ->
                riak_core_security:alter_user(
                  binary_to_list(Name),
                  maps:to_list(deep_binary_to_list(Options)));
            #{<<"action">> := <<"SecurityDeleteUser">>,
              <<"params">> := #{<<"name">> := Name}} ->
                riak_core_security:del_user(binary_to_list(Name));

            #{<<"action">> := <<"SecurityListGroups">>} ->
                A = [ begin
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
                {ok, A};
            #{<<"action">> := <<"SecurityCreateGroup">>,
              <<"params">> := #{<<"name">> := Name,
                                <<"options">> := Options}} ->
                riak_core_security:add_group(
                  binary_to_list(Name),
                  maps:to_list(deep_binary_to_list(Options)));
            #{<<"action">> := <<"SecurityUpdateGroup">>,
              <<"params">> := #{<<"name">> := Name,
                                <<"options">> := Options}} ->
                riak_core_security:alter_group(
                  binary_to_list(Name),
                  maps:to_list(deep_binary_to_list(Options)));
            #{<<"action">> := <<"SecurityDeleteGroup">>,
              <<"params">> := #{<<"name">> := Name}} ->
                riak_core_security:del_group(binary_to_list(Name));

            #{<<"action">> := <<"SecurityAddUserGroup">>,
              <<"params">> := #{<<"user">> := User,
                                <<"group">> := Group}} ->
                case lists:keyfind(User, 1, riak_core_security:get_users()) of
                    {_, [PL|_]} ->
                        GG0 = proplists:get_value("groups", PL, []),
                        GG9 = string:join([binary_to_list(A) || A <- lists:usort(GG0 ++ [Group])], ","),
                        Options = [{"groups", GG9}],
                        riak_core_security:alter_user(binary_to_list(User), Options);
                    _ ->
                        {error, notfound}
                end;
            #{<<"action">> := <<"SecurityDeleteUserGroup">>,
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

            #{<<"action">> := <<"SecurityAddUserGrant">>,
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
            #{<<"action">> := <<"SecurityDeleteUserGrant">>,
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

            #{<<"action">> := <<"SecurityAddGroupGrant">>,
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
            #{<<"action">> := <<"SecurityDeleteGroupGrant">>,
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

            #{<<"action">> := <<"SecurityListPermissions">>} ->
                {ok, PP} = application:get_env(riak_core, permissions),
                {ok, lists:flatten(
                       [[iolist_to_binary([atom_to_list(App), $., atom_to_list(K)]) || K <- KK]
                        || {App, KK} <- PP]
                      )}
        end,

    case Res of
        ok ->
            {ok, <<"ok">>};
        {ok, GoodResult} ->
            {ok, GoodResult};
        {error, notfound} ->
            {404, <<"No such user or group">>}
    end.


jsonify_scope({A}) ->
    list_to_binary(A);
jsonify_scope({A, any}) ->
    iolist_to_binary([A, $:, $*]);
jsonify_scope({A, B}) ->
    iolist_to_binary([A, $:, B]).

jsonify_permissions(PP) ->
    [list_to_binary(P) || P <- PP].

deep_binary_to_list(A) ->
    maps:fold(
      fun(K, V, Q) when is_binary(V) -> maps:put(binary_to_list(K), binary_to_list(V), Q);
         (K, V, Q) when is_map(V) -> maps:put(binary_to_list(K), deep_binary_to_list(V), Q)
      end, #{}, A).
