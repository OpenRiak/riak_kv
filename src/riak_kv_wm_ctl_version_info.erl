%% -------------------------------------------------------------------
%%
%% riak_kv_wm_ctl_version_info: Riak Control, SystemGetVersionInfo request.
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

-module(riak_kv_wm_ctl_version_info).

-export([process_request/1]).

-include_lib("kernel/include/logger.hrl").

-spec process_request(#{}) ->
          {ok, map()} | {412, binary()}.
process_request(Request) ->
    Res =
        case Request of
            #{<<"action">> := <<"SystemGetVersionInfo">>,
              <<"params">> := Params} ->
                Node =
                    case maps:get(<<"node">>, Params, undefined) of
                        undefined ->
                            node();
                        Defined ->
                            binary_to_atom(Defined)
                    end,
                try
                    A = rpc:call(Node, riak_kv_util, system_info, []),
                    {ok, A#{https_listeners => get_http_listeners()}}
                catch
                    exit:R ->
                        ?LOG_WARNING("rpc call to riak_kv_util:system_info()"
                                     " on node ~s failed: ~p", [Node, R]),
                        {badrpc, nodedown}
                end
        end,
    case Res of
        {ok, GoodResult} ->
            {ok, GoodResult};
        {badrpc, nodedown} ->
            {412, <<"Node is down">>}
    end.

get_http_listeners() ->
    lists:foldl(
      fun(N, Q) ->
              case rpc:call(N, application, get_env, [riak_api, https]) of
                  {ok, [{IP, Port}]} ->
                      Q#{N => iolist_to_binary(["https://", IP, $:, integer_to_binary(Port)])};
                  _ ->
                      Q
              end
       end, #{}, nodes()
     ).
