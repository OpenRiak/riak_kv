%% -------------------------------------------------------------------
%%
%% riak_kv_wm_ctl_tictacaae: Riak Control, TictacaaeGetStatus request.
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

-module(riak_kv_wm_ctl_tictacaae).

-export([process_request/1]).

-include_lib("kernel/include/logger.hrl").

-spec process_request(#{}) ->
          {ok, map()} | {400..500, binary()}.
process_request(Request) ->
    Res =
        case Request of
            #{<<"action">> := <<"GetTictacaaeStatus">>,
              <<"params">> := Params} ->
                Node =
                    case maps:get(<<"node">>, Params, undefined) of
                        undefined ->
                            node();
                        Defined ->
                            binary_to_atom(Defined)
                    end,
                try
                    case rpc:call(Node, riak_kv_tictacaae_report, produce, []) of
                        {badrpc, _} ->
                            {error, tictacaae_passive};
                        A ->
                            {ok, jsonify_report(A)}
                    end
                catch
                    exit:R ->
                        ?LOG_WARNING("rpc call to riak_kv_tictacaae_report:produce()"
                                     " on node ~s failed: ~p", [Node, R]),
                        {badrpc, nodedown}
                end
        end,
    case Res of
        {ok, GoodResult} ->
            {ok, GoodResult};
        {error, tictacaae_passive} ->
            {412, <<"tictacaae not active">>};
        {badrpc, nodedown} ->
            {412, <<"Node is down">>}
    end.

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
