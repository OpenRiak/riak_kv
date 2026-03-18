%% -------------------------------------------------------------------
%%
%% riak_kv_wm_ctl_cluster: Riak Control cluster ops.
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

-module(riak_kv_wm_ctl_cluster).

-export([process_request/1]).

-include_lib("kernel/include/logger.hrl").

-spec process_request(#{}) ->
          {ok, binary() | map()} | {400..500, binary()}.
process_request(Request) ->
    Res =
        case Request of
            #{<<"action">> := <<"GetClusterStatus">>} ->
                get_cluster();
            #{<<"action">> := <<"ClearPlan">>} ->
                riak_core_claimant:clear();
            #{<<"action">> := <<"CommitPlan">>} ->
                riak_core_claimant:commit();

            #{<<"action">> := <<"StageJoin">>,
              <<"params">> := #{<<"node">> := A}} ->
                Node = binary_to_atom(A),
                {ok, Ring} = riak_core_ring_manager:get_my_ring(),
                case riak_core_ring:all_members(Ring) of
                    [_Me] ->
                        riak_core:staged_join(Node);
                    _ ->
                        try rpc:call(Node, riak_core, staged_join, [node()]) of
                            X -> X
                        catch
                            exit:R ->
                                ?LOG_WARNING("staged_join on ~s failed: ~p",
                                             [Node, R]),
                                {badrpc, nodedown}
                        end
                end;
            #{<<"action">> := <<"StageLeave">>,
              <<"params">> := #{<<"node">> := A}} ->
                riak_core_claimant:leave_member(binary_to_atom(A));
            #{<<"action">> := <<"StageRemove">>,
              <<"params">> := #{<<"node">> := A}} ->
                riak_core_claimant:remove_member(binary_to_atom(A));
            #{<<"action">> := <<"StageReplace">>,
              <<"params">> := #{<<"node">> := A1,
                                <<"with">> := A2}} ->
                riak_core_claimant:replace(binary_to_atom(A1), binary_to_atom(A2));
            #{<<"action">> := <<"StageForceReplace">>,
              <<"params">> := #{<<"node">> := A1,
                                <<"with">> := A2}} ->
                riak_core_claimant:force_replace(binary_to_atom(A1), binary_to_atom(A2));

            #{<<"action">> := <<"DownNode">>,
              <<"params">> := #{<<"node">> := A}} ->
                riak_core:down(binary_to_atom(A));

            #{<<"action">> := <<"StopNode">>,
              <<"params">> := #{<<"node">> := A}} ->
                Node = binary_to_atom(A),
                try rpc:call(Node, riak_core, stop, []) of
                    X -> X
                catch
                    exit:R ->
                        ?LOG_WARNING("node stop on ~s failed: ~p", [Node, R]),
                        {badrpc, nodedown}
                end;

            #{<<"action">> := <<"GetNodeAppEnv">>,
              <<"params">> := #{<<"node">> := A}} ->
                AllAppEnvs = collect_app_env(binary_to_atom(A)),
                {ok, iolist_to_binary(io_lib:format("~120p", [AllAppEnvs]))};
            #{<<"action">> := <<"PutNodeAppEnv">>,
              <<"params">> := #{<<"node">> := A,
                                <<"config">> := B}} ->
                apply_app_env(binary_to_atom(A), B);
            #{<<"action">> := <<"GetNodeAdvancedConfig">>,
              <<"params">> := #{<<"node">> := A}} ->
                {ok, AdvConfig} = get_advanced_config(binary_to_atom(A)),
                {ok, iolist_to_binary(io_lib:format("~120p", [AdvConfig]))};
            #{<<"action">> := <<"PutNodeAdvancedConfig">>,
              <<"params">> := #{<<"node">> := A,
                                <<"config">> := B}} ->
                write_advanced_config(binary_to_atom(A), B);

            #{<<"action">> := <<"RestartNode">>,
              <<"params">> := #{<<"node">> := A}} ->
                ok = signal_restart(binary_to_atom(A)),
                spawn(
                  fun() ->
                          timer:sleep(3000 + 2000),
                          ?LOG_NOTICE("For restart via riak_cnotrol to work,"
                                      " make sure riak-deadmanshand is running")
                  end),
                ok;
            #{<<"action">> := A} ->
                {400, iolist_to_binary([<<"Missing request parameters for action ">>, A])}
        end,

    case Res of
        ok ->
            {ok, <<"ok">>};
        {ok, GoodResult} ->
            {ok, GoodResult};
        {error, ring_not_ready} ->
            {425, <<"Ring not ready">>};
        {error, claimant_is_down} ->
            {412, <<"Claimant node is down">>};
        {error, invalid_replacement} ->
            {409, <<"Invalid replacement">>};
        {error, already_replacement} ->
            {409, <<"Already a replacement">>};
        {error, not_member} ->
            {404, <<"Not a member">>};
        {error, not_single_node} ->
            {409, <<"Not a single node">>};
        {error, is_claimant} ->
            {409, <<"Node is claimant">>};
        {error, only_member} ->
            {412, <<"Node is last remaining">>};
        {error, self_join} ->
            {409, <<"Self-join">>};
        {error, already_leaving} ->
            {409, <<"Already leaving">>};
        {error, is_up} ->
            {412, <<"Node is up">>};
        {badrpc, nodedown} ->
            {412, <<"Node is down">>};
        {error, {bad_config, Extra}} ->
            {400, iolist_to_binary([<<"Bad config: ">>, Extra])};
        {error, PoorlyUnderstoodReason} ->
            ?LOG_WARNING("Error serving cluster request ~p: ~p",
                         [Request, PoorlyUnderstoodReason]),
            #{<<"action">> := Action} = Request,
            {500, iolist_to_binary(
                    [<<"Unexpected error while processing wm_ctl request ">>,
                     Action,
                     <<". Check logs around ">>,
                     calendar:system_time_to_rfc3339(erlang:system_time(second)),
                     <<" and report.">>])}
    end.


get_cluster() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Claimant = riak_core_ring:claimant(Ring),
    Nodes = get_nodes(Ring),
    {DownNodes, Pending_} = riak_core_status:transfers(),
    Pending = lists:map(
                fun({waiting_to_handoff, Node, Cnt}) ->
                        #{node => Node,
                          state => waiting_to_handoff,
                          count => Cnt};
                   ({stopped, Node, Cnt}) ->
                        #{node => Node,
                          state => stopped,
                          count => Cnt}
                end, Pending_),

    case get_plan() of
        {ok, Changes_, Claim} ->
            Current = [jsonify_current_node(
                         apply_status_change(Node, Changes_),
                         Claimant,
                         riak_core_node_watcher:services(proplists:get_value(node, Node)))
                       || Node <- Nodes],
            Final = [#{name => Name,
                       ring_pct => P1,
                       pending_pct => P2} || {Name, {P1, P2}} <- Claim],
            Changes = [#{name => Name,
                         action => Action} || {Name, Action} <- Changes_],

            Res = #{current_cluster => Current,
                    staged_changes => Changes,
                    final_cluster => Final,
                    down_nodes => DownNodes,
                    transfers => Pending},

            {ok, Res};
        {error, _} = ER ->
            ER
    end.

apply_status_change(Node, Changes) ->
    Name = proplists:get_value(node, Node),
    case proplists:get_value(Name, Changes) of
        undefined ->
            Node;
        {Action, Replacement} ->
            Node ++ [{action, Action}, {replacement, Replacement}];
        Action ->
            Node ++ [{action, Action}]
    end.

jsonify_current_node(Node, Claimant, Services) ->
    LWM = 0.1,
    MemUsed = proplists:get_value(mem_used, Node, null),
    MemTotal = proplists:get_value(mem_total, Node, null),
    Reachable = proplists:get_value(reachable, Node, false),
    LowMem = low_mem(Reachable, MemUsed, MemTotal, LWM),
    if Reachable ->
            #{name => proplists:get_value(node, Node),
              status => proplists:get_value(status, Node),
              system_info => proplists:get_value(system_info, Node),
              reachable => Reachable,
              services => Services,
              ring_pct => proplists:get_value(ring_pct, Node),
              pending_pct => proplists:get_value(pending_pct, Node),
              mem_total => MemTotal,
              mem_used => MemUsed,
              mem_erlang => proplists:get_value(mem_erlang, Node),
              low_mem => LowMem,
              is_me => (proplists:get_value(node, Node) == node()),
              claimant => (proplists:get_value(node, Node) == Claimant),
              staged_action => proplists:get_value(action, Node),
              replacement => proplists:get_value(replacement, Node)};
       el/=se ->
            #{name => proplists:get_value(node, Node),
              status => proplists:get_value(status, Node),
              reachable => Reachable,
              is_me => false}
    end.


get_nodes(Ring) ->
    Members = riak_core_ring:all_member_status(Ring),
    [get_member_info(M, Ring) || M <- Members].

get_member_info({Node, Status}, Ring) ->
    RingSize = riak_core_ring:num_partitions(Ring),
    Indices = riak_core_ring:indices(Ring, Node),
    FutureIndices = riak_core_ring:future_indices(Ring, Node),
    PctRing = length(Indices) / RingSize,
    PctPending = length(FutureIndices) / RingSize,

    case rpc:call(Node, riak_kv_util, node_info_for_riak_control, []) of
        {badrpc, _} ->
            [{node, Node},
             {status, down}];
        MemberInfo ->
            MemberInfo ++ [{node, Node},
                           {status, Status},
                           {ring_pct, PctRing},
                           {pending_pct, PctPending}
                          ]
    end.

low_mem(_Reachable = false, _, _, _) ->
    0.0;
low_mem(true, MemUsed, MemTotal, LWM) ->
    case MemTotal of
        undefined ->
            false;
        _ ->
            1.0 - (MemUsed/MemTotal) < LWM
    end.


get_plan() ->
    try riak_core_claimant:plan() of
        {error, Error} ->
            {error, Error};
        {ok, Changes, NextRings} ->
            case Changes of
                [] ->
                    {ok, [], []};
                _ ->
                    {ok, Changes, compute_final_ring_claim(NextRings)}
            end
    catch
        _:{{nodedown, _}, _} ->
            {error, claimant_is_down}
    end.

compute_final_ring_claim(Rings) ->
    {_, FinalRing} = lists:last(Rings),
    nodes_and_claim_percentages(FinalRing).

nodes_and_claim_percentages(Ring) ->
    Nodes = lists:keysort(2, riak_core_ring:all_member_status(Ring)),
    [{Name, riak_core_console:pending_claim_percentage(Ring, Name)} ||
        {Name, _} <- Nodes].


collect_app_env(Node) when Node == node() ->
    riak_kv_util:collect_all_app_env();
collect_app_env(Node) ->
    rpc:call(Node, riak_kv_util, collect_all_app_env, []).

apply_app_env(Node, AppEE_s) ->
    case erl_parse:parse_term(
           element(2, erl_scan:string(
                        binary_to_list(AppEE_s) ++ "."))) of
        {ok, AppEE} ->
            apply_app_env2(Node, AppEE);
        {error, BadTerm} ->
            {error, {bad_config, io_lib:format("~p", [BadTerm])}}
    end.
apply_app_env2(Node, AppEE) when Node == node() ->
    riak_kv_util:apply_app_env(AppEE);
apply_app_env2(Node, AppEE) ->
    rpc:call(Node, riak_kv_util, apply_app_env, [AppEE]).

get_advanced_config(Node) when Node == node() ->
    riak_kv_util:get_advanced_config();
get_advanced_config(Node) ->
    rpc:call(Node, riak_kv_util, get_advanced_config, []).

write_advanced_config(Node, Blob) ->
    case erl_parse:parse_term(
           element(2, erl_scan:string(
                        binary_to_list(Blob) ++ "."))) of
        {ok, EE} when is_list(EE) ->
            write_advanced_config2(Node, iolist_to_binary([Blob, $.]));
        {error, BadTerm} ->
            {error, {bad_config, io_lib:format("~p", [BadTerm])}}
    end.
write_advanced_config2(Node, Blob) when Node == node() ->
    riak_kv_util:write_advanced_config(Blob);
write_advanced_config2(Node, Blob) ->
    rpc:call(Node, riak_kv_util, write_advanced_config, [Blob]).


signal_restart(Node) when Node == node() ->
    riak:deadmans_hand_restart();
signal_restart(Node) ->
    rpc:call(Node, riak, deadmans_hand_restart, []).
