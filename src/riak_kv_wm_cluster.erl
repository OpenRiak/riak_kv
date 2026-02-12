%% -------------------------------------------------------------------
%%
%% riak_kv_wm_cluster: a Webmachine resource for cluster ops
%%                     Called by riak_control.
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

-module(riak_kv_wm_cluster).

-export([init/1,
         options/2,
         service_available/2,
         allowed_methods/2,
         is_authorized/2,
         forbidden/2,
         content_types_provided/2,
         content_types_accepted/2,
         process_post/2,
         to_json/2
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
    {['OPTIONS', 'GET', 'POST'], wrq:set_resp_headers(riak_kv_wm_utils:cors_headers(), RD), Ctx}.

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

-spec content_types_accepted(#wm_reqdata{}, #context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    {[{"application/json", from_json}], RD, Ctx}.


-spec to_json(#wm_reqdata{}, #context{}) -> {binary(), #wm_reqdata{}, #context{}}.
to_json(RD, Context) ->
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
        {error, ring_not_ready} ->
            {{halt, 425}, wrq:append_to_resp_body(<<"ring not ready">>, RD), Context};
        {error, claimant_is_down} ->
            {{halt, 400}, wrq:append_to_resp_body(<<"claimant node is down">>, RD), Context};
        {error, Reason} ->
            {{halt, 500}, wrq:set_resp_body(iolist_to_binary(io_lib:format("~p", [Reason])), RD), Context};
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

            {mochijson2:encode(Res), RD, Context}
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




-spec process_post(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
process_post(RD, Context) ->
    Request = #{<<"action">> := Action} =
        mochijson2:decode(wrq:req_body(RD), [{format, map}]),
    try
        Res =
            case Request of
                #{<<"action">> := <<"clear_plan">>} ->
                    riak_core_claimant:clear();
                #{<<"action">> := <<"commit_plan">>} ->
                    riak_core_claimant:commit();

                #{<<"action">> := <<"stage_join">>,
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
                                    logger:warning("rpc:call(~p, riak_core, staged_join, [~p]) failed with reason: ~p", [Node, node(), R]),
                                    {badrpc, nodedown}
                            end
                    end;
                #{<<"action">> := <<"stage_leave">>,
                  <<"params">> := #{<<"node">> := A}} ->
                    riak_core_claimant:leave_member(binary_to_atom(A));
                #{<<"action">> := <<"stage_remove">>,
                  <<"params">> := #{<<"node">> := A}} ->
                    riak_core_claimant:remove_member(binary_to_atom(A));
                #{<<"action">> := <<"stage_replace">>,
                  <<"params">> := #{<<"node">> := A1,
                                    <<"with">> := A2}} ->
                    riak_core_claimant:replace(binary_to_atom(A1), binary_to_atom(A2));
                #{<<"action">> := <<"stage_force_replace">>,
                  <<"params">> := #{<<"node">> := A1,
                                    <<"with">> := A2}} ->
                    riak_core_claimant:force_replace(binary_to_atom(A1), binary_to_atom(A2));

                #{<<"action">> := <<"down_node">>,
                  <<"params">> := #{<<"node">> := A}} ->
                    riak_core:down(binary_to_atom(A));

                #{<<"action">> := <<"stop_node">>,
                  <<"params">> := #{<<"node">> := A}} ->
                    Node = binary_to_atom(A),
                    try rpc:call(Node, riak_core, stop, []) of
                        X -> X
                    catch
                        exit:R ->
                            logger:warning("rpc:call(~p, riak_core, stop, []) failed with reason: ~p", [Node, R]),
                            {badrpc, nodedown}
                    end;

                #{<<"action">> := <<"get_app_env">>,
                  <<"params">> := #{<<"node">> := A}} ->
                    AllAppEnvs = collect_app_env(binary_to_atom(A)),
                    {ok, iolist_to_binary(io_lib:format("~120p", [AllAppEnvs]))};
                #{<<"action">> := <<"put_app_env">>,
                  <<"params">> := #{<<"node">> := A,
                                    <<"config">> := B}} ->
                    apply_app_env(binary_to_atom(A), B);
                #{<<"action">> := <<"get_advanced_config">>,
                  <<"params">> := #{<<"node">> := A}} ->
                    {ok, AdvConfig} = get_advanced_config(binary_to_atom(A)),
                    {ok, iolist_to_binary(io_lib:format("~120p", [AdvConfig]))};
                #{<<"action">> := <<"put_advanced_config">>,
                  <<"params">> := #{<<"node">> := A,
                                    <<"config">> := B}} ->
                    write_advanced_config(binary_to_atom(A), B);

                #{<<"action">> := <<"restart">>,
                  <<"params">> := #{<<"node">> := A}} ->
                    ok = signal_restart(binary_to_atom(A)),
                    spawn(
                      fun() ->
                              timer:sleep(3000 + 2000),
                              ?LOG_NOTICE("For restart via riak_cnotrol to work, make sure riak-deadmanshand is running")
                      end),
                    ok
            end,
        ResF = fun(A) -> wrq:append_to_resp_body(mochijson2:encode(#{result => A}), RD) end,
        case Res of
            ok ->
                {true, ResF(<<"ok">>), Context};
            {ok, ConfigString} ->
                {true, ResF(ConfigString), Context};
            {error, ring_not_ready} ->
                {{halt, 425}, ResF(<<"ring not ready">>), Context};
            {error, invalid_replacement} ->
                {true, ResF(<<"invalid replacement">>), Context};
            {error, already_replacement} ->
                {true, ResF(<<"already a replacement">>), Context};
            {error, not_member} ->
                {true, ResF(<<"not a member">>), Context};
            {error, not_single_node} ->
                {true, ResF(<<"not a single node">>), Context};
            {error, is_claimant} ->
                {true, ResF(<<"node is claimant">>), Context};
            {error, only_member} ->
                {true, ResF(<<"node is last remaining">>), Context};
            {error, self_join} ->
                {true, ResF(<<"self-join">>), Context};
            {error, already_leaving} ->
                {true, ResF(<<"already leaving">>), Context};
            {error, is_up} ->
                {true, ResF(<<"node is up">>), Context};
            {error, bad_config} ->
                {true, ResF(<<"bad config">>), Context};
            {error, PoorlyUnderstoodReason} ->
                ?LOG_WARNING("Error serving cluster request ~p: ~p", [Action, PoorlyUnderstoodReason]),
                {{halt, 400}, ResF(<<"unexpected error condition">>), Context};
            {badrpc, nodedown} ->
                {{halt, 412}, ResF(<<"node is down">>), Context}
        end
    catch
        _t:_e:_st ->
            ?LOG_WARNING("malformed action: ~p:~p  ~p", [_t, _e, _st]),
            {{halt, 400}, wrq:append_to_resp_body(<<"malformed action">>, RD), Context}
    end.

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
        {error, _e} ->
            ?LOG_WARNING("malformed term: ~p", [_e]),
            {error, bad_config}
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
        _ ->
            {error, bad_config}
    end.
write_advanced_config2(Node, Blob) when Node == node() ->
    riak_kv_util:write_advanced_config(Blob);
write_advanced_config2(Node, Blob) ->
    rpc:call(Node, riak_kv_util, write_advanced_config, [Blob]).


signal_restart(Node) when Node == node() ->
    riak:deadmans_hand_restart();
signal_restart(Node) ->
    rpc:call(Node, riak, deadmans_hand_restart, []).
