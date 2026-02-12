%% -------------------------------------------------------------------
%%
%% riak_kv_wm_vnode: a Webmachine resource for vnode_status
%%                   Called by riak_control.
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

-module(riak_kv_wm_vnode).

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
    Preflists = riak_core_vnode_manager:all_index_pid(riak_kv_vnode),
    Res = riak_kv_vnode:vnode_status(Preflists),
    {mochijson2:encode(Res), RD, Context}.


-spec process_post(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
process_post(RD, Context) ->
    Request = #{<<"action">> := Action} =
        mochijson2:decode(wrq:req_body(RD), [{format, map}]),
    try
        Res =
            case Request of
                #{<<"action">> := <<"get_vnode_status">>,
                  <<"params">> := #{<<"node">> := Node_,
                                    <<"preflists">> := PrefLists_}} ->
                    try
                        Node = binary_to_atom(Node_),
                        Selection = rpc:call(Node, riak_core_vnode_manager, all_index_pid, [riak_kv_vnode]),
                        PrefLists = select_preflists(PrefLists_, Selection),
                        jsonify_vnode_status_list(
                          rpc:call(Node, riak_kv_vnode, vnode_status, [PrefLists]))
                    catch
                        exit:R ->
                            logger:warning("rpc:call to node ~s failed: ~p", [Node_, R]),
                            {badrpc, nodedown}
                    end
            end,
        ResF = fun(A) -> wrq:append_to_resp_body(mochijson2:encode(#{result => A}), RD) end,
        case Res of
            ok ->
                {true, ResF(ok), Context};
            {ok, GoodResult} ->
                {true, wrq:append_to_resp_body(GoodResult, RD), Context};
            {error, PoorlyUnderstoodReason} ->
                ?LOG_WARNING("Error serving vnode request ~p: ~p", [Action, PoorlyUnderstoodReason]),
                {{halt, 400}, ResF(<<"unexpected error condition">>), Context};
            {badrpc, nodedown} ->
                {{halt, 412}, ResF(<<"node is down">>), Context}
        end
    catch
        _t:_e:_st ->
            ?LOG_WARNING("malformed action: ~p:~p  ~p", [_t, _e, _st]),
            {{halt, 400}, wrq:append_to_resp_body(<<"malformed action">>, RD), Context}
    end.

select_preflists(All, <<"all">>) ->
    All;
select_preflists(_All, Some) ->
    Some.

jsonify_vnode_status_list(AA) when is_list(AA) ->
    {ok, mochijson2:encode([jsonify_vnode_status(Idx, PP) || {Idx, PP} <- AA])}.

jsonify_vnode_status(Idx, PP) ->
    lists:foldl(
      fun({backend_status, Mod, SubPP}, Q) ->
              [{backend_status, [{mod, Mod}, {status, jsonify_backend_status(Mod, SubPP)}]} | Q];
         ({vnodeid, A}, Q) ->
              [{vnodeid, list_to_binary(mochihex:to_hex(A))} | Q];
         (AsIs, Q) ->
              [AsIs | Q]
      end,
      [{idx, integer_to_binary(Idx)}], PP).

jsonify_backend_status(riak_kv_leveled_backend, PP) ->
    maps:fold(
      fun(Item, undefined, Q) ->
              [{Item, null} | Q];
         (Item, A, Q) when Item == penciller_last_merge_time;
                           Item == journal_last_compaction_time ->
              [{Item, list_to_binary(calendar:system_time_to_rfc3339(A, [{unit, millisecond}]))} | Q];
         (journal_last_compaction_result, {NCompacted, Score}, Q) ->
              [{journal_last_compaction_result, #{files_compacted => NCompacted,
                                                  score => Score}} | Q];
         (level_files_count, M0, Q) ->
              M = maps:fold(fun(L, C, QQ) -> [#{level => L, count => C} | QQ] end, [], M0),
              [{level_files_count, M} | Q];
         (avg_compaction_score_sample, [], Q) ->
              Q;
         (avg_compaction_score_sample, L, Q) ->
              [{avg_compaction_score, lists:sum(L) / length(L)} | Q];
         (penciller_work_backlog_status, {WorkItems, Backlog, L0Full}, Q) ->
              [{penciller_work_backlog_status, #{work_items => WorkItems,
                                                 backlog => Backlog,
                                                 l0_full => L0Full}} | Q];
         (As, Is, Q) ->
              [{As, Is} | Q]
      end,
      [], PP);
jsonify_backend_status(_OtherBackend, PP) ->
    PP.
