%% -------------------------------------------------------------------
%%
%% riak_kv_wm_ctl_vnode: Riak Control, VnodeGetStatus request.
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

-module(riak_kv_wm_ctl_vnode).

-export([process_request/1]).

-include_lib("kernel/include/logger.hrl").

-spec process_request(#{}) ->
          {ok, map()} | {400..500, binary()}.
process_request(Request) ->
    Res =
        case Request of
            #{<<"action">> := <<"VnodeGetStatus">>,
              <<"params">> := Params = #{<<"preflists">> := PrefLists_}} ->
                Node =
                    case maps:get(<<"node">>, Params, undefined) of
                        undefined ->
                            node();
                        Defined ->
                            binary_to_atom(Defined)
                    end,
                try
                    Selection = rpc:call(Node, riak_core_vnode_manager, all_index_pid, [riak_kv_vnode]),
                    PrefLists = select_preflists(Selection, PrefLists_),
                    {ok, jsonify_vnode_status_list(
                           rpc:call(Node, riak_kv_vnode, vnode_status, [PrefLists]))}
                catch
                    exit:R ->
                        ?LOG_WARNING("rpc call to riak_core_vnode_manager:all_index_pid(riak_kv_vnode)"
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

select_preflists(All, <<"all">>) ->
    All;
select_preflists(_All, Some) ->
    Some.

jsonify_vnode_status_list(AA) when is_list(AA) ->
    map_from_deep_list(
      [jsonify_vnode_status(Idx, PP) || {Idx, PP} <- AA]).

jsonify_vnode_status(Idx, PP) ->
    lists:foldl(
      fun({backend_status, Mod, SubPP}, Q) ->
              [{backend_status, [{mod, Mod} | jsonify_backend_status(Mod, SubPP)]} | Q];
         ({vnodeid, A}, Q) ->
              [{vnodeid, list_to_binary(mochihex:to_hex(A))} | Q];
         (AsIs, Q) ->
              [AsIs | Q]
      end,
      [{idx, integer_to_binary(Idx)}], PP).

jsonify_backend_status(riak_kv_leveled_backend, PP) ->
    lists:foldl(
      fun({Item, undefined}, Q) ->
              [{Item, null} | Q];
         ({Item, A}, Q) when Item == penciller_last_merge_time;
                             Item == journal_last_compaction_time ->
              [{Item, list_to_binary(calendar:system_time_to_rfc3339(A, [{unit, millisecond}]))} | Q];
         ({journal_last_compaction_result, {NCompacted, Score}}, Q) ->
              [{journal_last_compaction_result, #{files_compacted => NCompacted,
                                                  score => Score}} | Q];
         ({level_files_count, M0}, Q) ->
              M = maps:fold(fun(L, C, QQ) -> [#{level => L, count => C} | QQ] end, [], M0),
              [{level_files_count, M} | Q];
         ({avg_compaction_score_sample, []}, Q) ->
              Q;
         ({avg_compaction_score_sample, L}, Q) ->
              [{avg_compaction_score, lists:sum(L) / length(L)} | Q];
         ({penciller_work_backlog_status, {WorkItems, Backlog, L0Full}}, Q) ->
              [{penciller_work_backlog_status, #{work_items => WorkItems,
                                                 backlog => Backlog,
                                                 l0_full => L0Full}} | Q];
         (AsIs, Q) ->
              [AsIs | Q]
      end,
      [], PP);
jsonify_backend_status(riak_kv_bitcask_backend, PP) ->
    lists:foldl(
      fun({status, StatusTuples}, Q) ->
              [{status, [[{filename, list_to_binary(filename:basename(A1))},
                          {fragmented, int_to_bool(A2)},
                          {dead_bytes, A3},
                          {total_bytes, A4}]
                         || {A1, A2, A3, A4} <- StatusTuples]} | Q];
         (AsIs, Q) ->
              [AsIs | Q]
      end, [], PP);
jsonify_backend_status(riak_kv_eleveldb_backend, PP) ->
    lists:foldl(
      fun({stats, StatsString}, Q) ->
              case re:run(StatsString,
                          <<"(\\d+) +(\\d+) +(\\d+) +(\\d+) +(\\d+) +(\\d+)">>,
                          [{capture, all, binary}]) of
                  {match, [_|Values]} ->
                      Val = lists:zip(
                              [compactions, level, files_size_mb, time, read_mb, write_mb],
                              [binary_to_integer(X) || X <- Values]),
                      Val ++ Q;
                  _ ->
                      Val = lists:zip(
                              [compactions, level, files_size_mb, time, read_mb, write_mb],
                              [null, null, null, null, null, null]),
                      Val ++ Q
              end;
         (AsIs, Q) ->
              [AsIs | Q]
      end, [], PP);
jsonify_backend_status(riak_kv_memory_backend, PP) ->
    lists:foldl(
      fun({TableStatus, TSProps}, Q) when is_list(TSProps) ->
              [{TableStatus, any_ref_or_pid_to_string(TSProps, [])}] ++ Q;
         (AsIs, Q) ->
              [AsIs | Q]
      end, [], PP);

jsonify_backend_status(_OtherBackend, PP) ->
    PP.

any_ref_or_pid_to_string([], Q) ->
    Q;
any_ref_or_pid_to_string([{A, B}|CC], Q) when is_pid(B) ->
    any_ref_or_pid_to_string(CC, [{A, list_to_binary(pid_to_list(B))} | Q]);
any_ref_or_pid_to_string([{A, B}|CC], Q) when is_reference(B) ->
    any_ref_or_pid_to_string(CC, [{A, list_to_binary(ref_to_list(B))} | Q]);
any_ref_or_pid_to_string([AB|CC], Q) ->
    any_ref_or_pid_to_string(CC, [AB | Q]).

int_to_bool(0) -> false;
int_to_bool(_) -> true.


map_from_deep_list(A) when is_map(A) ->
    maps:fold(fun(K, V, Q) -> Q#{K => map_from_deep_list(V)} end, #{}, A);
map_from_deep_list([{_,_}|_] = A) ->
    lists:foldl(fun({K, V}, Q) -> Q#{K => map_from_deep_list(V)} end, #{}, A);
map_from_deep_list(A) when is_list(A) ->
    lists:map(fun map_from_deep_list/1, A);
map_from_deep_list(A) -> A.
