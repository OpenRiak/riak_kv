%% -------------------------------------------------------------------
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

-module(riak_kv_tictacaae_report).

-export([produce/0]).

%% The report is collected in both cli and ag (Ag, argentum, silver,
%% SilverMachine, aha!) handlers, so let's isolate it into a module of
%% its own
-spec produce() -> proplists:proplist().
produce() ->
    VVSS =
        lists:append(
          [case sys:get_state(P) of
               {active, _CoreVnodeState =
                    {state, Idx, riak_kv_vnode, VSx, _, _, _, _, _, _, _, _}} ->
                   [{Idx, VSx}];
               _ ->
                   []
           end || {_, P, _, _} <- supervisor:which_children(riak_core_vnode_sup)]),
    [begin
         AAECntrl = riak_kv_vnode:aae_controller(VNState),
         TictacRebuilding = riak_kv_vnode:aae_rebuilding(VNState),
         InProgress = TictacRebuilding /= false,
         AAEReport = aae_controller:aae_produce_progress_report(AAECntrl),
         IsEmpty = proplists:get_value(is_empty, AAEReport),
         LastRebuild = proplists:get_value(last_rebuild, AAEReport),
         NextRebuild = proplists:get_value(next_rebuild, AAEReport),
         Status =
             case {IsEmpty, LastRebuild, InProgress, NextRebuild} of
                 {true, _, _, _} ->
                     empty;
                 {_, never, false, Scheduled} when Scheduled /= undefined ->
                     partial;
                 {_, Built, false, _} when Built /= never ->
                     built;
                 {_, Built, true, _} when Built /= never ->
                     rebuilding;
                 {_, never, true, _} ->
                      building
             end,
         Extra = [{status, Status},
                  {partition, Idx},
                  {controller_pid, list_to_binary(pid_to_list(AAECntrl))}
                 ],
         lists:keydelete(is_empty, 1, AAEReport ++ Extra)
     end || {Idx, VNState} <- VVSS].
