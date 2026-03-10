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

-module(riak_kv_vnode_status_cli).

-behaviour(clique_handler).

-include_lib("kernel/include/logger.hrl").

-export([register_cli/0]).

register_cli() ->
    register_all_usage(),
    register_all_commands().

register_all_usage() ->
    clique:register_usage(["riak-admin", "vnode-status"], main_usage()).

register_all_commands() ->
    lists:foreach(
      fun(Args) -> apply(clique, register_command, Args) end,
      [get_vnode_status_specs()]).

main_usage() ->
    ["riak-admin vnode-status [-n|--node NODE|all] [-p|--partition PARTITION|all]\n",
     "Print vnode status, including backend stats and info,\n",
     "on specified NODE and PARTITION (defaults to current node\n",
     "and all partitions), as a json object.\n"
    ].


-define(NODEOPT,
        {node, [{shortname, "n"},
                {longname, "node"},
                {typecast, fun to_node/1}]}).
-define(PARTITIONOPT, {partition, [{shortname, "p"},
                                   {longname, "partition"},
                                   {typecast, fun to_partition/1}]}).


get_vnode_status_specs() ->
    [["riak-admin", "vnode-status"],
     '_', [?NODEOPT, ?PARTITIONOPT],
     fun get_vnode_status_cmd/3
    ].


get_vnode_status_cmd([_, _ | Args], _, Options) ->
    Nodes = extract_nodes(Options),
    Partitions = extract_vnodes(Options),
    PerNode = [{Node, [{integer_to_binary(Idx), jsonify1(X)} || {Idx, X} <- Res]}
                || {Res, Node} <- vnode_status_on_nodes(Nodes, Partitions, [])],
    case Args of
        [] ->
            [clique_status:text(
               riak_kv_wm_json:encode(map_from_deep_list(PerNode)))];
        _ ->
            clique_status:usage()
    end.

map_from_deep_list(A) when is_map(A) ->
    maps:fold(fun(K, V, Q) -> Q#{K => map_from_deep_list(V)} end, #{}, A);
map_from_deep_list([{_,_}|_] = A) ->
    lists:foldl(fun({K, V}, Q) -> Q#{K => map_from_deep_list(V)} end, #{}, A);
map_from_deep_list(A) when is_list(A) ->
    lists:map(fun map_from_deep_list/1, A);
map_from_deep_list(A) -> A.


jsonify1(PP) ->
    lists:append([jsonify2(P) || P <- PP]).

jsonify2({P, undefined}) ->
    [{P, null}];
jsonify2({backend_status, MB, BB}) when MB == riak_kv_multi_backend;
                                        MB == riak_kv_multi_prefix_backend ->
    [{backend, MB},
     {backend_status, [{N, [{mod, Mod} | jsonify_backend(Mod, Status)]}
                       || {N, [{mod, Mod} | Status]} <- BB]}];
jsonify2({backend_status, N, BS}) ->
    [{backend, N}, {backend_status, jsonify_backend(N, BS)}];
jsonify2({vnodeid, Id}) ->
    [{vnodeid, printable_bin(Id)}];
jsonify2(P) -> [P].

jsonify_backend(Backend, PP) ->
    lists:append(
      [jsonify_backend_prop(Backend, P) || P <- PP]).

jsonify_backend_prop(_, {A, undefined}) ->
    [{A, null}];
jsonify_backend_prop(riak_kv_leveled_backend, {A, TS})
  when A =:= penciller_last_merge_time;
       A =:= journal_last_compaction_time ->
    [{A, iolist_to_binary(
           calendar:system_time_to_rfc3339(TS, [{unit, millisecond}]))}];
jsonify_backend_prop(riak_kv_leveled_backend, {penciller_work_backlog_status, {A, B1, B2}}) ->
    [{penciller_work_backlog_status, #{work_items => A, backlog => B1, l0_full => B2}}];
jsonify_backend_prop(riak_kv_leveled_backend, Unchanged) ->
    [Unchanged];

jsonify_backend_prop(riak_kv_memory_backend, {TableStatus, TSProps})
  when is_list(TSProps) ->
    [{TableStatus, any_ref_or_pid_to_string(TSProps, [])}];
jsonify_backend_prop(riak_kv_eleveldb_backend, {stats, StatsString}) ->
    case re:run(StatsString,
                <<"(\\d+) +(\\d+) +(\\d+) +(\\d+) +(\\d+) +(\\d+)">>,
                [{capture, all, binary}]) of
        {match, [_|Values]} ->
            lists:zip(
              [compactions, level, files_size_mb, time, read_mb, write_mb],
              [binary_to_integer(X) || X <- Values]);
        _ ->
            %% on fresh start, eleveldb (sometimes?) doesn't report these items, so:
            [{compactions, 0},
             {level, -1},
             {files_size_mb, 0},
             {time, 0},
             {read_mb, 0},
             {write_mb, 0}]
    end;

jsonify_backend_prop(riak_kv_bitcask_backend, {status, StatusTuples}) ->
    [{status, [[{filename, list_to_binary(A1)}, {fragmented, A2},
                {dead_bytes, A3}, {total_bytes, A4}] || {A1, A2, A3, A4} <- StatusTuples]}];

jsonify_backend_prop(_, AsIs) ->
    [AsIs].

any_ref_or_pid_to_string([], Q) ->
    Q;
any_ref_or_pid_to_string([{A, B}|CC], Q) when is_pid(B) ->
    any_ref_or_pid_to_string(CC, [{A, list_to_binary(pid_to_list(B))} | Q]);
any_ref_or_pid_to_string([{A, B}|CC], Q) when is_reference(B) ->
    any_ref_or_pid_to_string(CC, [{A, list_to_binary(ref_to_list(B))} | Q]);
any_ref_or_pid_to_string([AB|CC], Q) ->
    any_ref_or_pid_to_string(CC, [AB | Q]).



vnode_status_on_nodes([], _, Q) ->
    Q;
vnode_status_on_nodes([N|Rest], PP, Q) when N == node() ->
    Preflists = filter(PP, riak_core_vnode_manager:all_index_pid(riak_kv_vnode)),
    Res = riak_kv_vnode:vnode_status(Preflists),
    vnode_status_on_nodes(Rest, PP, [{Res, N} | Q]);
vnode_status_on_nodes([N|Rest], PP, Q) ->
    Preflists = filter(PP, rpc:call(N, riak_core_vnode_manager, all_index_pid, [riak_kv_vnode])),
    Res = rpc:call(N, riak_kv_vnode, vnode_status, [Preflists]),
    vnode_status_on_nodes(Rest, PP, [{Res, N} | Q]).

filter(all, Preflists) ->
    Preflists;
filter(PP, Preflists) ->
    lists:filter(
      fun({P, _}) -> lists:member(P, PP) end, Preflists).

extract_nodes(Options) ->
    NN = [N || {node, N} <- Options],
    case lists:member(all, NN) of
        true ->
            [node() | nodes()];
        false when NN /= [] ->
            NN;
        _ ->
            [node()]
    end.

extract_vnodes(Options) ->
    PP = [P || {partition, P} <- Options],
    case lists:member(all, PP) or (length(PP) == 0) of
        true ->
            all;
        false ->
            PP
    end.

to_node("all") ->
    all;
to_node(A) ->
    clique_typecast:to_node(A).

to_partition("all") ->
    all;
to_partition(A) ->
    try
        list_to_integer(A)
    catch _:_ ->
            {error, bad_partition}
    end.

printable_bin(K) ->
    iolist_to_binary(["0x", mochihex:to_hex(K)]).
