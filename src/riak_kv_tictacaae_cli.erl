%% -------------------------------------------------------------------
%%
%% Copyright (c) 2025 TI Tokyo.  All Rights Reserved.
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

-module(riak_kv_tictacaae_cli).

-behaviour(clique_handler).

-include_lib("kernel/include/logger.hrl").

-export([register_cli/0]).

register_cli() ->
    register_all_usage(),
    register_all_commands().

register_all_usage() ->
    clique:register_usage(["riak-admin", "tictacaae"], main_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuild-schedule"], rebuild_schedule_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuild-schedule", '*', '*'], rebuild_schedule_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "storeheads"], storeheads_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "storeheads", '*'], storeheads_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "tokenbucket"], tokenbucket_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "tokenbucket", '*'], tokenbucket_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuildtick"], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuildtick", '*'], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "exchangetick"], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "exchangetick", '*'], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "maxresults"], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "maxresults", '*'], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rangeboost"], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rangeboost", '*'], simple_envvar_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuild-soon"], rebuild_soon_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "rebuild-now"], rebuild_now_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "treestatus"], treestatus_usage()),
    clique:register_usage(["riak-admin", "tictacaae", "fold"], fold_usage()).

register_all_commands() ->
    lists:foreach(
      fun(Args) -> apply(clique, register_command, Args) end,
      [get_rebuild_schedule_specs(),
       set_rebuild_schedule_specs(),
       get_storeheads_specs(),
       set_storeheads_specs(),
       get_tokenbucket_specs(),
       set_tokenbucket_specs(),
       get_rebuildtick_specs(),
       set_rebuildtick_specs(),
       get_exchangetick_specs(),
       set_exchangetick_specs(),
       get_maxresults_specs(),
       set_maxresults_specs(),
       get_rangeboost_specs(),
       set_rangeboost_specs(),
       rebuild_soon_specs(),
       rebuild_now_specs(),
       treestatus_specs(),
       fold_specs()
      ]).

main(Fun, A, B, C) ->
    case application:get_env(riak_kv, tictacaae_active) of
        {ok, active} ->
            try
                Fun(A, B, C)
            catch
                _:_ ->
                    clique_status:usage()
            end;
        _ ->
            [clique_status_alert("tictacaae not active")]
    end.

main_usage() ->
    ["riak-admin tictacaae { rebuild-schedule | storeheads | tokenbucket\n",
     "                     | rebuildtick | exchangetick | maxresults | rangeboost\n",
     "                     | rebuild-soon | rebuild-now | treestatus | fold\n",
     "                     }\n",
     "See individual subcommand usage for options and arguments\n",
     "\n",
     "Unless given specifically with -n NODE, commands are executed on the current node.\n",
     "NODE (also PARTITION where applicable) can be \"all\".\n"
    ].


-define(NODEOPT, {node, [{shortname, "n"},
                         {longname, "node"},
                         {typecast, fun to_node/1}]}).
-define(PARTITIONOPT, {partition, [{shortname, "p"},
                                   {longname, "partition"},
                                   {typecast, fun to_partition/1}]}).

get_rebuild_schedule_specs() ->
    [["riak-admin", "tictacaae", "rebuild-schedule"],
     '_', [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun rebuild_schedule_cmd/3, A, B, C) end
    ].

set_rebuild_schedule_specs() ->
    [["riak-admin", "tictacaae", "rebuild-schedule", '*', '*'],
     '_', [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun rebuild_schedule_cmd/3, A, B, C) end
    ].

rebuild_schedule_usage() ->
    ["Set/show rebuild schedule on an AAE controller managing PARTITION on NODE:\n\n",
     "  riak admin tictacaae rebuild-schedule [-n NODE] [-p PARTITION] [RW RD]\n"
    ].

rebuild_schedule_cmd([_, _, _ | Args], _, Options) ->
    Nodes = extract_nodes(Options),
    Vnodes = extract_vnodes(Options, Nodes),
    case Args of
        [Arg1, Arg2] ->
            RS = {RW = ensure_valid_range(Arg1, 0, 5*365*24),  %% ~5 years in hours
                  RD = ensure_valid_range(Arg2, 0, 1*365*24*3600)},  %% one year
            post_set_fun(
              set_rebuild_schedule(Vnodes, RS),
              "rebuild_schedule",
              io_lib:format("RW: ~b, RD: ~b", [RW, RD]));
        [] ->
            FmtF = fun({ok, {RW, RD}}) ->
                           io_lib:format("RW: ~b, RD: ~b", [RW, RD]);
                      ({error, Reason}) ->
                           io_lib:format("(error: ~p)", [Reason])
                   end,
            [clique_status:table(
               [[{node, N}, {index, P}, {rebuild_schedule, FmtF(Res)}]
                || {Res, {P, N}} <- get_rebuild_schedule(Vnodes)])];
        _ ->
            clique_status:usage()
    end.


get_storeheads_specs() ->
    [["riak-admin", "tictacaae", "storeheads"],
     '_', [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun storeheads_cmd/3, A, B, C) end
    ].

set_storeheads_specs() ->
    [["riak-admin", "tictacaae", "storeheads", '*'],
     '_', [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun storeheads_cmd/3, A, B, C) end
    ].

storeheads_usage() ->
    ["Set/show storeheads flag on an AAE controller managing PARTITION on NODE:\n\n",
     "  riak admin tictacaae storeheads [-n NODE] [-p PARTITION] [VALUE]\n"
    ].

storeheads_cmd([_, _, _ | Args], _, Options) ->
    Nodes = extract_nodes(Options),
    Vnodes = extract_vnodes(Options, Nodes),
    io:format("Vnodes: ~p\n", [Vnodes]),
    case Args of
        [Arg1] ->
            Val = list_to_boolean(Arg1),
            post_set_fun(
              set_storeheads(Vnodes, Val),
              "storeheads",
              Val);
        [] ->
            FmtF = fun({error, Reason}) ->
                           io_lib:format("(error: ~p)", [Reason]);
                      (V) ->
                           io_lib:format("~s", [V])
                   end,
            [clique_status:table(
               [[{node, N}, {index, P}, {storeheads, FmtF(Res)}]
                || {Res, {P, N}} <- get_storeheads(Vnodes)])];
        _ ->
            clique_status:usage()
    end.


get_tokenbucket_specs() ->
    [["riak-admin", "tictacaae", "tokenbucket"],
     '_', [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun tokenbucket_cmd/3, A, B, C) end
    ].

set_tokenbucket_specs() ->
    [["riak-admin", "tictacaae", "tokenbucket", '*'],
     '_', [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun tokenbucket_cmd/3, A, B, C) end
    ].

tokenbucket_usage() ->
    ["Set/show tokenbucket flag on a vnode managing PARTITION on NODE:\n\n",
     "  riak admin tictacaae tokenbucket [-n NODE] [-p PARTITION] [VALUE]\n"
    ].

tokenbucket_cmd([_, _, _ | Args], _, Options) ->
    Nodes = extract_nodes(Options),
    Vnodes = extract_vnodes(Options, Nodes),
    case Args of
        [Arg1] ->
            Val = list_to_boolean(Arg1),
            post_set_fun(
              set_tokenbucket(Vnodes, Val),
              "tokenbucket",
              Val);
        [] ->
            FmtF = fun({error, Reason}) ->
                           io_lib:format("(error: ~p)", [Reason]);
                      (V) ->
                           io_lib:format("~s", [V])
                   end,
            [clique_status:table(
               [[{node, N}, {index, P}, {tokenbucket, FmtF(Res)}]
                || {Res, {P, N}} <- get_tokenbucket(Vnodes)])];
        _ ->
            clique_status:usage()
    end.


get_rebuildtick_specs() ->
    [["riak-admin", "tictacaae", "rebuildtick"],
     '_', [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].
set_rebuildtick_specs() ->
    [["riak-admin", "tictacaae", "rebuildtick", '*'],
     '_', [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].

get_exchangetick_specs() ->
    [["riak-admin", "tictacaae", "exchangetick"],
     '_', [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].
set_exchangetick_specs() ->
    [["riak-admin", "tictacaae", "exchangetick", '*'],
     '_', [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].

get_maxresults_specs() ->
    [["riak-admin", "tictacaae", "maxresults"],
     '_', [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].
set_maxresults_specs() ->
    [["riak-admin", "tictacaae", "maxresults", '*'],
     '_', [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].

get_rangeboost_specs() ->
    [["riak-admin", "tictacaae", "rangeboost"],
     '_', [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].
set_rangeboost_specs() ->
    [["riak-admin", "tictacaae", "rangeboost", '*'],
     '_', [?NODEOPT],
     fun(A, B, C) -> main(fun simple_envvar_cmd/3, A, B, C) end
    ].

simple_envvar_usage() ->
    ["Set/show env var VAR on NODE:\n\n",
     "  riak admin tictacaae VAR [-n NODE] [VALUE]\n\n",
     "VAR is one of rebuildtick, exchangetick, maxresults, rangeboost\n"
    ].

simple_envvar_cmd([_, _, Var | Args], _, Options) ->
    Nodes = extract_nodes(Options),
    case Args of
        [Arg1] ->
            case Var of
                "rebuildtick" ->
                    Msec = ensure_valid_range(Arg1, 0, 60*60*1000*1000),
                    set_tictacaae_envvar(tictacaae_rebuildtick, Nodes, Msec);
                "exchangetick" ->
                    MSec = ensure_valid_range(Arg1, 0, 60*60*1000*1000),
                    set_tictacaae_envvar(tictacaae_exchangetick, Nodes, MSec);
                "maxresults" ->
                    A = ensure_valid_range(Arg1, 1, 1000*1000),
                    set_tictacaae_envvar(tictacaae_maxresults, Nodes, A);
                "rangeboost" ->
                    A = ensure_valid_range(Arg1, 0, 60*60*1000*1000),
                    set_tictacaae_envvar(tictacaae_rangeboost, Nodes, A)
            end;
        [] ->
            case Var of
                "rebuildtick" ->
                    print_tictacaae_envvar(tictacaae_rebuildtick, Nodes);
                "exchangetick" ->
                    print_tictacaae_envvar(tictacaae_exchangetick, Nodes);
                "maxresults" ->
                    print_tictacaae_envvar(tictacaae_maxresults, Nodes);
                "rangeboost" ->
                    print_tictacaae_envvar(tictacaae_rangeboost, Nodes)
            end;
        _ ->
            clique_status:usage()
    end.

print_tictacaae_envvar(A, Nodes) ->
    [clique_status:table(
       [begin
            {ok, Current} = rpc:call(Node, application, get_env, [riak_kv, A]),
            [{node, Node}, {A, Current}]
        end || Node <- Nodes])].

set_tictacaae_envvar(A, Nodes, V) ->
    [ok = rpc:call(Node, application, set_env, [riak_kv, A, V])
     || Node <- Nodes],
    [].


rebuild_soon_specs() ->
    [["riak-admin", "tictacaae", "rebuild-soon", '*'],
     '_', [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun rebuild_soon_cmd/3, A, B, C) end
    ].

rebuild_soon_usage() ->
    ["Set next rebuild time to now + DELAY sec, on PARTITION on NODE (default is\n",
     "all partitions on local node):\n\n",
     "  riak admin tictacaae rebuild-soon DELAY [-n NODE] [-p PARTITION]\n"
    ].

rebuild_soon_cmd([_, _, _, Arg], _, Options) ->
    Nodes = extract_nodes(Options),
    Vnodes = extract_vnodes(Options, Nodes),
    AffectedVNodes = prompt_nextrebuild(
                       Vnodes, list_to_integer(Arg)),
    if length(Nodes) == 1 ->
            [clique_status_text(
               "scheduled rebuild of aae trees on ~b partition~s on ~s\n",
               [length(AffectedVNodes), ending(AffectedVNodes), hd(Nodes)])];
       el/=se ->
            [clique_status_text(
               "scheduled rebuild of aae trees on ~b nodes\n",
               [length(Nodes)])]
    end.


rebuild_now_specs() ->
    [["riak-admin", "tictacaae", "rebuild-now"],
     [], [?NODEOPT, ?PARTITIONOPT],
     fun(A, B, C) -> main(fun rebuild_now_cmd/3, A, B, C) end
    ].

rebuild_now_usage() ->
    ["Send a rebuild poke for PARTITION on NODE (default is all partitions\n",
     "on local node):\n\n",
     "  riak admin tictacaae rebuild-now [-n NODE] [-p PARTITION]\n"
    ].

rebuild_now_cmd([_, _, _], _, Options) ->
    Nodes = extract_nodes(Options),
    Vnodes = extract_vnodes(Options, Nodes),
    send_rebuildpoke(Vnodes),
    if length(Nodes) == 1 ->
            [clique_status_text(
               "rebuilding aae trees on ~b partition~s on ~s\n",
               [length(Vnodes), ending(Vnodes), hd(Nodes)])];
       el/=se ->
            [clique_status_text(
               "rebuilding aae trees on ~b nodes (~b partitions)\n",
               [length(Nodes), length(Vnodes)])]
    end.

post_set_fun(Res, Par, Val) ->
    case Res of
        [{ok, {P, N}}] ->
            [clique_status_text(
               "Set ~s to ~s on partition ~b on ~s\n", [Par, Val, P, N])];
        [{ok, N}] ->
            [clique_status_text(
               "Set ~s to ~s on ~s\n", [Par, Val, N])];
        Multiple ->
            case length([PN || {Resx, PN} <- Multiple, Resx == ok]) of
                AllSucceeded when AllSucceeded == length(Multiple) ->
                    [clique_status_text("Set ~s to ~s on ~b (v)nodes\n",
                                        [Par, Val, length(Multiple)])];
                SomeSucceeded when SomeSucceeded > 0 ->
                    [clique_status_text("Successfully set ~s to ~p on ~b (v)vnodes, but"
                                        " failed on ~b (v)nodes\n",
                                        [Par, Val, SomeSucceeded, length(Multiple) - SomeSucceeded])];
                _ ->
                    [clique_status_alert("Failed to set ~s to ~p on all ~b (v)nodes\n",
                                         [Par, Val, length(Multiple)])]
            end
    end.


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
extract_vnodes(Options, Nodes) ->
    PP = [P || {partition, P} <- Options],
    HaveAll = lists:member(all, PP) or (length(PP) == 0),
    HaveManyNodes = length(Nodes) > 1,
    case {HaveManyNodes, HaveAll} of
        {true, false} ->
            io:format("With more than a single node, only -p all is allowed\n", []),
            throw(inconsistent_options);
        {_, true} ->
            lists:flatten([[{P, N} || {P, _} <- vnodes(N, all)] || N <- Nodes]);
        {_, false} ->
            lists:flatten([[{P, N} || {P, _} <- vnodes(N, PP)] || N <- Nodes])
    end.

vnodes(Node, all) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    [VN || VN = {_, Owner} <- rpc:call(Node, riak_core_ring, all_owners, [Ring]), Owner =:= Node];
vnodes(Node, List) ->
    [{P, Node} || P <- List].


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

prompt_nextrebuild(Vns, Delay) ->
    exec_command_on_vnodes(Vns, {aae_prompt_nextrebuild, [Delay]}).
get_rebuild_schedule(Vns) ->
    exec_command_on_vnodes(Vns, {aae_get_rebuild_schedule, []}).
set_rebuild_schedule(Vns, RS) ->
    exec_command_on_vnodes(Vns, {aae_set_rebuild_schedule, [RS]}).
get_storeheads(Vns) ->
    exec_command_on_vnodes(Vns, {aae_get_storeheads, []}).
set_storeheads(Vns, A) ->
    exec_command_on_vnodes(Vns, {aae_set_storeheads, [A]}).
get_tokenbucket(Vns) ->
    exec_command_on_vnodes(Vns, {aae_get_tokenbucket, []}).
set_tokenbucket(Vns, A) ->
    exec_command_on_vnodes(Vns, {aae_set_tokenbucket, [A]}).
send_rebuildpoke(Vns) ->
    exec_command_on_vnodes(Vns, {aae_rebuildpoke, []}).

exec_command_on_vnodes(Vnodes, {F, A}) ->
    [{rpc:call(N, riak_kv_vnode, F, [VN | A]), VN} || {_, N} = VN <- Vnodes].

list_to_boolean("true") -> true;
list_to_boolean("enabled") -> true;
list_to_boolean("on") -> true;
list_to_boolean("false") -> false;
list_to_boolean("disabled") -> false;
list_to_boolean("off") -> false.


treestatus_specs() ->
    [["riak-admin", "tictacaae", "treestatus"],
    [], [{show, [{shortname, "s"}, {longname, "show"}, {typecast, fun to_show_state/1}]}],
     fun(A, B, C) -> main(fun treestatus_cmd/3, A, B, C) end
    ].

treestatus_usage() ->
    ["Generate the tree rebuild report:\n\n",
     "  riak admin tictacaae treestatus [--show STATES]\n\n",
     "STATES is a comma-separated list of ''empty', 'partial', 'built',\n",
     "'rebuilding', 'building', or 'all'. Default is\n",
     "'partial,rebuilding,building'.\n"
    ].

treestatus_cmd([_, _, _], _, Options) ->
    Report = get_aae_progress_report(),
    print_aae_progress_report(Report, Options).

get_aae_progress_report() ->
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
         AAEReport ++ Extra
     end || {Idx, VNState} <- VVSS].

print_aae_progress_report(Report, Options) ->
    Show_ =
        case proplists:get_all_values(show, Options) of
            [] -> ["partial","rebuilding","building"];
            Some ->
                case lists:member("all", Some) of
                    true ->
                        ["empty", "partial", "rebuilding", "building", "built"];
                    false ->
                        lists:append([string:split(S, ",", all) || S <- Some])
                end
        end,
    Show = [list_to_atom(A) || A <- Show_],
    Rows =
        lists:foldl(
          fun(M, Q) ->
                  Idx = proplists:get_value(partition, M),
                  LastRebuild = time2s(proplists:get_value(last_rebuild, M)),
                  NextRebuild = time2s(proplists:get_value(next_rebuild, M)),
                  ControllerPid = proplists:get_value(controller_pid, M),
                  Status = proplists:get_value(status, M),
                  case lists:member(Status, Show) of
                      true ->
                          [[{idx, Idx},
                            {status, Status},
                            {last_rebuild, LastRebuild},
                            {next_rebuild, NextRebuild},
                            {aae_cntr_pid, ControllerPid}] | Q];
                      false ->
                          Q
                  end
          end, [], Report),
    [clique_status:table(Rows)].

to_show_state(A) ->
    A.


-define(DEFAULT_AAEFOLD_OUTFILE, "aaefold-%o-results-%t.json").

fold_specs() ->
    [["riak-admin", "tictacaae", "fold", '*'],
     '_', [{output, [{shortname, "o"},
                     {longname, "outfile"},
                     {typecast, fun to_filename/1}]}],
     fun(A, B, C) -> main(fun fold_cmd/3, A, B, C) end
    ].

to_filename(A) ->
    A.

fold_usage() ->
    ["AAE fold operations, dumping results in JSON format to a file specified with '-o'.\n\n",
     "List buckets:\n\n",
     "  riak-admin tictacaae fold list-buckets\n",
     "                            nval=NVAL\n\n",
     "Find/count keys matching filters:\n\n",
     "  riak-admin tictacaae fold find-keys|count-keys\n",
     "                            bucket=BUCKET key_range=KEY_RANGE\n",
     "                            modified_range=MODIFIED_RANGE\n",
     "                            sibling_count=COUNT|object_size=BYTES\n\n",
     "where BUCKET is BUCKETNAME|TYPENAME/BUCKETNAME,\n",
     "KEY_RANGE is all|FROM,TO, MODIFIED_RANGE is all|FROM,TO (in RFC3339 format).\n\n",
     "Find/count tombstones in the range that match the criteria:\n\n",
     "  riak-admin tictacaae fold find-tombstones|count-tombstones\n",
     "                            key_range=KEY_RANGE segments=SEGMENTS\n",
     "                            modified_range=MODIFIED_RANGE\n\n",
     "where KEY_RANGE and MODIFIED_RANGE are as above, and SEGMENTS is\n",
     "all|S1,S2,...;TREE_SIZE and TREE_SIZE is xxsmall|xsmall|small|medium|large|xlarge.\n\n",
     "Reap tombstones in the range that match the criteria:\n\n",
     "  riak-admin tictacaae fold reap-tombstones\n",
     "                            key_range=KEY_RANGE segments=SEGMENTS\n",
     "                            modified_range=MODIFIED_RANGE change_method=CHANGE_METHOD\n\n",
     "where KEY_RANGE, MODIFIED_RANGE and SEGMENTS are as above and CHANGE_METHOD is\n",
     "jobs:N|local|count.\n\n",
     "Collect object stats in the specified ranges:\n\n",
     "  riak-admin tictacaae fold object-stats\n",
     "                            bucket=BUCKET key_range=KEY_RANGE\n",
     "                            modified_range=MODIFIED_RANGE\n\n",
     "Returns the following:\n",
     "  - the total count of objects in the key range;\n",
     "  - the accumulated total size of all objects in the range;\n",
     "  - a list [{Magnitude, ObjectCount}] tuples where Magnitude represents\n",
     "    the order of magnitude of the size of the object.\n\n",
     "Erase keys matching filters:\n\n",
     "  riak-admin tictacaae fold erase-keys\n",
     "                            bucket=BUCKET key_range=KEY_RANGE segments=SEGMENTS\n",
     "                            modified_range=MODIFIED_RANGE change_method=CHANGE_METHOD\n\n",
     "BUCKET, KEY_RANGE and MODIFIED_RANGE are as above.\n\n",
     "Repair keys matching filters:\n\n",
     "  riak-admin tictacaae fold repair-keys\n",
     "                            bucket=BUCKET key_range=KEY_RANGE\n",
     "                            modified_range=MODIFIED_RANGE\n",
     "BUCKET, KEY_RANGE and MODIFIED_RANGE are as above.\n"
    ].


fold_cmd([_, _, _ | Items], Keys, Options) ->
    DumpF =
        fun(Op, Fun) ->
                Outfile_ =
                    iolist_to_binary(
                      string:replace(
                        string:replace(
                          proplists:get_value(output, Options, ?DEFAULT_AAEFOLD_OUTFILE),
                          "%o", Op),
                        "%t", time2s(now))),
                Outfile =
                    case Outfile_ of
                        <<$/, _/binary>> ->
                            Outfile_;
                        _ ->
                            {ok, CWD} = file:get_cwd(),
                            filename:join([CWD, Outfile_])
                    end,
                spawn(
                  fun() ->
                          case file:open(Outfile, [write]) of
                              {ok, FD} ->
                                  io:format(
                                     "Results will be written to ~s\n", [Outfile]),
                                  try
                                      Fun(FD)
                                  catch _:_ ->
                                          ok
                                  end,
                                  file:close(FD);
                              {error, Reason} ->
                                  io:format(
                                     "Failed to open \"~p\" for writing: ~p\n", [Outfile, Reason])
                          end
                  end),
                []
        end,
    case Items of
        ["list-buckets"] ->
            DumpF(
              "list-buckets",
              fun(FD) ->
                      Query =
                          {list_buckets,
                           fold_query_arg(nval, Keys)
                          },
                      {ok, BB} = riak_client:aae_fold(Query),
                      Printable = [printable_bin(B) || B <- BB],
                      io:format(FD, "~s\n", [riak_kv_wm_json:encode(Printable)])
              end);

        ["find-keys"] ->
            DumpF(
              "find-keys",
              fun(FD) ->
                      Query =
                          {find_keys,
                           fold_query_arg(bucket, Keys),
                           fold_query_arg(key_range, Keys),
                           fold_query_arg(modified_range, Keys),
                           fold_query_arg(sibling_count_or_object_size, Keys)
                          },
                      {ok, KK} = riak_client:aae_fold(Query),
                      Printable = [#{<<"key">> => printable_bin(K),
                                     <<"sibling_count">> => SibCnt
                                    } || {_B, K, SibCnt} <- KK],
                      io:format(FD, "~s\n", [riak_kv_wm_json:encode(Printable)])
              end);

        ["count-keys"] ->
            DumpF(
              "count-keys",
              fun(FD) ->
                      Query =
                          {find_keys,
                           fold_query_arg(bucket, Keys),
                           fold_query_arg(key_range, Keys),
                           fold_query_arg(modified_range, Keys),
                           fold_query_arg(sibling_count_or_object_size, Keys)
                          },
                      {ok, KK} = riak_client:aae_fold(Query),
                      io:format(FD, "~b\n", [length(KK)])
              end);

        ["find-tombstones"] ->
            DumpF(
              "find-tombstones",
              fun(FD) ->
                      Query =
                          {find_tombs,
                           fold_query_arg(bucket, Keys),
                           fold_query_arg(key_range, Keys),
                           fold_query_arg(segments, Keys),
                           fold_query_arg(modified_range, Keys)
                          },
                      {ok, TT} = riak_client:aae_fold(Query),
                      Printable = [#{bucket => printable_bin(B),
                                     key => printable_bin(K),
                                     vclock => printable_vclock(VC)} || {B, K, VC} <- TT],
                      io:format(FD, "~s\n", [riak_kv_wm_json:encode(Printable)])
              end);

        ["count-tombstones"] ->
            DumpF(
              "count-tombstones",
              fun(FD) ->
                      Query =
                          {find_tombs,
                           fold_query_arg(bucket, Keys),
                           fold_query_arg(key_range, Keys),
                           fold_query_arg(segments, Keys),
                           fold_query_arg(modified_range, Keys)
                          },
                      {ok, TT} = riak_client:aae_fold(Query),
                      io:format(FD, "~b\n", [length(TT)])
              end);

        ["reap-tombstones"] ->
            DumpF(
              "reap-tombstones",
              fun(FD) ->
                      Query =
                          {reap_tombs,
                           fold_query_arg(bucket, Keys),
                           fold_query_arg(key_range, Keys),
                           fold_query_arg(segments, Keys),
                           fold_query_arg(modified_range, Keys),
                           fold_query_arg(change_method, Keys)
                          },
                      {ok, TT} = riak_client:aae_fold(Query),
                      io:format(FD, "~b\n", [TT])
              end);

        ["object-stats"] ->
            DumpF(
              "object-stats",
              fun(FD) ->
                      Query =
                          {object_stats,
                           fold_query_arg(bucket, Keys),
                           fold_query_arg(key_range, Keys),
                           fold_query_arg(modified_range, Keys)
                          },
                      {ok, SS} = riak_client:aae_fold(Query),
                      TC = proplists:get_value(total_count, SS),
                      TS = proplists:get_value(total_size, SS),
                      Sizes = proplists:get_value(sizes, SS),
                      Siblings = proplists:get_value(siblings, SS),
                      io:format(FD, "~s\n", [riak_kv_wm_json:encode(
                                               #{total_count => TC,
                                                 total_size => TS,
                                                 sizes => [#{min => Min,
                                                             max => Max} || {Min, Max} <- Sizes],
                                                 siblings => [#{min => Min,
                                                                max => Max} || {Min, Max} <- Siblings]
                                                })])
              end);

        ["erase-keys"] ->
            DumpF(
              "erase-keys",
              fun(FD) ->
                      Query =
                          {erase_keys,
                           fold_query_arg(bucket, Keys),
                           fold_query_arg(key_range, Keys),
                           fold_query_arg(segments, Keys),
                           fold_query_arg(modified_range, Keys),
                           fold_query_arg(change_method, Keys)
                          },
                      {ok, Res} = riak_client:aae_fold(Query),
                      io:format(FD, "~b\n", [Res])
              end);

        ["repair-keys"] ->
            DumpF(
              "erase-keys",
              fun(FD) ->
                      Query =
                          {repair_keys_range,
                           fold_query_arg(bucket, Keys),
                           fold_query_arg(key_range, Keys),
                           fold_query_arg(modified_range, Keys),
                           all
                          },
                      {ok, {_Tail, Count, all, _RBS}} = riak_client:aae_fold(Query),
                      io:format(FD, "~b\n", [Count])
              end);

        _ ->
            clique_status:usage()
    end.

fold_query_arg(nval, Keys) ->
    try
        ensure_valid_range(proplists:get_value("nval", Keys), 1, 999)
    catch _:_ ->
      io:format("Invalid/missing 'nval' parameter\n", []),
      throw(arg_bad_or_missing)
    end;
fold_query_arg(bucket, Keys) ->
    case proplists:get_value("bucket", Keys) of
        A when is_list(A) ->
            case string:split(A, "/") of
                [BT, B] ->
                    case lists:last(BT) of
                        $\\ ->
                            bin_from_maybe_hex(A);
                        _ ->
                            {bin_from_maybe_hex(BT), bin_from_maybe_hex(B)}
                    end;
                _ ->
                    bin_from_maybe_hex(A)
            end;
        undefined ->
            io:format("Missing 'bucket' parameter\n", []),
            throw(arg_bad_or_missing)
    end;
fold_query_arg(key_range, Keys) ->
    case proplists:get_value("key_range", Keys) of
        "all" -> all;
        A when is_list(A) ->
            [From, To] = string:split(A, ","),
            {bin_from_maybe_hex(From), bin_from_maybe_hex(To)}
    end;
fold_query_arg(modified_range, Keys) ->
    try
        case proplists:get_value("modified_range", Keys) of
            "all" -> all;
            A when is_list(A) ->
                [From, To] = string:split(A, ","),
                {date, calendar:rfc3339_to_system_time(From),
                 calendar:rfc3339_to_system_time(To)}
        end
    catch _:_ ->
            io:format("Missing/malformed 'modified_range' parameter\n", []),
            throw(arg_bad_or_missing)
    end;
fold_query_arg(segments, Keys) ->
    try
        case proplists:get_value("segments", Keys) of
            "all" -> all;
            A when is_list(A) ->
                [SegmentFilter_, TreeSize_] = string:split(A, ";"),
                SegmentFilter = [ensure_valid_range(S, 0, infinity)
                                 || S <- string:split(SegmentFilter_, ",", all)],
                TreeSize = tree_size(TreeSize_),
                {segments, SegmentFilter, TreeSize}
        end
    catch _:_ ->
            io:format("Missing/malformed 'segments' parameter\n", []),
            throw(arg_bad_or_missing)
    end;
fold_query_arg(sibling_count_or_object_size, Keys) ->
    try
        case {proplists:get_value("sibling_count", Keys),
              proplists:get_value("object_size", Keys)} of
            {A, undefined} when is_list(A) ->
                {sibling_count, ensure_valid_range(A, 0, infinity)};
            {undefined, A} when is_list(A) ->
                {object_size, ensure_valid_range(A, 0, infinity)}
        end
    catch _:_ ->
            io:format("Missing/malformed 'sibling_count' or 'object_size' parameter\n", []),
            throw(arg_bad_or_missing)
    end;
fold_query_arg(change_method, Keys) ->
    try
        case proplists:get_value("change_method", Keys) of
            "jobs:" ++ V ->
                {jobs, ensure_valid_range(V, 1, infinity)};
            "local" ->
                local;
            "count" ->
                count
        end
    catch _:_ ->
            io:format("Missing/malformed 'change_method' parameter\n", []),
            throw(arg_bad_or_missing)
    end.

ensure_valid_range(V_, Min, infinity) ->
    case list_to_integer(V_) of
        V when V >= Min ->
            V;
        _ ->
            throw(arg_out_of_range)
    end;
ensure_valid_range(V_, Min, Max) ->
    case list_to_integer(V_) of
        V when V >= Min, V =< Max ->
            V;
        _ ->
            throw(arg_out_of_range)
    end.


printable_bin(K) ->
    case re:run(K, <<"[[:alnum:][:punct:]]+">>) of
        {match, [{0, N}]} when N == size(K) ->
            K;
        _ ->
            iolist_to_binary(["0x", mochihex:to_hex(K)])
    end.
bin_from_maybe_hex("0x" ++ A) -> mochihex:to_bin(A);
bin_from_maybe_hex(A) -> list_to_binary(A).

printable_vclock(A) ->
    base64:encode(riak_object:encode_vclock(A)).

tree_size("xxsmall") -> xxsmall;
tree_size("xsmall") -> xsmall;
tree_size("small") -> small;
tree_size("medium") -> medium;
tree_size("large") -> large;
tree_size("xlarge") -> xlarge.

time2s(never) ->
    never;
time2s(now) ->
    time2s(calendar:local_time());
time2s({_, _, _} = A) ->
    time2s(calendar:now_to_local_time(A));
time2s({{LRY, LRMo, LRD}, {LRH, LRMi, LRS}}) ->
    iolist_to_binary(
      io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B",
                    [LRY, LRMo, LRD, LRH, LRMi, LRS])).

ending([_]) -> "";
ending(_) -> "s".

clique_status_text(F, A) ->
    clique_status:text(io_lib:format(F, A)).
clique_status_alert(S) ->
    clique_status_alert(S, []).
clique_status_alert(F, A) ->
    clique_status:alert([clique_status_text(F, A)]).
