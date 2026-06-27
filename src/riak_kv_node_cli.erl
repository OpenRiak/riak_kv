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

-module(riak_kv_node_cli).

-behaviour(clique_handler).

-include_lib("kernel/include/logger.hrl").

-export([register_cli/0]).

register_cli() ->
    register_all_usage(),
    register_all_commands().

register_all_usage() ->
    clique:register_usage(["riak-admin", "node"], node_usage()),
    clique:register_usage(["riak-admin", "node", "repair"], node_repair_usage()),
    clique:register_usage(["riak-admin", "node", "repair", "start"], node_repair_start_usage()),
    clique:register_usage(["riak-admin", "node", "repair", "status"], node_repair_status_usage()),
    clique:register_usage(["riak-admin", "node", "repair", "stop", '*'], node_repair_stop_usage()).

register_all_commands() ->
    lists:foreach(
      fun(Args) -> apply(clique, register_command, Args) end,
      [node_repair_status_specs(),
       node_repair_start_specs(),
       node_repair_stop_specs()
      ]).

node_usage() ->
    ["riak admin node repair { start | status | stop REASON } [OPTIONS]\n",
     "See individual subcommand usage for options and arguments.\n"
    ].
node_repair_usage() ->
    node_usage().

node_repair_status_usage() ->
    ["riak admin node repair status [-n|--node NODE|all] [-f|--format table|json]\n",
     "Print status of any ongoing partition repairs on NODE.\n"
    ].

node_repair_start_usage() ->
    ["riak admin node repair start [-n|--node NODE]\n",
     "Start partition repair on NODE.\n"
    ].

node_repair_stop_usage() ->
    ["riak admin node repair stop [-n|--node NODE|all] REASON\n",
     "Kill all ongoing partition repair on NODE, with REASON.\n"
    ].

-define(NODEOPT, {node, [{shortname, "n"},
                         {longname, "node"},
                         {typecast, fun to_node/1}]}).
-define(FMTOPTION, {format, [{shortname, "f"},
                             {longname, "format"},
                             {typecast, fun to_fmt/1}]}).

node_repair_status_specs() ->
    [["riak-admin", "node", "repair", "status"],
     [], [?NODEOPT, ?FMTOPTION],
     fun(A, B, C) -> main(fun node_repair_status_cmd/3, A, B, C) end
    ].
node_repair_start_specs() ->
    [["riak-admin", "node", "repair", "start"],
     [], [?NODEOPT],
     fun(A, B, C) -> main(fun node_repair_start_cmd/3, A, B, C) end
    ].
node_repair_stop_specs() ->
    [["riak-admin", "node", "repair", "stop", '*'],
     [], [?NODEOPT],
     fun(A, B, C) -> main(fun node_repair_stop_cmd/3, A, B, C) end
    ].


main(Fun, A, B, C) ->
    try
        Fun(A, B, C)
    catch
        Class:Reason:Stack ->
            logger:error("node repair: handler failed: ~p:~p stack=~p",
                         [Class, Reason, Stack]),
            [alert("Error: ~p:~p", [Class, Reason])]
    end.

node_repair_status_cmd(_Cmd, _Args, Opts) ->
    Nodes =
        case [A || {node, A} <- Opts] of
            [all] ->
                get_nodes();
            [] ->
                [node()];
            NN ->
                NN
        end,
    Fmt = extract_fmt_option(Opts),

    Res = get_node_repair_status(Nodes),

    case Fmt of
        table ->
            Table =
                [begin
                     Rows =
                         [[{mod, Mod}, {idx, integer_to_binary(Idx)}]
                          || {Mod, Idx} <- NRes],
                     case Rows of
                         [] ->
                             text("No active node repairs on ~s\n", [Node]);
                         _ ->
                             [text("Vnode repairs triggered by node repair on ~s", [Node]), table(Rows)]
                     end
                 end || {Node, NRes} <- Res],
            lists:flatten(Table);
        json ->
            [text("~s", [riak_kv_wm_json:encode(
                           [#{node => Node,
                              status => [jsonify_status(S) || S <- Statuses]}
                            || {Node, Statuses} <- Res])])]
    end.

get_node_repair_status(Nodes) ->
    [begin
         Vnodes = erpc:call(Node, riak_core_vnode_manager, all_vnodes, []),
         Statuses = [{Mod, Idx,
                      erpc:call(Node, riak_core_vnode_manager, repair_status, [{Mod, Idx}])}
                     || {Mod, Idx, _Pid} <- Vnodes],
         {Node, [{Mod, Idx} || {Mod, Idx, Status} <- Statuses, Status /= not_found]}
     end || Node <- Nodes].

nodes_running_repair() ->
    AllSS = get_node_repair_status(get_nodes()),
    [N || {N, SS} <- AllSS, SS /= []].

jsonify_status({Mod, Idx}) ->
    #{mod => Mod,
      idx => Idx}.

node_repair_start_cmd(_Cmd, _Args, Opts) ->
    Node =
        case [A || {node, A} <- Opts] of
            [all] -> invalid;
            [] -> node();
            [N] -> N;
            _ -> invalid
        end,
    case Node of
        invalid ->
            [alert("Error: node repair can be started on one node at a time")];
        Node ->
            case nodes_running_repair() of
                [] ->
                    case erpc:call(Node, riak_client, repair_node, []) of
                        ok ->
                            [text("Node repair started on ~s.", [Node])];
                        {error, BadRpcReason} ->
                            [alert("Error: failed to start node repair on ~s: ~p", [Node, BadRpcReason])]
                    end;
                NwAA ->
                    [alert("There are repairs currently ongoing on node~s ~s.\n"
                           "Wait until these are completed before starting a new node repair.",
                           [ending(NwAA), string:join([atom_to_list(N) || N <- NwAA], ",")])]
            end
    end.

node_repair_stop_cmd([_, _, _, _, Reason], _, Opts) ->
    Nodes =
        case [A || {node, A} <- Opts] of
            [all] ->
                get_nodes();
            [] ->
                [node()];
            NN ->
                NN
        end,
    AllNodesWithRepairs = nodes_running_repair(),
    Items =
        [begin
             case lists:member(Node, AllNodesWithRepairs) of
                 false ->
                     io_lib:format("\n* ~s: No active repairs", [Node]);
                 true ->
                     case erpc:call(Node, riak_core_vnode_manager, kill_repairs, [Reason]) of
                         ok ->
                             io_lib:format("\n* ~s: Node repair stopped", [Node]);
                         {error, BadRpcReason} ->
                             io_lib:format("\n* ~s: Failed to stop node repair on: ~p", [Node, BadRpcReason])
                     end
             end
         end || Node <- Nodes],
    [clique_status:list("Stopping repairs", Items)].

get_nodes() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Members = riak_core_ring:all_member_status(Ring),
    [N || {N, Valid} <- Members, is_really(Valid)].
is_really(A) when A == joining;
                  A == valid;
                  A == leaving -> true;
is_really(_) -> false.


extract_fmt_option(Opts) ->
    case [A || {format, A} <- Opts] of
        [] -> table;
        ["table"] -> table;
        ["json"] -> json;
        _ -> invalid
    end.

to_node("all") ->
    all;
to_node(A) ->
    clique_typecast:to_node(A).

to_fmt(A) ->
    A.

text(F, A) ->
    clique_status:text(lists:flatten(io_lib:format(F, A))).
alert(S) ->
    alert(S, []).
alert(F, A) ->
    clique_status:alert([text(F, A)]).
table(A) ->
    clique_status:table(A).

ending([_]) -> "";
ending(_) -> "s".
