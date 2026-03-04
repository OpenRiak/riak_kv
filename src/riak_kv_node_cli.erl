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
    clique:register_usage(["riak-admin", "node", "repair"], node_repair_usage()).

register_all_commands() ->
    lists:foreach(
      fun(Args) -> apply(clique, register_command, Args) end,
      [node_repair_specs()
      ]).

node_usage() ->
    ["riak admin node { repair }\n",
     "See individual subcommand usage for options and arguments.\n",
     "\n",
     "Unless given specifically with -n NODE, commands are executed on the current node.\n",
     "NODE can be \"all\".\n"
    ].

node_repair_usage() ->
    ["riak admin node repair\n",
     "Triggers a partition repair on all partitions on node(s).\n",
     "\n",
     "Unless given specifically with -n NODE, commands are executed on the current node.\n",
     "NODE can be \"all\".\n"
    ].

main(Fun, A, B, C) ->
    try
        Fun(A, B, C)
    catch
        Class:Reason:Stack ->
            logger:error("node repair: handler failed: ~p:~p stack=~p",
                         [Class, Reason, Stack]),
            clique_status:text(io_lib:format("ERROR: ~p:~p", [Class, Reason]))
    end.

-define(NODEOPT, {node, [{shortname, "n"},
                         {longname, "node"},
                         {typecast, fun to_node/1}]}).

target_nodes(Opts) ->
    NN = [N || {node, N} <- Opts],
    case lists:member(all, NN) of
        true ->
            Ns = [node() | nodes()],
            Ns;
        false when NN /= [] ->
            NN;
        _ ->
            Ns = [node()],
            Ns
    end.

node_repair_specs() ->
    [["riak-admin", "node", "repair"],
     [], [?NODEOPT],
     fun(A, B, C) -> main(fun node_repair_cmd/3, A, B, C) end
    ].

node_repair_cmd(_Cmd, _Args, Opts) ->
    Nodes = target_nodes(Opts),

    [{text,
      lists:flatten(
        io_lib:format("~p -> ~p",
                      [N, rpc:call(N, riak_client, repair_node, [])]))}
     || N <- Nodes].

to_node("all") ->
    all;
to_node(A) ->
    clique_typecast:to_node(A).

