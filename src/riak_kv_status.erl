%% -------------------------------------------------------------------
%%
%% Riak: A lightweight, decentralized key-value store.
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_status).

-export([statistics/0,
         get_stats/1,
         ringready/0,
         transfers/0,
         vnode_status/0,
         fixed_index_status/0]).

-include("riak_kv_vnode.hrl").

%% ===================================================================
%% Public API
%% ===================================================================

-spec(statistics() -> [any()]).
statistics() ->
    get_stats(console).

ringready() ->
    riak_core_status:ringready().

transfers() ->
    riak_core_status:transfers().

%% @doc Get status information about the node local vnodes.
-spec vnode_status() -> [{atom(), term()}].
vnode_status() ->
    %% Get the kv vnode indexes and the associated pids for the node.
    PrefLists = riak_core_vnode_manager:all_index_pid(riak_kv_vnode),
    riak_kv_vnode:vnode_status(PrefLists).

%% @doc Get status of 2i reformat. If the backend requires reformatting, a boolean
%%      value is returned indicating if all partitions on the node have completed
%%      upgrading (if downgrading then false indicates all partitions have been downgraded.
%%      If the backend does not require reformatting, undefined is returned
-spec fixed_index_status() -> boolean() | undefined.
fixed_index_status() ->
    Backend = app_helper:get_env(riak_kv, storage_backend),
    fixed_index_status(Backend).

fixed_index_status(Affected) when Affected =:= riak_kv_eleveldb_backend orelse
                                  Affected =:= riak_kv_multi_backend ->
    Statuses = vnode_status(),
    fixed_index_status(Affected, Statuses);
fixed_index_status(_) ->
    undefined.

fixed_index_status(Affected, Statuses) ->
    lists:foldl(fun(Elem, Acc) -> Acc andalso are_indexes_fixed(Affected, Elem) end,
                true, Statuses).

are_indexes_fixed(riak_kv_eleveldb_backend, {_Idx, [{backend_status,_,Status}]}) ->
    are_indexes_fixed(riak_kv_eleveldb_backend, Status);
are_indexes_fixed(riak_kv_eleveldb_backend, Status) ->
    case proplists:get_value(fixed_indexes, Status) of
        Bool when is_boolean(Bool) -> Bool;
        _ -> false
    end;
are_indexes_fixed(riak_kv_multi_backend, {_Idx, [{backend_status,_,Status}]}) ->
    Statuses = [S || {_, S} <- Status, lists:member({mod, riak_kv_eleveldb_backend}, Status)],
    fixed_index_status(riak_kv_eleveldb_backend, Statuses).

get_stats(web) ->
    aliases()
        ++ expand_disk_stats(riak_kv_stat_bc:disk_stats())
        ++ riak_kv_stat_bc:app_stats();
get_stats(console) ->
    aliases()
        ++ riak_kv_stat_bc:disk_stats()
        ++ riak_kv_stat_bc:app_stats().


aliases() ->
    AllStats =
        exometer_alias:prefix_foldl(
            <<>>,
            fun(Alias, Entry, DP, Acc) -> [{Entry, {DP, Alias}}|Acc] end,
            []
        ),
        %% It is assumed that prefix_foldl/3 will return an ordered list of
        %% entries - and that ordering will be used for efficiency below.
        %% Note if unordered, stats will still be produced, but requiring more
        %% calls to exometer:get_value/2.
    case AllStats of
        [] ->
            [];
        AllStats when is_list(AllStats) ->
            {{FinalEntry, FinalDPMap}, AliasVals} =
                lists:foldl(
                    fun({Entry, {DP, Alias}}, {{PrevEntry, DPmap}, Acc}) ->
                        case Entry of
                            Entry when Entry == PrevEntry ->
                                {{PrevEntry, maps:put(DP, Alias, DPmap)}, Acc};
                            Entry when PrevEntry == none ->
                                {{Entry, maps:put(DP, Alias, DPmap)}, Acc};
                            Entry ->
                                UpdAcc = get_exometer_values(PrevEntry, DPmap),
                                {{Entry, #{DP => Alias}}, UpdAcc ++ Acc}
                        end
                    end,
                    {{none, #{}}, []},
                    AllStats
                ),
            lists:keysort(
                1, 
                get_exometer_values(FinalEntry, FinalDPMap) ++ AliasVals
            )
    end.

get_exometer_values(Entry, DPmap) ->
    case exometer:get_value(Entry, maps:keys(DPmap)) of
        {ok, Vs} when is_list(Vs) ->
            lists:map(
                fun({D, V}) ->
                    {maps:get(D, DPmap), V}
                end,
                Vs
            );
        Other ->
            DefaultValue =
                case Other of
                    {ok, disabled} -> disabled;
                    _ -> 0
                end,
            lists:map(
                fun(A) -> {A, DefaultValue} end,
                maps:values(DPmap)
            )
    end.

expand_disk_stats([{disk, Stats}]) ->
    [    
        {
            disk,
            lists:map(
                fun({Id, Size, Used}) ->
                    #{
                        id => list_to_binary(Id),
                        size => Size,
                        used => Used
                    }
                end,
                Stats
            )
        }
    ].
