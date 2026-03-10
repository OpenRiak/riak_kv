%% -------------------------------------------------------------------
%%
%% Copyright (c) 2025, 2026 TI Tokyo.  All Rights Reserved.
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

-module(riak_kv_cli_registry).

-define(CLI_MODULES, [riak_kv_tictacaae_cli,
                      riak_kv_vnode_status_cli
                     ]).

-export([register_cli/0
        ]).

-spec register_cli() -> ok.
register_cli() ->
    clique:register(?CLI_MODULES).

%% schemas loaded in riak_core
