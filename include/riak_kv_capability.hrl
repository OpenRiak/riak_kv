%% -------------------------------------------------------------------
%%
%% riak_kv_capability: fixing the setting of previously variable capabilities
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-define(CAP_VNODE_VCLOCKS, riak_core_capability:get({riak_kv, vnode_vclocks})).
-define(CAP_RPC_VNODE_VCLOCKS(Node), 
    case rpc:call(Node, riak_core_capability, get, [{riak_kv, vnode_vclocks}]) of
        {badrpc, {'EXIT', {undef, _}}} ->
            rpc:call(Node, app_helper, get_env, [riak_kv, vnode_vclocks, false]);
        Result ->
            Result
    end
).

-define(CAP_CRDT_EPOCH,
    riak_core_capability:get({riak_kv, crdt_epoch_versions}, ?E1_DATATYPE_VERSIONS)
).
-define(CAP_CRDT_TYPES,
    riak_core_capability:get({riak_kv, crdt}, [])
).

% -define(CAP_2I_VERSION,
%     riak_core_capability:get({riak_kv, secondary_index_version}, v1)
% ).
-define(CAP_2I_VERSION, v3).
-define(CAP_KEYS_BACKPRESSURE,
    riak_core_capability:get({riak_kv, listkeys_backpressure}, false)
).
% -define(CAP_INDEX_BACKPRESSURE,
%     riak_core_capability:get({riak_kv, index_backpressure}, false)
% ).
-define(CAP_INDEX_BACKPRESSURE, true).

% -define(CAP_OBJECT_FORMAT,
%     riak_core_capability:get({riak_kv, object_format}, v0)
% ).
-define(CAP_OBJECT_FORMAT, app_helper:get_env(riak_kv, object_format, v1)).

% -define(CAP_VCLOCK_ENCODING,
%     riak_core_capability:get({riak_kv, vclock_data_encoding}, encode_zlib)
% ).
-define(CAP_VCLOCK_ENCODING, encode_raw).
% -define(CAP_HANDOFF_DATA_ENCODING,
%     riak_core_capability:get({riak_kv, handoff_data_encoding}, encode_zlib)
% ).
-define(CAP_HANDOFF_DATA_ENCODING, encode_raw).
% -define(CAP_OBJECT_HASH_VERSION,
%     riak_core_capability:get({riak_kv, object_hash_version}, legacy)
% ).
-define(CAP_OBJECT_HASH_VERSION, 0).
% -define(CAP_GETREQUEST_TYPE,
%     riak_core_capability:get({riak_kv, get_request_type}, head)
% ).
-define(CAP_GETREQUEST_TYPE, head).
-define(CAP_MAPRED_2I_PIPE,
    riak_core_capability:get({riak_kv, mapred_2i_pipe}, false)
).
% -define(CAP_PUTFSM_ACK,
%     riak_core_capability:get({riak_kv, put_fsm_ack_execute}, disabled)
% ).
-define(CAP_PUTFSM_ACK, enabled).
-define(CAP_PUTFSM_SOFTLIMIT,
    riak_core_capability:get({riak_kv, put_soft_limit}, false)
).

-define(CAP_LEGACY_AAE,
    riak_core_capability:get({riak_kv, anti_entropy}, disabled)
).

-define(CAP_TICTACAAE_REPAIRS,
    riak_core_capability:get({riak_kv, tictacaae_prompted_repairs}, false)
).
