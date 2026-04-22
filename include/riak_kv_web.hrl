%% -------------------------------------------------------------------
%%
%% Copyright (c) 2026 Martin Sumner
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
%% @doc common definitions for web handlers

-define(TXT_HEADER, {'Content-Type', <<"text/plain">>}).
-define(JSN_HEADER, {'Content-Type', <<"application/json">>}).
-define(BIN_HEADER, {'Content-Type', <<"application/octet-stream">>}).

-define(HEAD_VCLOCK, <<"X-Riak-Vclock">>).
-define(HEAD_USERMETA_PREFIX, <<"X-Riak-Meta-">>).
-define(HEAD_INDEX_PREFIX, <<"X-Riak-Index-">>).
-define(HEAD_DELETED, <<"X-Riak-Deleted">>).
-define(HEAD_CONTINUATION, <<"X-Riak-Continuation">>).

%% Case-folded headers to be used in lookups
-define(HEAD_VCLOCK_CASEFOLD, <<"x-riak-vclock">>).
-define(HEAD_IFNOTMOD_CASEFOLD, <<"x-riak-if-not-modified">>).
-define(HEAD_USERMETA_CASEFOLD, <<"x-riak-meta-">>).
-define(HEAD_INDEX_CASEFOLD, <<"x-riak-index-">>).

-define(Q_2I_CONTINUATION_BIN, <<"continuation">>).
-define(Q_RESULTS_BIN, <<"results">>).
-define(Q_KEYS_BIN, <<"keys">>).

%% erlfmt:ignore-begin
-type stream_fun() ::
    fun(() ->
            {binary(), stream_fun()}
            | done
            | error
    ).
%% erlfmt:ignore-end
