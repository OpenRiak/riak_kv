%% -------------------------------------------------------------------
%%
%% riak_kv_query_sup: supervise the riak_kv query servers.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc supervise the riak_kv query server filebuffers used to
%% process queues
%% 
%% A supervisor is used for the filebuffer, as it external messages will be
%% received targeting the specific pid().  If the process has expired, or
%% the request has been doctored - there is a risk that a random pid() will
%% receive a message it cannot handle (and crash).  So we must always check
%% the pid is the right type - by confirming it was started by this supervisor.

-module(riak_kv_query_filebuffer_sup).

-behaviour(supervisor).

-export([start_query_filebuffer/2]).
-export([start_link/0]).
-export([init/1]).

start_query_filebuffer(Node, Args) ->
    case supervisor:start_child({?MODULE, Node}, Args) of
        {ok, Pid, ReqID} ->
            {ok, Pid, ReqID};
        Error ->
            Error
    end.


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    QueryChildSpec =
        {
            undefined,
            {riak_kv_query_filebuffer, new, []},
            temporary,
            5000,
            worker,
            [riak_kv_query_server]
        },

    {ok, {{simple_one_for_one, 10, 10}, [QueryChildSpec]}}.
