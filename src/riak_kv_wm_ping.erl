%% -------------------------------------------------------------------
%%
%% Copyright (c) 2010-2013 Basho Technologies, Inc.
%% Copyright (c) 2018 Workday, Inc.
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

%% @doc simple Webmachine resource for availability test

-module(riak_kv_wm_ping).

%% webmachine resource exports
-export([
         init/1,
         is_authorized/2,
         to_html/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {
    connection_id = riak_kv_wm_utils:make_connection_id()
}).

init([]) ->
    {ok, #ctx{}}.

is_authorized(ReqData, Ctx) ->
    case riak_kv_wm_utils:is_authorized(ReqData, Ctx#ctx.connection_id) of
        false ->
            {"Basic realm=\"Riak\"", ReqData, Ctx};
        {true, _SecContext} ->
            {true, ReqData, Ctx};
        insecure ->
            %% XXX 301 may be more appropriate here, but since the http and
            %% https port are different and configurable, it is hard to figure
            %% out the redirect URL to serve.
            {{halt, 426}, wrq:append_to_resp_body(<<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>, ReqData), Ctx}
    end.

to_html(ReqData, Ctx) ->
    {"OK", ReqData, Ctx}.
