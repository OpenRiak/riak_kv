%% -------------------------------------------------------------------
%%
%% riak_kv_wm_stats: publishing Riak runtime stats via HTTP
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

-module(riak_kv_wm_stats).

%% webmachine resource exports
-export([
         init/1,
         encodings_provided/2,
         content_types_provided/2,
         service_available/2,
         forbidden/2,
         malformed_request/2,
         produce_body/2,
         pretty_print/2
        ]).

-define(TIMEOUT, 30000). %% In milliseconds

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

-record(ctx, {timeout = ?TIMEOUT :: non_neg_integer()}).

init(_) ->
    {ok, #ctx{}}.

%% @spec encodings_provided(webmachine:wrq(), context()) ->
%%         {[encoding()], webmachine:wrq(), context()}
%% @doc Get the list of encodings this resource provides.
%%      "identity" is provided for all methods, and "gzip" is
%%      provided for GET as well
encodings_provided(ReqData, Context) ->
    case wrq:method(ReqData) of
        'GET' ->
            {[{"identity", fun(X) -> X end},
              {"gzip", fun(X) -> zlib:gzip(X) end}], ReqData, Context};
        _ ->
            {[{"identity", fun(X) -> X end}], ReqData, Context}
    end.

%% @spec content_types_provided(webmachine:wrq(), context()) ->
%%          {[ctype()], webmachine:wrq(), context()}
%% @doc Get the list of content types this resource provides.
%%      "application/json" and "text/plain" are both provided
%%      for all requests.  "text/plain" is a "pretty-printed"
%%      version of the "application/json" content.
content_types_provided(ReqData, Context) ->
    {[{"application/json", produce_body},
      {"text/plain", pretty_print}],
     ReqData, Context}.

service_available(ReqData, Ctx) ->
    {true, ReqData, Ctx}.

malformed_request(RD, Ctx) ->
    case wrq:get_qs_value("timeout", RD) of
        undefined ->
            {false, RD, Ctx};
        TimeoutStr ->
            try
                case list_to_integer(TimeoutStr) of
                    Timeout when Timeout > 0 ->
                        {false, RD, Ctx#ctx{timeout=Timeout}}
                end
            catch
                _:_ ->
                    {true,
                        wrq:append_to_resp_body(
                            io_lib:format(
                                "Bad timeout value ~0p "
                                "expected milliseconds > 0",
                                [TimeoutStr]
                            ),
                        wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
                        Ctx}
            end
    end.

forbidden(RD, Ctx) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx}.

produce_body(RD, Ctx) ->
    try 
        Stats = riak_kv_http_cache:get_stats(Ctx#ctx.timeout),
        Body = mochijson2:encode({struct, Stats}),
        {Body, RD, Ctx}
    catch
        exit:{timeout, _} ->
            {
                {halt, 503},
                wrq:set_resp_header(
                    ?HEAD_CTYPE,
                    "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format(
                            "Request timed out after ~w ms",
                            [Ctx#ctx.timeout]
                        ),
                        RD
                    )
                ),
                Ctx
            }
    end.

%% @spec pretty_print(webmachine:wrq(), context()) ->
%%          {string(), webmachine:wrq(), context()}
%% @doc Format the respons JSON object is a "pretty-printed" style.
pretty_print(RD, Ctx) ->
    case produce_body(RD, Ctx) of
        {{halt, RepsonseCode}, UpdRD, UpdCtx} ->
            {{halt, RepsonseCode}, UpdRD, UpdCtx};
        {Json, UpdRD, UpdCtx} ->
            {
                json_pp:print(binary_to_list(list_to_binary(Json))),
                UpdRD,
                UpdCtx
            }
    end.

