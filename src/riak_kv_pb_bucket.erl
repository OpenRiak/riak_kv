%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2016 Basho Technologies, Inc.
%% Copyright (c) 2025 Workday, Inc.
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

%% @doc <p>The Bucket PB service for Riak KV. This covers the
%% following request messages in the original protocol:</p>
%%
%% <pre>
%% 15 - RpbListBucketsReq
%% 17 - RpbListKeysReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%% 16 - RpbListBucketsResp
%% 18 - RpbListKeysResp{1,}
%% </pre>
%%
%% <p>The semantics are unchanged from their original
%% implementations.</p>
%% @end

-module(riak_kv_pb_bucket).

-type optional(A) :: A | undefined.

-include_lib("kernel/include/logger.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process/3,
         process_stream/3,
         process_stream/4,
         handle_metrics/2,
         bucket_type/2,
         maybe_create_bucket_type/2]).

-record(state, {client,    % local client
                req,       % current request (for multi-message requests like list keys)
                req_ctx}). % context to go along with request (partial results, request ids etc)

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),
    #state{client=C}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    handle_decoded(Msg).

handle_decoded(rpblistbucketsreq = Msg) ->
    %% backwards compat
    {ok, Msg, {"riak_kv.list_buckets", <<"default">>}};
handle_decoded(#rpblistbucketsreq{} = Msg) ->
    Type = convert_type(Msg#rpblistbucketsreq.type),
    {ok, Msg, {"riak_kv.list_buckets", Type}};
handle_decoded(#rpblistkeysreq{} = Msg) ->
    Type = convert_type(Msg#rpblistkeysreq.type),
    {ok, Msg, {"riak_kv.list_keys", {Type,
                                     Msg#rpblistkeysreq.bucket}}}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.


%% this should remain for backwards compatibility
process(rpblistbucketsreq, State) ->
    process(#rpblistbucketsreq{stream = false}, State, []);

%% @doc process/2 callback. Handles an incoming request message.
process(Req, State) ->
    process(Req, State, #{}).

%% @doc process/3 callback. Handles an incoming request message.
process(Req, State, ProcessOptions) ->
    {Class, Listing} = determine_class_and_listing(Req),
    Accept = determine_accept_and_report_job_disposition(Class),
    % Inspect ProcessOptions for the presence of a message recv_time.
    % If present, update the timeout value to account for the time spent
    OriginalTimeout = get_timeout_from_req(Req),
    Timeout = case app_helper:get_env(riak_core, use_dynamic_timeouts, true) of
        true ->
            case maps:get(recv_time, ProcessOptions, undefined) of
                Time when is_integer(Time) ->
                    DiffTime = (erlang:monotonic_time() - Time),
                    max(OriginalTimeout - erlang:convert_time_unit(DiffTime, native, millisecond), 0);
                _ ->
                    OriginalTimeout
            end;
        _ ->
            OriginalTimeout
    end,
    % if the timeout is at or below the minimum, the don't bother with the request
    % and return a timeout error
    case {Accept, Listing} of
        {true, buckets} ->
            maybe_do_list_buckets(Req, Timeout, State);
        {true, keys} ->
            maybe_stream_list_keys(Req, Timeout, State);
        {false, _} ->
            error_accept(Class, State)
    end.

%% @doc get the timeout from the request.  This is used to
%% determine the timeout for the list keys and list buckets
%% requests.
-spec get_timeout_from_req(#rpblistbucketsreq{} | #rpblistkeysreq{}) ->
    optional(pos_integer()).
get_timeout_from_req(#rpblistbucketsreq{timeout = Timeout}) ->
    Timeout;
get_timeout_from_req(#rpblistkeysreq{timeout = Timeout}) ->
    Timeout;
get_timeout_from_req(_) ->
    undefined.

determine_accept_and_report_job_disposition(Class) ->
    Accept = riak_core_util:job_class_enabled(Class),
    _ = riak_core_util:report_job_request_disposition(
        Accept, Class, ?MODULE, process, ?LINE, protobuf),
    Accept.

determine_class_and_listing(#rpblistbucketsreq{stream = true}) ->
    {{riak_kv, stream_list_buckets}, buckets};
%% Protobuf does _not_ set optional booleans to `false` per the spec.
%% Therefore, if it's not explicitly `true` it must be `false`.
determine_class_and_listing(#rpblistbucketsreq{stream = _Stream}) ->
    {{riak_kv, list_buckets}, buckets};
determine_class_and_listing(#rpblistkeysreq{}) ->
    %% at present list-keys always streams
    {{riak_kv, stream_list_keys}, keys}.

maybe_stream_list_keys(#rpblistkeysreq{type = Type, bucket = B} = Req, T,
                       #state{client = Client} = State) ->
    case check_bucket_type(Type) of
        {ok, GoodType} ->
            Bucket = maybe_create_bucket_type(GoodType, B),
            {ok, ReqId} = riak_client:stream_list_keys(Bucket, T, Client),
            {reply, {stream, ReqId}, State#state{req = Req, req_ctx = ReqId}};
        error ->
            error_no_bucket_type(Type, State)
    end.

error_no_bucket_type(Type, State) ->
    {error, {format, "No bucket-type named '~s'", [Type]}, State}.

error_accept(Class, State) ->
    {error, riak_core_util:job_class_disabled_message(binary, Class), State}.

maybe_do_list_buckets(#rpblistbucketsreq{type = Type, stream = S} = Req, T, State) ->
    case check_bucket_type(Type) of
        {ok, GoodType} ->
            do_list_buckets(GoodType, T, S, Req, State);
        error ->
            error_no_bucket_type(Type, State)
    end.


%% @doc process_stream/3 callback. Handles streaming keys messages and
process_stream(Req, ReqId, State) ->
    process_stream(Req, ReqId, State, []).

%% @doc process_stream/4 callback. Handles streaming keys messages and
%% streaming buckets.
process_stream({ReqId, done}, ReqId,
               State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}, _Options) ->
    {done, #rpblistkeysresp{done = 1}, State};
process_stream({ReqId, From, {keys, []}}, ReqId,
               State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}, _Options) ->
    _ = riak_kv_keys_fsm:ack_keys(From),
    {ignore, State};
process_stream({ReqId, {keys, []}}, ReqId,
               State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}, _Options) ->
    {ignore, State};
process_stream({ReqId, From, {keys, Keys}}, ReqId,
               State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}, _Options) ->
    _ = riak_kv_keys_fsm:ack_keys(From),
    {reply, #rpblistkeysresp{keys = Keys}, State};
process_stream({ReqId, {keys, Keys}}, ReqId,
               State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}, _Options) ->
    {reply, #rpblistkeysresp{keys = Keys}, State};
process_stream({ReqId, {error, Error}}, ReqId,
               State=#state{ req=#rpblistkeysreq{}, req_ctx=ReqId}, _Options) ->
    {error, {format, Error}, State#state{req = undefined, req_ctx = undefined}};
process_stream({ReqId, Error}, ReqId,
               State=#state{ req=#rpblistkeysreq{}, req_ctx=ReqId}, _Options) ->
    {error, {format, Error}, State#state{req = undefined, req_ctx = undefined}};
%% list buckets clauses.
process_stream({ReqId, done}, ReqId,
               State=#state{req=#rpblistbucketsreq{}, req_ctx=ReqId}, _Options) ->
    {done, #rpblistbucketsresp{done = 1}, State};
process_stream({ReqId, {buckets_stream, []}}, ReqId,
               State=#state{req=#rpblistbucketsreq{}, req_ctx=ReqId}, _Options) ->
    {ignore, State};
process_stream({ReqId, {buckets_stream, Buckets}}, ReqId,
               State=#state{req=#rpblistbucketsreq{}, req_ctx=ReqId}, _Options) ->
    {reply, #rpblistbucketsresp{buckets = Buckets}, State};
process_stream({ReqId, {error, Error}}, ReqId,
               State=#state{ req=#rpblistbucketsreq{}, req_ctx=ReqId}, _Options) ->
    {error, {format, Error}, State#state{req = undefined, req_ctx = undefined}};
process_stream({ReqId, Error}, ReqId,
               State=#state{ req=#rpblistbucketsreq{}, req_ctx=ReqId}, _Options) ->
    {error, {format, Error}, State#state{req = undefined, req_ctx = undefined}}.

handle_metrics(_Message, #{req_end_time := undefined}) ->
    ok;
handle_metrics(#rpblistbucketsreq{}, _Metrics) ->
    riak_kv_stat:update({pb_client, list_buckets});
handle_metrics(#rpblistkeysreq{}, #{req_start_time := Start, req_end_time := End}) ->
    ElapsedUs = erlang:convert_time_unit(End - Start, native, microsecond),
    riak_kv_stat:update({pb_client, list_keys, ElapsedUs});
handle_metrics(Message, _Metrics) ->
    ?LOG_ERROR("Unhandled metrics for message: ~p", [Message]),
    ok.

-spec do_list_buckets(binary(), optional(pos_integer()), optional(boolean()), #rpblistbucketsreq{}, #state{}) ->
                             {reply, tuple(), #state{}} |
                             {error, {format, iodata()}, #state{}}.
do_list_buckets(Type, Timeout, true, Req, #state{client=C}=State) ->
    {ok, ReqId} = riak_client:stream_list_buckets(none, Timeout, Type, C),
    {reply, {stream, ReqId}, State#state{req = Req, req_ctx = ReqId}};
do_list_buckets(Type, Timeout, _Stream, _Req, #state{client=C}=State) ->
    case riak_client:list_buckets(none, Timeout, Type, C) of
        {ok, Buckets} ->
            {reply, #rpblistbucketsresp{buckets = Buckets}, State};
        {error, Reason} ->
            {error, {format, Reason}, State}
    end.

-spec check_bucket_type(optional(binary())) -> {ok, binary()} | error.
check_bucket_type(undefined) -> {ok, <<"default">>};
check_bucket_type(<<"default">>) -> {ok, <<"default">>};
check_bucket_type(Type) ->
    case riak_core_bucket_type:get(Type) of
        Props when is_list(Props) ->
            {ok, Type};
        _ ->
            error
    end.

-spec maybe_create_bucket_type(binary()|undefined, binary()) -> riak_core_bucket:bucket().
maybe_create_bucket_type(<<"default">>, Bucket) ->
    Bucket;
maybe_create_bucket_type(undefined, Bucket) ->
    Bucket;
maybe_create_bucket_type(Type, Bucket) when is_binary(Type) ->
    {Type, Bucket}.

%% always construct {Type, Bucket} tuple, filling in default type if needed
-spec bucket_type(binary()|undefined, binary()) -> riak_core_bucket:bucket().
bucket_type(undefined, B) ->
    {<<"default">>, B};
bucket_type(T, B) ->
    {T, B}.


convert_type(undefined) ->
    <<"default">>;
convert_type(T) ->
    T.
