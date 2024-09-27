%% -------------------------------------------------------------------
%%
%% riak_kv_token_session:
%%  Process for managing a session associated with a token
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

%% @doc Receive a session request, and attempt to gain an exclusive token for
%% that session.  If one has been gained, allow for riak_client functions to
%% be managed via the session

-module(riak_kv_token_session).

-behavior(gen_server).

-include_lib("kernel/include/logger.hrl").

-export(
    [
        session_request/4,
        session_request_retry/1,
        session_use/3,
        session_release/1,
        session_renew/1
    ]
).

-export(
    [
        session_local_request/4,
        session_local_use/4,
        session_local_release/2,
        session_local_renew/2
    ]
).

-export(
    [
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3
    ]
).

-define(TOKEN_REQUEST_TIMEOUT, 12000).
-define(MINIMUM_SINGLE_REQUEST_TIMEOUT, 500).
-define(TOKEN_SESSION_TIMEOUT, 30000).
-define(TOKEN_RETRY_COUNT, 12).

-record(state, {token_id :: token_id(),
                token_timeout :: pos_integer(),
                client :: riak_client:riak_client()|undefined,
                session_id :: session_id()|undefined,
                start_time = os:system_time(microsecond) :: pos_integer()}).

-type token_id() :: riak_kv_token_manager:token_id().
-type verify_list() :: riak_kv_token_manager:verify_list().
-type session_ref() :: binary().
-type session_id() :: non_neg_integer().
-type timeout_ms() :: pos_integer().
-type erpc_session_error() ::
    {error, session_noconnection}|{error, session_remote_exit}.
-type token_request_mode() :: head_only|small_consensus|large_consensus.

-export_type([session_ref/0]).

%%%============================================================================
%%% External API
%%%============================================================================

-spec session_request_retry(
    token_id()) -> {true, session_ref()}|{false, none}|erpc_session_error().
session_request_retry(TokenID) ->
    TokenRequestMode =
        application:get_env(
            riak_kv,
            token_request_mode,
            primary_consensus
        ),
    session_request_retry(
        TokenID,
        TokenRequestMode,
        ?TOKEN_REQUEST_TIMEOUT,
        ?TOKEN_SESSION_TIMEOUT,
        ?TOKEN_RETRY_COUNT
    ).

-spec session_request_retry(
    token_id(),
    token_request_mode(),
    timeout_ms(),
    timeout_ms(),
    pos_integer()) -> {true, session_ref()}|{false, none}|erpc_session_error().
session_request_retry(TokenID, TRM, RequestTimeout, TokenTimeout, Retry) ->
    session_request_retry(
        TokenID, TRM, RequestTimeout, TokenTimeout, Retry, 0
    ).

session_request_retry(_TokenID, _TRM, 0, _TokenTO, _Retry, _Attempts) ->
    {false, none};
session_request_retry(TokenID, TRM, RequestTO, TokenTO, Retry, Retry) ->
    session_request(TokenID, TRM, RequestTO, TokenTO);
session_request_retry(TokenID, TRM, RequestTO, TokenTO, Retry, Attempts) ->
    RT0 = os:system_time(millisecond),
    NextTimeOut = max(?MINIMUM_SINGLE_REQUEST_TIMEOUT, RequestTO div 4),
    case session_request(TokenID, TRM, NextTimeOut, TokenTO) of
        {true, SessionRef} ->
            {true, SessionRef};
        _Error ->
            timer:sleep(
                rand:uniform(
                    max(
                        ?MINIMUM_SINGLE_REQUEST_TIMEOUT,
                        RequestTO div (Retry - Attempts)
                    )
                )
            ),
            session_request_retry(
                TokenID,
                TRM,
                max(0, RequestTO + (RT0 - os:system_time(millisecond))),
                TokenTO,
                Retry,
                Attempts + 1
            )
    end.

-spec session_request(
    token_id(),
    token_request_mode(),
    timeout_ms(),
    timeout_ms()) -> {true, session_ref()}|{false, none}|erpc_session_error().
session_request(TokenID, TRM, RequestTimeout, TokenTimeout) ->
    DocIdx = chash_key(TokenID),
    {TargetNodeCount, MinimumNodeCount, Partitions} = 
        case TRM of
            head_only ->
                {1, 1, riak_core_apl:get_primary_apl(DocIdx, 3, riak_kv)};
            basic_consensus ->
                {
                    3,
                    1,
                    riak_core_apl:get_apl_ann(
                        DocIdx, 3, riak_core_node_watcher:nodes(riak_kv))
                };
            primary_consensus ->
                {3, 3, riak_core_apl:get_primary_apl(DocIdx, 5, riak_kv)}
        end,
    PrimNodes =
        lists:sublist(
            uniq(lists:map(fun({{_Idx, N}, _}) -> N end, Partitions)),
            TargetNodeCount
        ),
    case PrimNodes of
        [Head|VerifyList] when length(PrimNodes) >= MinimumNodeCount ->
            case Head of
                ThisNode when ThisNode == node() ->
                    session_local_request(
                        TokenID, VerifyList, RequestTimeout, TokenTimeout);
                _ ->
                    try
                        erpc:call(
                            Head,
                            ?MODULE,
                            session_local_request,
                            [TokenID, VerifyList, RequestTimeout, TokenTimeout]
                        )
                    catch
                        error:{erpc,noconnection}:_ ->
                            ?LOG_WARNING(
                                "Connection error"
                                " accessing token_manager on ~w",
                                [Head]),
                            riak_kv_stat:update(token_session_unreachable),
                            {false, none}
                    end
            end;
        _ ->
            ok = riak_kv_stat:update(token_session_preflist_short),
            {false, none}
    end.

-spec session_use(session_ref(), atom(), list()) -> any().
session_use(SessionReference, FuncName, Args) ->
    {N, P, ID} = decode_session_reference(SessionReference),
    case node() of
        ThisNode when ThisNode == N ->
            session_local_use(P, FuncName, Args, ID);
        _ ->
            safe_erpc(N, ?MODULE, session_local_use, [P, FuncName, Args, ID])
    end.

-spec session_release(session_ref()|none) -> ok|erpc_session_error().
session_release(none) ->
    ok;
session_release(SessionReference) ->
    {N, P, ID} = decode_session_reference(SessionReference),
    case node() of
        ThisNode when ThisNode == N ->
            session_local_release(P, ID);
        _ ->
            safe_erpc(N, ?MODULE, session_local_release, [P, ID])
    end.


-spec session_renew(session_ref()) -> ok|erpc_session_error().
session_renew(SessionReference) ->
    {N, P, ID} = decode_session_reference(SessionReference),
    case node() of
        ThisNode when ThisNode == N ->
            session_local_renew(P, ID);
        _ ->
            safe_erpc(N, ?MODULE, session_local_renew, [P, ID])
    end.

%%%============================================================================
%%% Local API
%%%============================================================================

-spec safe_erpc(node(), atom(), atom(), list()) -> any().
safe_erpc(Node, Module, Function, Args) ->
    try
        erpc:call(Node, Module, Function, Args)
    catch
        error:{erpc, noconnection}:_ ->
            {error, session_noconnection};
        exit:_:_ ->
            {error, session_remote_exit}
    end.

-spec session_local_request(
        token_id(), verify_list(), timeout_ms(), timeout_ms()
    ) -> {boolean(), session_ref()}.
session_local_request(TokenID, VerifyList, RequestTimeout, TokenTimeout) ->
    {ok, Session} =
        gen_server:start(?MODULE, [{TokenID, TokenTimeout}], []),
    gen_server:call(Session, {request, RequestTimeout, VerifyList}, infinity).

-spec session_local_use(pid(), atom(), list(), session_id()) -> any().
session_local_use(Pid, FuncName, Args, ID) ->
    ok = riak_kv_token_manager:associated(Pid),
    receive
        {true, _Upstream} ->
            gen_server:call(
                Pid,
                {use_session, FuncName, Args, ID},
                infinity);
        _ ->
            {error, session_down}
    after
        ?TOKEN_SESSION_TIMEOUT ->
            {error, session_down}
    end.

-spec session_local_release(pid(), session_id()) -> ok.
session_local_release(Pid, ID) ->
    case is_process_alive(Pid) of
        true ->
            gen_server:call(Pid, {release, ID}, infinity);
        false ->
            ok
    end.

-spec session_local_renew(pid(), session_id()) -> ok.
session_local_renew(Pid, ID) ->
    gen_server:call(Pid, {renew, ID}, infinity).

%%%============================================================================
%%% Callback functions
%%%============================================================================

init([{TokenID, TokenTimeout}]) ->
    _Ref = erlang:monitor(process, whereis(riak_kv_token_manager)),
    {ok, #state{token_id = TokenID, token_timeout = TokenTimeout}}.

handle_call({request, RequestTimeout, VerifyList}, _From, State) ->
    ok = riak_kv_token_manager:request_token(State#state.token_id, VerifyList),
    receive
        granted ->
            <<SessionID:32/integer>> = crypto:strong_rand_bytes(4),
            SessionRef = encode_session_reference(SessionID),
            Client = riak_client:new(node(), State#state.token_id),
            {reply,
                {true, SessionRef},
                State#state{client = Client, session_id = SessionID},
                State#state.token_timeout};
        refused ->
            ok = riak_kv_stat:update(token_session_refusal),
            {stop, normal, {false, none}, State}
    after
        RequestTimeout ->
            ok = riak_kv_stat:update(token_session_request_timeout),
            {stop, normal, {false, none}, State}
    end;
handle_call({use_session, Function, Args, ID}, _From, State)
        when ID == State#state.session_id ->
    C = State#state.client,
    R = apply(riak_client, Function, lists:append(Args, [C])),
    {reply, R, State, State#state.token_timeout};
handle_call({release, ID}, _From, State) when ID == State#state.session_id ->
    Duration = os:system_time(microsecond) - State#state.start_time,
    ok = riak_kv_stat:update({token_session_time, Duration}),
    {stop, normal, ok, State};
handle_call({renew, ID}, _From, State) when ID == State#state.session_id ->
    ok = riak_kv_stat:update(token_session_renewal),
    {reply, ok, State, State#state.token_timeout}.

handle_cast(_Msg, State) ->
    {stop, normal, State}.

handle_info({'DOWN', _Ref, process, Manager, Reason}, State) ->
    ok = riak_kv_stat:update(token_session_error),
    ?LOG_WARNING(
        "Token session ~w terminated as manager ~w down for Reason ~w",
        [State#state.token_id, Manager, Reason]
    ),
    {stop, normal, State};
handle_info(timeout, State) ->
    ok = riak_kv_stat:update(token_session_timeout),
    ?LOG_INFO(
        "Token session ~w terminated due to timeout",
        [State#state.token_id]
    ),
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec chash_key(token_id()) -> chash:index().
chash_key({token, TokenID}) ->
    chash:key_of(TokenID);
chash_key(BucketKey) ->
    riak_core_util:chash_key(BucketKey).

-spec encode_session_reference(session_id()) -> session_ref().
encode_session_reference(SessionID) ->
    base64:encode(
        term_to_binary({node(), self(), SessionID})
    ).

-spec decode_session_reference(session_ref()) -> {node(), pid(), session_id()}.
decode_session_reference(SessionReference) ->
    binary_to_term(base64:decode(SessionReference)).


-if(?OTP_RELEASE < 25).
-spec uniq(List1) -> List2 when
    List1 :: [T],
    List2 :: [T],
    T :: term().

uniq(L) ->
  uniq_1(L, #{}).

uniq_1([X | Xs], M) ->
  case is_map_key(X, M) of
      true ->
          uniq_1(Xs, M);
      false ->
          [X | uniq_1(Xs, M#{X => true})]
  end;
uniq_1([], _) ->
  [].
-else.
uniq(L) -> lists:uniq(L).
-endif.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

basic_session_request_test_() ->
    {timeout, 10, fun basic_session_request_tester/0}.

basic_session_request_tester() ->
    _TokenManager = riak_kv_token_manager:start_link(),
    {true, SessionRef1} =
        session_local_request({token, <<"Batch1">>}, [], 1000, 2000),
    timer:sleep(1000),
    session_renew(SessionRef1),
    {1, 0, 1} = riak_kv_token_manager:stats(),
    timer:sleep(1001),
    {1, 0, 1} = riak_kv_token_manager:stats(),
    timer:sleep(1000),
    {0, 0, 0} = riak_kv_token_manager:stats(),

    {true, SessionRef2} =
        session_local_request({token, <<"Batch2">>}, [], 1000, 2000),
    {true, SessionRef3} =
        session_local_request({token, <<"Batch3">>}, [], 1000, 2000),
    Me = self(),
    SpawnFun =
        fun(TokenID) ->
            fun() ->
                SessionRsp =
                    session_local_request({token, TokenID}, [], 1000, 2000),
                Me ! SessionRsp
            end
        end,
    SpawnForQueueKillFun =
        fun(TokenID) ->
            fun() ->
                {ok, SPid} =
                    gen_server:start(?MODULE, [{{token, TokenID}, 2000}], []),
                Me ! {ok, SPid},
                SessionRsp =
                    gen_server:call(SPid, {request, 1000, []}, infinity),
                Me ! SessionRsp
            end
        end,
    spawn(SpawnFun(<<"Batch3">>)),
    timer:sleep(100),
    {2, 1, 3} = riak_kv_token_manager:stats(),
    session_release(SessionRef3),
    receive
        {true, SessionRef4} ->
            ok
    end,
    P4 = element(2, decode_session_reference(SessionRef4)),
    {2, 0, 2} = riak_kv_token_manager:stats(),
    spawn(SpawnFun(<<"Batch3">>)),
    spawn(SpawnFun(<<"Batch3">>)),
    spawn(SpawnForQueueKillFun(<<"Batch3">>)),
    receive {ok, P7} -> ok end,
    timer:sleep(100),
    {2, 3, 5} = riak_kv_token_manager:stats(),
    exit(P4, kill),
    receive
        {true, SessionRef5} ->
            ok
    end,
    {2, 2, 4} = riak_kv_token_manager:stats(),
    session_release(SessionRef2),
    {1, 2, 3} = riak_kv_token_manager:stats(),
    exit(P7, kill),
    timer:sleep(100),
    {1, 1, 2} = riak_kv_token_manager:stats(),
    session_release(SessionRef5),
    receive
        {true, _SessionRef6} ->
            ok
    end,
    {1, 0, 1} = riak_kv_token_manager:stats(),

    gen_server:stop(riak_kv_token_manager).


-endif.