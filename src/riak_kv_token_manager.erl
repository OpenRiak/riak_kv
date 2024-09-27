%% -------------------------------------------------------------------
%%
%% riak_kv_token_manager: Process for granting access to tokens
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

%% @doc Grant a requests for token, when no other request is active.
%% 
%% Every node in a Riak cluster is expecetd to have one, and only one
%% riak_kv_token_manager.  Client actions requiring tokens, may start a
%% riak_kv_token_session process on a node, and request a token from the local
%% riak_kv_token_manager.  If a token is granted, the session may then be used
%% to marhsall riak_client functions, under the "ownership" of that token.
%% 
%% A riak_kv_token_session process will cast a request for the grant of a given
%% token, and await an async receive of either a `granted` or `refused` message
%% in response.
%% 
%% Tokens may be a {Bucket, Key} pair, a {{Type, Bucket}, Key} tuple or a
%% {token, TokenName} tuple where TokenName is any binary name. 
%% 
%% The session process should monitor the token_manager to which it makes a
%% request, and fail itself on the failure of that token_manager.  Requests
%% should only be made from local session (i.e. a remote process should start
%% a local session to make the request).  The session process should close as
%% soon as the token is no longer required, or otherwise on a timeout.  The
%% token manager will monitor the session pid and release the grant on closure
%% of the session for any reason.  Sessions which never end, will never have
%% their grants released.
%% 
%% A request may be granted immediately, if the token is available. If the
%% token is not available, then the response may be deferred and the request
%% queued, to be granted when the holding session is released (as well as any
%% session requests queued earlier).  The queue if FIFO.
%% 
%% A request may optionally pass a "verify list", a list of nodes with whom the
%% request should be confirmed before granting.  Within riak the verify list is
%% aligned with the preflist, of the token key being requested (i.e. it is the
%% list of nodes associated with the vnodes in the preflist).  If a verify list
%% is present those "downstream" token managers should confirm that they have
%% not granted the token before the `granted` message is returned (and record
%% the existence of the upstream grant in their map of grants).  Confirmation
%% is achieved through the passing of async downstream check/reply messages.
%% 
%% Not passing a consistent verify list for token requests will increase the
%% number of failure scenarios where duplicate tokens could be concurrently
%% granted within the cluster - but reduce the latency and cost of granting a
%% token.
%% 
%% The grant may be refused immediately, rather than queued, if a previous
%% token has been granted but to a different verify list.  In this case, the
%% session should back-off and retry.  Once any tokens requested under
%% different conditions have been released new tokens under the changed
%% conditions may be granted.  A grant will always be refused immediately if
%% a grant for that token is active upstream.
%% 
%% When a request is de-queued, it bypasses the wait to confirm downstream
%% nodes do not have a grant (given that they already confirmed for the grant
%% made at the head of the queue).  A downstream renew is sent in the
%% background, rather than the downstream check message (and a renew message
%% does not require a reply).
%% 
%% Each Token Manager should monitor every Token Manager from which it
%% receives a downstream check.  If a remote token manager goes down, then all
%% notifications of upstream grants associated with that manager should be
%% cleared.
%% 
%% The token manager monitors every local token session to which it grants a
%% token, and on receipt of a 'DOWN' notification, it will clear the grant (or
%% queued request) and association for that session - and inform the verify
%% list of managers using a downstream release message.
%% 
%% This overall mechanism is to provide "stronger" but not "strong"
%% consistency.  The aim is to have a sub-system whereby in healthy clusters
%% and in common failure scenarios tokens can be requested without conflict -
%% in the first case to allow for conditional logic on PUTs to detect conflict
%% with greater reliability.
%% 
%% It is accepted that there will be partition scenarios, and scenarios where
%% rapid changes in up/down state where guarantees cannot be met. The intention
%% is that eventual consistency will be the fallback.  The application/operator
%% may have to deal with siblings to protect against data loss - but the
%% frequency with which those siblings occur can be reduced through the use of
%% these tokens.
%% 
%% The wnd-to-end system (Riak) is still intended to be a used as an eventually
%% consistent database.

-module(riak_kv_token_manager).

-behavior(gen_server).

-include_lib("kernel/include/logger.hrl").

-export(
    [
        start_link/0,
        request_token/2,
        associated/1,
        associated/2,
        stats/0,
        grants/0
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

-ifdef(TEST).
-define(SWEEP_DELAY, 1000).
-define(ASSOCIATION_CHECK_TIMEOUT, 1000).
-else.

-define(SWEEP_DELAY, 10000).
-define(ASSOCIATION_CHECK_TIMEOUT, 5000).

-endif.

-record(state, {grants = maps:new() :: grant_map(),
                queues = maps:new() :: request_queues(),
                associations = maps:new() :: #{session_pid() => token_id()},
                monitored_managers = [] :: list(manager_mon()),
                last_sweep_grants = sets:new([{version, 2}]) ::
                    sets:set({token_id(), granted_session()})}).

-type verify_count() :: non_neg_integer().
-type granted_session()
    :: {local, session_pid(), verify_list(), verify_count()}|
        {upstream, upstream_ref()}.
-type grant_map() :: #{token_id() => granted_session()}.
-type request_queues()
    :: #{token_id() => list({pid(), verify_list()})}.
-type token_id() :: {token, binary()}|{riak_object:bucket(), riak_object:key()}.
-type verify_list() :: [downstream_node()].
-type session_pid() :: pid().
-type manager_pid() :: pid().
-type manager_mon() :: {manager_pid(), reference()}.
-type upstream_ref() :: {manager_pid(), session_pid()}.
-type downstream_node() :: node()|pid().
    % in tests will be a pid() not a node()

-export_type([token_id/0, verify_list/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec start_link() -> {ok, manager_pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc request_token/2
%% Request a token (TokenID) from this node's Token Manager, requiring the
%% request to be verified in the nodes provided in the VerifyList.
%% 
%% An async response message of either `granted` or `refused` will be returned.
%% Any session process making a request should monitor the riak_kv_token_manager
%% and terminate should the manager terminate.
%% 
%% A token is released when the riak_kv_token_manager receives a 'DOWN' message
%% from a session process associated with a grant.
-spec request_token(token_id(), [node()]) -> ok.
request_token(TokenID, VerifyList) ->
    gen_server:cast(
        ?MODULE, {request, TokenID, VerifyList, self()}). 

%% @doc associated/1
%% Confirms if this token manager has an association with the PID of a session.
%% Associations are kept for local sessions only (where this manager has
%% granted the token)
%% 
%% Associated check used by a session process before calls to use the session
%% to confirm that the session is still active, and also by downstream token
%% managers to confirm that lingering upstream sessions are still active.
-spec associated(session_pid()) -> ok.
associated(SessionPid) ->
    gen_server:cast(?MODULE, {associated, SessionPid, self()}).

-spec associated(node(), session_pid()) -> ok.
associated(Node, SessionPid) ->
    gen_server:cast({?MODULE, Node}, {associated, SessionPid, self()}).

%%%============================================================================
%%% API Remote - Downstream/Upstream messages between token managers
%%%============================================================================

%% @doc downstream_check/3
%% Call to a remote node to confirm that it does not currently have a
%% information of a grant from another node.  If it does have such a grant the
%% token request will be refused (return false).
%% 
%% If there is no grant registered, or if the grant that is registered is from
%% the same remote_manager, then the downstream_check will pass and the grant
%% information will be updated
-spec downstream_check(downstream_node(), token_id(), pid()) -> ok.
downstream_check(TestPid, TokenID, SessionPid) when is_pid(TestPid) ->
    gen_server:cast(
        TestPid, {downstream_check, TokenID, {self(), SessionPid}});
downstream_check(ToNode, TokenID, SessionPid) ->
    gen_server:cast(
        {?MODULE, ToNode}, {downstream_check, TokenID, {self(), SessionPid}}).

%% @doc renew_downstream
%% If a token has been released, and a queued token is now to be granted the
%% granting is not blocked by the use of is_downstream_check/3, it is assumed
%% that a notification is present due to the original grant.
-spec downstream_renew(verify_list(), token_id(), pid()) -> ok.
downstream_renew([], _TokenID, _SessionPid) ->
    ok;
downstream_renew([TestPid|Rest], TokenID, SessionPid) when is_pid(TestPid) ->
    gen_server:cast(
        TestPid,
        {downstream_renew, TokenID, {self(), SessionPid}}
    ),
    downstream_renew(Rest, TokenID, SessionPid);
downstream_renew([N|Rest], TokenID, SessionPid) ->
    gen_server:cast(
        {?MODULE, N}, {downstream_renew, TokenID, {self(), SessionPid}}),
    downstream_renew(Rest, TokenID, SessionPid).

%% @doc release_downstream
%% If a token has been released, and there is no queued token, release the
%% downstream blocks.
-spec downstream_release(verify_list(), token_id(), pid()) -> ok.
downstream_release([], _TokenID, _SessionPid) ->
    ok;
downstream_release([TestPid|Rest], TokenID, SessionPid) when is_pid(TestPid) ->
    gen_server:cast(
        TestPid,
        {downstream_release, TokenID, {self(), SessionPid}}
    ),
    downstream_release(Rest, TokenID, SessionPid);
downstream_release([N|Rest], TokenID, SessionPid) ->
    gen_server:cast(
        {?MODULE, N}, {downstream_release, TokenID, {self(), SessionPid}}),
    downstream_release(Rest, TokenID, SessionPid).

%%%============================================================================
%%% API - Operations (helper functions)
%%%============================================================================

%% @doc stats/0
%% Return three counts - the count of grants, the count of queued requests and
%% the count of associations (whenever a grant is made or queued an association
%% is retain to map the PID to the token as a helper should the PID go down).
-spec stats() -> {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
stats() ->
    gen_server:call(?MODULE, stats).

%% @doc grants/0
%% A map of the current grants that have bee made by this token_manager
-spec grants() -> grant_map().
grants() ->
    gen_server:call(?MODULE, grants).

%%%============================================================================
%%% Callback functions
%%%============================================================================

init(_Args) ->
    erlang:send_after(?SWEEP_DELAY, self(), sweep),
    {ok, #state{}}.
    
handle_call(stats, _From, State) ->
    Grants = maps:size(State#state.grants),
    QueuedRequests =
        maps:fold(
            fun(_T, Q, Acc) -> length(Q) + Acc end,
            0,
            State#state.queues
        ),
    Associations = maps:size(State#state.associations),
    {reply, {Grants, QueuedRequests, Associations}, State};
handle_call(grants, _From, State) ->
    {reply, State#state.grants, State}.

handle_cast({request, TokenID, VerifyList, Session}, State) ->
    case maps:get(TokenID, State#state.grants, not_found) of
        not_found ->
            lists:foreach(
                fun(N) -> downstream_check(N, TokenID, Session) end,
                VerifyList
            ),
            UpdAssocs =
                maps:put(Session, TokenID, State#state.associations),
            _SidRef = monitor(process, Session),
            UpdGrants =
                maps:put(
                    TokenID,
                    {local, Session, VerifyList, 0},
                    State#state.grants
                ),
            case VerifyList of
                [] ->
                    Session ! granted;
                _ ->
                    %% Need to wait for downstream replies
                    ok
            end,
            {noreply,
                State#state{
                    grants = UpdGrants, associations = UpdAssocs
                }
            };
        {local, _OtherSession, CurrentVerifyList, _VerifyCount}
                when CurrentVerifyList == VerifyList ->
            %% Can queue this session, it has the same verifylist as the
            %% existing grant
            UpdAssocs =
                maps:put(Session, TokenID, State#state.associations),
            _SidRef = monitor(process, Session),
            TokenQueue = maps:get(TokenID, State#state.queues, []),
            UpdQueue =
                maps:put(
                    TokenID,
                    [{Session, VerifyList}|TokenQueue],
                    State#state.queues
                ),
            {noreply,
                State#state{
                    queues = UpdQueue,
                    associations = UpdAssocs
                }
            };
        _ ->
            Session ! refused,
            {noreply, State}
    end;
handle_cast({downstream_renew, TokenID, {Manager, Session}}, State) ->
    case maps:get(TokenID, State#state.grants, not_found) of
        not_found ->
            UpdGrants =
                maps:put(
                    TokenID,
                    {upstream, {Manager, Session}},
                    State#state.grants
                ),
            {noreply,
                State#state{
                    grants = UpdGrants,
                    monitored_managers =
                        monitor_manager(
                            Manager,
                            State#state.monitored_managers
                        )
                }
            };
        {upstream, {CurrentManager, _OldSession}}
                when Manager == CurrentManager ->
           UpdGrants =
                maps:put(
                    TokenID,
                    {upstream, {Manager, Session}},
                    State#state.grants
                ),
            {noreply, State#state{grants = UpdGrants}};
        ExistingGrant ->
            ?LOG_WARNING(
                "Potential conflict ignored on token ~w between ~w and ~w",
                [TokenID, ExistingGrant, {Manager, Session}]
            ),
            {noreply, State}
    end;
handle_cast({downstream_release, TokenID, {Manager, Session}}, State) ->
    case maps:get(TokenID, State#state.grants, not_found) of
        not_found ->
            {noreply, State};
        {upstream, {CurrentManager, _OldSession}}
                when Manager == CurrentManager ->
            UpdGrants = maps:remove(TokenID, State#state.grants),
            {noreply, State#state{grants = UpdGrants}};
        ExistingGrant ->
            ?LOG_WARNING(
                "Potential conflict ignored on token ~w between ~w and ~w",
                [TokenID, ExistingGrant, {Manager, Session}]
            ),
            {noreply, State}
    end;
handle_cast({downstream_check, TokenID, {Manager, Session}}, State) ->
    case maps:get(TokenID, State#state.grants, not_found) of
        not_found ->
            gen_server:cast(
                Manager, {downstream_reply, TokenID, Session, true}
            ),
            UpdGrants =
                maps:put(
                    TokenID,
                    {upstream, {Manager, Session}},
                    State#state.grants
                ),
            {noreply,
                State#state{
                    grants = UpdGrants,
                    monitored_managers =
                        monitor_manager(
                            Manager,
                            State#state.monitored_managers
                        )
                }
            };
        {upstream, {BlockingMgr, _PrevSession}} when BlockingMgr == Manager ->
            %% Trust the upstream manager knows what they're doing.  This is
            %% likely to be as a result of message misordering
            gen_server:cast(
                Manager, {downstream_reply, TokenID, Session, true}
            ),
            UpdGrants =
                maps:put(
                    TokenID,
                    {upstream, {Manager, Session}},
                    State#state.grants
                ),
            {noreply, State#state{grants = UpdGrants}};
        _Block ->
            gen_server:cast(
                Manager, {downstream_reply, TokenID, Session, false}
            ),     
            {noreply, State}
    end;
handle_cast({downstream_reply, TokenID, Session, true}, State) ->
    case maps:get(TokenID, State#state.grants, not_found) of
        {local, ThisSession, VerifyList, VerifyCount}
                when ThisSession == Session ->
            case length(VerifyList) of
                VL when VL == (VerifyCount + 1) ->
                    Session ! granted;
                _ ->
                    ok
            end,
            {noreply,
                State#state{
                    grants = 
                        maps:put(
                            TokenID,
                            {local, Session, VerifyList, VerifyCount + 1},
                            State#state.grants
                        )
                    }
                };
        _ ->
            {noreply, State}
    end;
handle_cast({downstream_reply, TokenID, Session, false}, State) ->
    case maps:get(TokenID, State#state.grants, not_found) of
        {local, ThisSession, _VerifyList, _VerifyCount}
                when Session == ThisSession ->
            Session ! refused,
            {noreply,
                State#state{grants = maps:remove(TokenID, State#state.grants)}
            };
        _ ->
            {noreply, State}
    end;
handle_cast({associated, SessionPid, FromPid}, State) ->
    FromPid !
        {maps:is_key(SessionPid, State#state.associations),
            {self(), SessionPid}
        },
    {noreply, State}.


handle_info({'DOWN', Ref, process, Pid, Reason}, State) ->
    case maps:take(Pid, State#state.associations) of
        {TokenID,  UpdAssocs} ->
            %% An association exists, so this is assumed to be a session
            %% process which has gone down.  This might be in a queue, or
            %% in receipt of a grant
            Queues =
                clear_session_from_queues(
                    State#state.queues,
                    TokenID,
                    Pid
                ),
            case maps:get(TokenID, State#state.grants, not_found) of
                {local, ActivePid, VerifyList, _VerifyCount}
                        when ActivePid == Pid ->
                    %% There is a grant, is there a queued request for that
                    %% same token that we can now grant
                    case return_session_from_queues(Queues, TokenID) of
                        {none, UpdQueues} ->
                            ok =
                                downstream_release(
                                    VerifyList,
                                    TokenID,
                                    Pid
                                ),
                            {noreply,
                                State#state{
                                    associations = UpdAssocs,
                                    queues = UpdQueues,
                                    grants =
                                        maps:remove(
                                            TokenID,
                                            State#state.grants
                                        )
                                }
                            };
                        {{NextSession, NextVerifyList}, UpdQueues}
                                when NextVerifyList == VerifyList ->
                            %% Only requests with the same VerifyList should be
                            %% queued
                            UpdGrants =
                                maps:put(
                                    TokenID,
                                    {local, NextSession, VerifyList, 0},
                                    State#state.grants
                                ),
                            NextSession ! granted,
                            %% Check that the downstream nodes are still aware
                            %% of this grant (in case, for example, they have
                            %% restarted in between)
                            ok =
                                downstream_renew(
                                    VerifyList,
                                    TokenID,
                                    NextSession
                                ),
                            {noreply,
                                State#state{
                                    associations = UpdAssocs,
                                    queues = UpdQueues,
                                    grants = UpdGrants
                                }
                            }
                    end;
                _ ->
                    %% The session may have been queued and not had a grant
                    %% It may be an upstream grant which has changed before the
                    %% 'DOWN' message was received
                    {noreply,
                        State#state{associations = UpdAssocs, queues = Queues}
                    }
            end;
        _ ->
            case lists:member({Pid, Ref}, State#state.monitored_managers) of
                true ->
                    ?LOG_WARNING(
                        "Remote Token Manager ~w reported down due to ~p",
                        [Pid, Reason]
                    ),
                    FilterFun =
                        fun({_Tid, G}) ->
                            case G of
                                {upstream, {UpstreamPid, _Session}}
                                        when UpstreamPid == Pid ->
                                    false;
                                _ ->
                                    true
                            end
                        end,
                    UpdGrants =
                        maps:from_list(
                            lists:filter(
                                FilterFun,
                                maps:to_list(State#state.grants)
                            )
                        ),
                    UpdMonitors =
                        lists:delete(
                            {Pid, Ref},
                            State#state.monitored_managers
                        ),
                    {noreply,
                        State#state{
                            grants =  UpdGrants,
                            monitored_managers = UpdMonitors
                        }
                    };
                false ->
                    ?LOG_INFO(
                        "Session ~w cleared for ~w but not present",
                        [Pid, Reason]
                    ),
                    {noreply, State}
            end
    end;
handle_info(sweep, State) ->
    erlang:send_after(
        rand:uniform(?SWEEP_DELAY) + ?SWEEP_DELAY div 2,
        self(),
        sweep
    ),
    LastSweep = State#state.last_sweep_grants,
    ThisSweep =
        sets:from_list(maps:to_list(State#state.grants), [{version, 2}]),
    StillPresent = sets:to_list(sets:intersection(LastSweep, ThisSweep)),
    check_active(StillPresent, self()),
    {noreply, State#state{last_sweep_grants = ThisSweep}};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec clear_session_from_queues(
    request_queues(), token_id(), pid()) -> request_queues().
clear_session_from_queues(Queues, TokenID, Session) ->
    Queue = maps:get(TokenID, Queues, []),
    case lists:filter(fun({QS, _VL}) -> QS =/= Session end, Queue) of
        [] ->
            maps:remove(TokenID, Queues);
        UpdTQueue ->
            maps:put(TokenID, UpdTQueue, Queues)
    end.

-spec return_session_from_queues(
    request_queues(), token_id()) -> 
        {{pid(), verify_list()}|none, request_queues()}.
return_session_from_queues(Queues, TokenID) ->
    case maps:get(TokenID, Queues, []) of
        [] ->
            {none, Queues};
        QueuedRequests ->
            case lists:reverse(QueuedRequests) of
                [{NextSession, VerifyList}] ->
                    {{NextSession, VerifyList}, maps:remove(TokenID, Queues)};
                [{NextSession, VerifyList}|QueueRem] ->
                    {{NextSession, VerifyList},
                        maps:put(TokenID, lists:reverse(QueueRem), Queues)}
            end
    end.


-spec monitor_manager(
    manager_pid(), list(manager_mon())) -> list(manager_mon()).
monitor_manager(Manager, MonitoredManagers) ->
    case lists:keymember(Manager, 1, MonitoredManagers) of
        true ->
            MonitoredManagers;
        false ->
            MgrRef = monitor(process, Manager),
            [{Manager, MgrRef}|MonitoredManagers]
    end.

-spec check_active(list({token_id(), granted_session()}), pid()) -> ok.
check_active([], _Mgr) ->
    ok;
check_active([{_TokenID, {local, Session, _VL, _VC}}|Rest], Mgr) ->
    case is_process_alive(Session) of
        true ->
            check_active(Rest, Mgr);
        false ->
            _NewRef = monitor(process, Session),
            check_active(Rest, Mgr)
    end;
check_active([{TokenID, {upstream, {UpstreamMgr, UpstreamSess}}}|Rest], Mgr) ->
    _P = check_upstream_async(TokenID, UpstreamMgr, UpstreamSess, Mgr),
    check_active(Rest, Mgr).


-spec check_upstream_async(
    token_id(), manager_pid(), session_pid(), manager_pid()) -> pid().
check_upstream_async(TokenID, RemoteManager, Session, Mgr) ->
    F = fun() -> check_upstream(TokenID, RemoteManager, Session, Mgr) end,
    spawn(F).

-spec check_upstream(
    token_id(), manager_pid(), session_pid(), manager_pid()) -> ok. 
check_upstream(TokenID, RemoteManager, Session, Mgr) ->
    RemoteNode = node(RemoteManager),
    {UpstreamAssociated, Reason} =
        case RemoteNode of
            nonode@nohost ->
                {false, nonode@nohost};
            _ ->
                associated(RemoteNode, Session),
                receive
                    Reply ->
                        Reply
                after
                    ?ASSOCIATION_CHECK_TIMEOUT ->
                        {false, timeout}
                end
        end,
    case UpstreamAssociated of
        true ->
            ok;
        false ->
            ?LOG_WARNING(
                "Upstream association check to ~w prompted release "
                "of TokenID ~w due to reason ~p",
                [RemoteNode, TokenID, Reason]
            ),
            gen_server:cast(
                Mgr,
                {downstream_release, TokenID, {RemoteManager, Session}}
            )
    end.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

gc_test_() ->
    {timeout, 60, fun gc_tester/0}.

gc_tester() ->
    {ok, Mgr1} = gen_server:start(?MODULE, [], []),
    {ok, Mgr2} = gen_server:start(?MODULE, [], []),
    {ok, Mgr3} = gen_server:start(?MODULE, [], []),
    Req1 = requestor_fun(self(), <<"T1">>, Mgr1, [Mgr2, Mgr3]),
    Req2 = requestor_fun(self(), <<"T2">>, Mgr2, [Mgr3]),
    S1 = spawn(Req1),
    receive Gr1 -> ?assertMatch({granted, S1}, Gr1) end,
    S2 = spawn(Req2),
    receive Gr2 -> ?assertMatch({granted, S2}, Gr2) end,
    S3 = spawn(Req2),
    F1 = fun() -> {1, 0, 1} == gen_server:call(Mgr1, stats) end,
    F2 = fun() -> {2, 1, 2} == gen_server:call(Mgr2, stats) end,
    F3 = fun() -> {2, 0, 0} == gen_server:call(Mgr3, stats) end,
    wait_until(F1, 10, 1),
    wait_until(F2, 10, 1),
    wait_until(F3, 10, 1),

    timer:sleep(?SWEEP_DELAY + 1), % First sweep at fixed time
    State1 = sys:get_state(Mgr1),
    ?assertMatch(1, sets:size(State1#state.last_sweep_grants)),
    State2 = sys:get_state(Mgr2),
    ?assertMatch(2, sets:size(State2#state.last_sweep_grants)),
    State3 = sys:get_state(Mgr3),
    ?assertMatch(2, sets:size(State3#state.last_sweep_grants)),
    ?assert(F1()),
    ?assert(F2()),
    ?assert(F3()),

    Mgr1 ! {'DOWN', self(), process, S1, inactive},
    F1A = fun() -> {0, 0, 0} == gen_server:call(Mgr1, stats) end,
    F2A = fun() -> {1, 1, 2} == gen_server:call(Mgr2, stats) end,
    F3A = fun() -> {1, 0, 0} == gen_server:call(Mgr3, stats) end,
    wait_until(F1A, 10, 1),
    wait_until(F2A, 10, 1),
    wait_until(F3A, 10, 1),
    S1 ! terminate,
    F1A(),
    F2A(),
    F3A(),
    Mgr2 ! {'DOWN', self(), process, S2, inactive},
    receive Gr3 -> ?assertMatch({granted, S3}, Gr3) end,
    F2B = fun() -> {1, 0, 1} == gen_server:call(Mgr2, stats) end,
    F3B = fun() -> {1, 0, 0} == gen_server:call(Mgr3, stats) end,
    wait_until(F2B, 10, 1),
    wait_until(F3B, 10, 1),
    S2 ! terminate,
    ?assert(receive _ -> false after 1 -> true end),
    F2B(),
    F3B(),
    S3 ! terminate,
    ?assert(receive _ -> false after 1 -> true end),

    timer:sleep(?SWEEP_DELAY + ?SWEEP_DELAY div 2), % should be nothing to sweep
    StateZ1 = sys:get_state(Mgr1),
    ?assertMatch(0, sets:size(StateZ1#state.last_sweep_grants)),
    StateZ2 = sys:get_state(Mgr2),
    ?assertMatch(0, sets:size(StateZ2#state.last_sweep_grants)),
    StateZ3 = sys:get_state(Mgr3),
    ?assertMatch(0, sets:size(StateZ3#state.last_sweep_grants)),

    SA1 = spawn(Req1),
    receive GrA1 -> ?assertMatch({granted, SA1}, GrA1) end,
    SA2 = spawn(Req2),
    receive GrA2 -> ?assertMatch({granted, SA2}, GrA2) end,
    SA3 = spawn(Req2),
    wait_until(F1, 10, 1),
    wait_until(F2, 10, 1),
    wait_until(F3, 10, 1),

    timer:sleep(3 * ?SWEEP_DELAY),
        % Should prompt sweep, and remove remotes as nonode@nohost
    ?assertMatch({1, 0, 1}, gen_server:call(Mgr1, stats)),
    ?assertMatch({1, 1, 2}, gen_server:call(Mgr2, stats)),
    ?assertMatch({0, 0, 0}, gen_server:call(Mgr3, stats)),
    timer:sleep(3 * ?SWEEP_DELAY),
        % PRevious grants should remain stable
    StateX1 = sys:get_state(Mgr1),
    ?assertMatch(1, sets:size(StateX1#state.last_sweep_grants)),
    StateX2 = sys:get_state(Mgr2),
    ?assertMatch(1, sets:size(StateX2#state.last_sweep_grants)),
    StateX3 = sys:get_state(Mgr3),
    ?assertMatch(0, sets:size(StateX3#state.last_sweep_grants)),
    ?assertMatch({1, 0, 1}, gen_server:call(Mgr1, stats)),
    ?assertMatch({1, 1, 2}, gen_server:call(Mgr2, stats)),
    ?assertMatch({0, 0, 0}, gen_server:call(Mgr3, stats)),

    SA1 ! terminate,
    SA2 ! terminate,
    receive GrA3 -> ?assertMatch({granted, SA3}, GrA3) end,
    SA3 ! terminate,
    ?assert(receive _ -> false after 1 -> true end),
    
    gen_server:stop(Mgr1),
    gen_server:stop(Mgr2),
    gen_server:stop(Mgr3).

already_dead_test() ->
    {ok, Mgr1} = gen_server:start(?MODULE, [], []),
    DeadPid1 = spawn(fun() -> ok end),
    DeadPid2 = spawn(fun() -> ok end),
    T1 = <<"T1">>,
    VL = [],
    ok = gen_server:cast(Mgr1, {request, T1, VL, DeadPid1}),
    ok = gen_server:cast(Mgr1, {request, T1, VL, DeadPid2}),
    F = fun() -> {0, 0, 0} == gen_server:call(Mgr1, stats) end,
    ?assert(wait_until(F, ?SWEEP_DELAY div 2, 1)),
    gen_server:stop(Mgr1).


wait_until(F, Wait, _RetrySleep) when Wait =< 0 ->
    F();
wait_until(F, Wait, RetrySleep) ->
    timer:sleep(RetrySleep),
    case F() of
        true -> true;
        false -> wait_until(F, Wait - RetrySleep, RetrySleep)
    end.

manager_simple_test() ->
    {ok, Mgr1} = gen_server:start(?MODULE, [], []),
    {ok, Mgr2} = gen_server:start(?MODULE, [], []),
    {ok, Mgr3} = gen_server:start(?MODULE, [], []),
    Req1 = requestor_fun(self(), <<"T1">>, Mgr1, [Mgr2, Mgr3]),
    Req2 = requestor_fun(self(), <<"T1">>, Mgr2, [Mgr3]),
    S1 = spawn(Req1),
    receive Gr1 -> ?assertMatch({granted, S1}, Gr1) end,
    S2 = spawn(Req1),
    timer:sleep(1),
        % Make sure first request received
        % Avoid race over spawn time
    S3 = spawn(Req1),
    S4 = spawn(Req2),
    receive Rf1 -> ?assertMatch({refused, S4}, Rf1) end,
    S1 ! terminate,
    receive Gr2 -> ?assertMatch({granted, S2}, Gr2) end,
    S2 ! terminate,
    receive Gr3 -> ?assertMatch({granted, S3}, Gr3) end,
    S3 ! terminate,
    ?assert(receive _ -> false after 10 -> true end),
    gen_server:stop(Mgr1),
    gen_server:stop(Mgr2),
    gen_server:stop(Mgr3).


manager_multitoken_test() ->
    {ok, Mgr1} = gen_server:start(?MODULE, [], []),
    {ok, Mgr2} = gen_server:start(?MODULE, [], []),
    {ok, Mgr3} = gen_server:start(?MODULE, [], []),
    Req1 = requestor_fun(self(), <<"T1">>, Mgr1, [Mgr2, Mgr3]),
    Req2 = requestor_fun(self(), <<"T2">>, Mgr2, [Mgr1, Mgr3]),
    Req3 = requestor_fun(self(), <<"T3">>, Mgr2, [Mgr3, Mgr1]),
    Req4 = requestor_fun(self(), <<"T4">>, Mgr3, [Mgr1, Mgr2]),
    S1R1 = spawn(Req1),
    S2R1 = spawn(Req2),
    S3R1 = spawn(Req3),
    S4R1 = spawn(Req4),
    timer:sleep(1),
        % Make sure first round of requests received
        % Avoid race over spawn time
    S1R2 = spawn(Req1),
    S2R2 = spawn(Req2),
    S3R2 = spawn(Req3),
    S4R2 = spawn(Req4),
    ok = receive {granted, S1R1} -> ok end,
    ok = receive {granted, S2R1} -> ok end,
    ok = receive {granted, S3R1} -> ok end,
    ok = receive {granted, S4R1} -> ok end,
    S1R1 ! terminate,
    receive Gr1 -> ?assertMatch({granted, S1R2}, Gr1) end,
    S4R1 ! terminate,
    receive Gr2 -> ?assertMatch({granted, S4R2}, Gr2) end,
    S3R3 = spawn(Req3),
    S3R2 ! terminate,
    S2R1 ! terminate,
    receive Gr3 -> ?assertMatch({granted, S2R2}, Gr3) end,
    S3R1 ! terminate,
    receive Gr4 -> ?assertMatch({granted, S3R3}, Gr4) end,
    ?assert(receive _ -> false after 10 -> true end),
    ?assertMatch({4, 0, 1}, gen_server:call(Mgr1, stats)),
    ?assertMatch({4, 0, 2}, gen_server:call(Mgr2, stats)),
    ?assertMatch({4, 0, 1}, gen_server:call(Mgr3, stats)),
    S1R2 ! terminate,
    S4R2 ! terminate,
    S2R2 ! terminate,
    S3R3 ! terminate,
    ?assert(receive _ -> false after 10 -> true end),
        % This also allows time for Mgrs to receive 'DOWN'
    ?assertMatch({0, 0, 0}, gen_server:call(Mgr1, stats)),
    ?assertMatch({0, 0, 0}, gen_server:call(Mgr2, stats)),
    ?assertMatch({0, 0, 0}, gen_server:call(Mgr3, stats)),
    ?assertMatch(#{}, gen_server:call(Mgr1, grants)),
    ?assertMatch(#{}, gen_server:call(Mgr2, grants)),
    ?assertMatch(#{}, gen_server:call(Mgr3, grants)),
    gen_server:stop(Mgr1),
    gen_server:stop(Mgr2),
    gen_server:stop(Mgr3).

    
manager_downstream_failure_test() ->
    {ok, Mgr1} = gen_server:start(?MODULE, [], []),
    {ok, Mgr2} = gen_server:start(?MODULE, [], []),
    {ok, Mgr3} = gen_server:start(?MODULE, [], []),
    Req1 = requestor_fun(self(), <<"T1">>, Mgr1, [Mgr2, Mgr3]),
    S1 = spawn(Req1),
    receive Gr1 -> ?assertMatch({granted, S1}, Gr1) end,
    S2 = spawn(Req1),
    timer:sleep(1),
        % Make sure first request received
        % Avoid race over spawn time
    S3 = spawn(Req1),
    ok = gen_server:stop(Mgr2),
    {ok, Mgr2A} = gen_server:start(?MODULE, [], []),
    ?assert(is_process_alive(S1)),
    Req2 = requestor_fun(self(), <<"T1">>, Mgr1, [Mgr2A, Mgr3]),
        % Will not queue a request if the verify list has changed
    S4 = spawn(Req2),
    receive Rf1 -> ?assertMatch({refused, S4}, Rf1) end,
    S1 ! terminate,
    receive Gr2 -> ?assertMatch({granted, S2}, Gr2) end,
    S2 ! terminate,
    receive Gr3 -> ?assertMatch({granted, S3}, Gr3) end,
    S3 ! terminate,
    ?assert(receive _ -> false after 10 -> true end),
        % This also allows time for Mgrs to receive 'DOWN'
    S5 = spawn(Req2),
    receive Gr4 -> ?assertMatch({granted, S5}, Gr4) end,
    S5 ! terminate,
    ?assert(receive _ -> false after 10 -> true end),
    gen_server:stop(Mgr1),
    gen_server:stop(Mgr2A),
    gen_server:stop(Mgr3).

manager_primary_failure_test() ->
    {ok, Mgr1} = gen_server:start(?MODULE, [], []),
    {ok, Mgr2} = gen_server:start(?MODULE, [], []),
    {ok, Mgr3} = gen_server:start(?MODULE, [], []),
    Req1 = requestor_fun(self(), <<"T1">>, Mgr1, [Mgr2, Mgr3]),
    S1 = spawn(Req1),
    receive Gr1 -> ?assertMatch({granted, S1}, Gr1) end,
    S2 = spawn(Req1),
    S3 = spawn(Req1),
    gen_server:stop(Mgr1),
    Req2 = requestor_fun(self(), <<"T1">>, Mgr2, [Mgr3]),
    S4 = spawn(Req2),
    receive Gr2 -> ?assertMatch({granted, S4}, Gr2) end,
    {ok, Mgr1A} = gen_server:start(?MODULE, [], []),
    Req3 = requestor_fun(self(), <<"T1">>, Mgr1A, [Mgr2, Mgr3]),
    S5 = spawn(Req3),
    receive Rf3 -> ?assertMatch({refused, S5}, Rf3) end,
    S4 ! terminate,
    ?assert(receive _ -> false after 10 -> true end),
        % This also allows time for Mgrs to receive 'DOWN'
    S6 = spawn(Req3),
    receive Gr4 -> ?assertMatch({granted, S6}, Gr4) end,
    lists:foreach(fun(P) -> P ! terminate end, [S1, S2, S3, S6]),
    gen_server:stop(Mgr1A),
    gen_server:stop(Mgr2),
    gen_server:stop(Mgr3).
    

requestor_fun(ReturnPid, Token, Mgr, VerifyList) ->
    fun() ->
        gen_server:cast(Mgr, {request, Token, VerifyList, self()}),
        requestor_receive_loop(ReturnPid)
    end.


requestor_receive_loop(ReturnPid) ->
    receive
        granted ->
            ReturnPid ! {granted, self()},
            requestor_receive_loop(ReturnPid);
        refused ->
            ReturnPid ! {refused, self()},
            requestor_receive_loop(ReturnPid);
        _ ->
            ok
    end.

-endif.