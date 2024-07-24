%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
%% Copyright (c) 2024 Workday, Inc.
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

%% @doc object used for access into the riak system

-module(riak_client).

-export([new/2]).
-export([get/3,get/4,get/5]).
-export([put/2,put/3,put/4,put/5,put/6]).
-export([clone/7]).
-export([copy/5,copy/6,copy/7]).
-export([move/5,move/6,move/7]).
-export([delete/3,delete/4,delete/5,reap/3,reap/4]).
-export([delete_vclock/4,delete_vclock/5,delete_vclock/6]).
-export([list_keys/2,list_keys/3,list_keys/4]).
-export([stream_list_keys/2,stream_list_keys/3,stream_list_keys/4]).
-export([filter_buckets/2]).
-export([filter_keys/3,filter_keys/4]).
-export([list_buckets/1,list_buckets/2,list_buckets/3, list_buckets/4]).
-export([stream_list_buckets/1,stream_list_buckets/2,
         stream_list_buckets/3,stream_list_buckets/4, stream_list_buckets/5]).
-export([get_index/4,get_index/3]).
-export([aae_fold/1, aae_fold/2]).
-export([ttaaefs_fullsync/1, ttaaefs_fullsync/2, ttaaefs_fullsync/3]).
-export([hotbackup/4]).
-export([stream_get_index/4,stream_get_index/3]).
-export([set_bucket/3,get_bucket/2,reset_bucket/2]).
-export([reload_all/2]).
-export([remove_from_cluster/2]).
-export([get_stats/2]).
-export([get_client_id/1]).
-export([for_dialyzer_only_ignore/3]).
-export([ensemble/1]).
-export([fetch/2, push/4]).
-export([membership_request/1, replrtq_reset_all_peers/1, replrtq_reset_all_workercounts/2]).
-export([tictacaae_suspend_node/0, tictacaae_resume_node/0]).
-export([remove_node_from_coverage/0, reset_node_for_coverage/0]).
-export([repair_node/0]).

-compile({no_auto_import,[put/2]}).

-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_TIMEOUT, 60000).
-define(DEFAULT_FOLD_TIMEOUT, 3600000).
-define(DEFAULT_ERRTOL, 0.00003).
-define(DEFAULT_CLONE_TIMEOUT, (?DEFAULT_TIMEOUT * 2)).

%% Declared this way to keep both the compiler and dialyzer happy.
%% Should find a better way, since this annotation is deprecated.
%% @type default_timeout() = 60000

%% TODO: This type needs to be better specified and validated against dependents.
%%
%% The proper type specification SHOULD be:
%% ```
%%  -opaque client_this() :: {node(), client_id()}.
%% '''
%% instead of the two-element list, which isn't a legal type specification.
%%
%% We should then be able to declare
%% ```
%%  -opaque riak_client() :: {?MODULE, client_this()}.
%% '''
%% without running afoul of Dialyzer.
%%
%% It goes without saying that this change will require extensive testing to
%% find (illegal) uses of the list-based structure.
%%
%% Until then declare riak_client() as a term(), which effectively makes it
%% opaque.
%%
%%-opaque client_this() :: {node(), client_id()}.
%%-opaque riak_client() :: {?MODULE, client_this()}.

-type client_id() :: term().
-type riak_client() :: term().

-type req_id() :: term().

-export_type([
    client_id/0, req_id/0, riak_client/0,
    clone_options/0,
    copy_options/0, move_options/0,
    del_option/0, del_options/0,
    get_option/0, get_options/0,
    put_option/0, put_options/0,
    n_val/0, sym_quorum/0,
    pd_val/0, rw_val/0,
    pd_quorum/0, rw_quorum/0
]).

-type n_val() :: pos_integer().                 %% `n_val' value.
-type rw_val() :: pos_integer().                %% `r', `w', or `rw' value.
-type pd_val() :: non_neg_integer().            %% `pr', `pw', or `dw' value.
-type sym_quorum() :: one | quorum | all | default. %% Symbolic quorum option.
-type rw_quorum() :: rw_val() | sym_quorum().   %% `r', `w', or `rw' option.
-type pd_quorum() :: pd_val() | sym_quorum().   %% `pr', `pw', or `dw' option.

-type del_option() :: riak_kv_delete:option() | {timeout, timeout()}.
-type del_options() :: list(del_option()).

-type get_option() :: riak_kv_get_fsm:option().
-type get_options() :: list(get_option()).

-type put_option() ::
    riak_kv_put_fsm:option() | returnhead | {returnhead, boolean()}.
-type put_options() :: list(put_option()).

-type detail_keys() :: list(timing | vnodes| boolean()).
%% All of the detail keys recognized by any operation.

-type clone_options() :: #{
    r               =>  rw_quorum(),    %% Get/Del Read quorum.
    pr              =>  pd_quorum(),    %% Get/Del Primary Read quorum.
    w               =>  rw_quorum(),    %% Put/Del Write quorum.
    pw              =>  pd_quorum(),    %% Put/Del Primary Write quorum.
    dw              =>  pd_quorum(),    %% Put/Del Durable Write quorum.
    rw              =>  rw_quorum(),    %% Del Replicas to delete before returning.
    n_val           =>  n_val(),        %% Get/Put/Del Alternate NVal.
    basic_quorum    =>  boolean(),      %% Get Bail out early on failure.
    sloppy_quorum   =>  boolean(),      %% Get/Put/Del Allow alternate partition(s).
    notfound_ok     =>  boolean(),      %% Get @link riak_kv_get_fsm:options/0}
    asis            =>  boolean(),      %% Put @link riak_kv_put_fsm:options/0}
    sync_on_write   =>  atom(),         %% Put @link riak_kv_put_fsm:options/0}
    timeout         =>  timeout(),      %% Total timeout for all operations.
    recv_timeout    =>  timeout(),      %% Get/Put/Del Receive timeout.
    del_src         =>  boolean(),      %% Move - delete source on successful copy.
    provmeta        =>  store | strip,  %% Add(Replace)/Remove Provenance Metadata.
    returnbody      =>  boolean(),      %% Return the full result object, not just metadata.
    details         =>  detail_keys()   %% Return operation details from phases supporting them.
}.

-type copy_options() :: #{
    r               =>  rw_quorum(),    %% Get/Del Read quorum.
    pr              =>  pd_quorum(),    %% Get/Del Primary Read quorum.
    w               =>  rw_quorum(),    %% Put/Del Write quorum.
    pw              =>  pd_quorum(),    %% Put/Del Primary Write quorum.
    dw              =>  pd_quorum(),    %% Put/Del Durable Write quorum.
    n_val           =>  n_val(),        %% Get/Put/Del Alternate NVal.
    basic_quorum    =>  boolean(),      %% Get Bail out early on failure.
    sloppy_quorum   =>  boolean(),      %% Get/Put/Del Allow alternate partition(s).
    notfound_ok     =>  boolean(),      %% Get @link riak_kv_get_fsm:options/0}
    asis            =>  boolean(),      %% Put @link riak_kv_put_fsm:options/0}
    sync_on_write   =>  atom(),         %% Put @link riak_kv_put_fsm:options/0}
    timeout         =>  timeout(),      %% Total timeout for all operations.
    recv_timeout    =>  timeout(),      %% Get/Put/Del Receive timeout.
    provmeta        =>  store | strip,  %% Add(Replace)/Remove Provenance Metadata.
    returnbody      =>  boolean(),      %% Return the full result object, not just metadata.
    details         =>  detail_keys()   %% Return operation details from phases supporting them.
}.
%% Subset of clone_options() for dialyzer precision.

-type move_options() :: #{
    r               =>  rw_quorum(),    %% Get/Del Read quorum.
    pr              =>  pd_quorum(),    %% Get/Del Primary Read quorum.
    w               =>  rw_quorum(),    %% Put/Del Write quorum.
    pw              =>  pd_quorum(),    %% Put/Del Primary Write quorum.
    dw              =>  pd_quorum(),    %% Put/Del Durable Write quorum.
    rw              =>  rw_quorum(),    %% Del Replicas to delete before returning.
    n_val           =>  n_val(),        %% Get/Put/Del Alternate NVal.
    basic_quorum    =>  boolean(),      %% Get Bail out early on failure.
    sloppy_quorum   =>  boolean(),      %% Get/Put/Del Allow alternate partition(s).
    notfound_ok     =>  boolean(),      %% Get @link riak_kv_get_fsm:options/0}
    asis            =>  boolean(),      %% Put @link riak_kv_put_fsm:options/0}
    sync_on_write   =>  atom(),         %% Put @link riak_kv_put_fsm:options/0}
    timeout         =>  timeout(),      %% Total timeout for all operations.
    recv_timeout    =>  timeout(),      %% Get/Put/Del Receive timeout.
    provmeta        =>  store | strip,  %% Add(Replace)/Remove Provenance Metadata.
    returnbody      =>  boolean(),      %% Return the full result object, not just metadata.
    details         =>  detail_keys()   %% Return operation details from phases supporting them.
}.
%% Subset of clone_options() for dialyzer precision.

%% Just to make specs easier and more consistent.

-type op_label() :: atom().         %% Examples: `clone', `delete', `get', `put'.
-type op_detail_rec() :: {atom(), term()}.
-type op_detail_recs() :: list(op_detail_rec()).
-type op_detail() :: {op_label(), op_detail_recs()}.
-type op_details() :: list(op_detail()).
-type nativetime() :: integer().    %% erlang:monotonic_time() value

-type clone_state() :: #{
    start       :=  nativetime(),   %% when it all began
    timeout     :=  timeout(),      %% overall operation timeout

    %% clone(...) parameters
    opts        :=  clone_options(),
    client      :=  riak_client(),
    srcbucket   :=  riak_object:bucket(),
    srckey      :=  riak_object:key(),
    srcvclock   :=  vclock:vclock() | undefined,
    dstbucket   :=  riak_object:bucket(),
    dstkey      :=  riak_object:key() | undefined,

    %% filtered 'get' options carried forward for re-use
    getopts     =>  get_options(),

    %% the source object, once retrieved
    srcobj      =>  riak_object:riak_object(),

    %% Keys below here are only present if we're collecting some manner of
    %% details, and some only for certain classes of details.

    %% reusable predicate for details per sub-operation
    d_filter    =>  fun((atom()) -> boolean()),

    %% if timing, when the last clone phase interval ended
    lasttime    =>  nativetime(),
    %% accumulated phase details from all operations
    details     =>  op_details()
}.
%% The 'clone' operation is structured much as a state machine.
%% While the state only ever progresses, information is accumulated along the
%% way. This approach is a lot easier to work with that passing a dozen or
%% more parameters through the chain of functions that comprise clone
%% functionality.

-type num_replies() :: non_neg_integer().

-type del_err_reason() ::
    notfound | timeout | too_many_fails |
    {n_val_violation, n_val()} |
    term().
-type del_error() :: {error, del_err_reason()}.
-type del_result() :: ok | del_error().

-type get_err_reason() ::
    notfound | timeout |
    {deleted, vclock:vclock()} |
    {n_val_violation, n_val()} |
    {r_val_unsatisfied, rw_val(), num_replies()} |
    term().
-type get_error() ::
    {error, get_err_reason()} | {error, get_err_reason(), op_details()}.
-type get_result() ::
    {ok, riak_object:riak_object()} |
    {ok, riak_object:riak_object(), op_details()} |
    get_error().

-type put_err_reason() ::
    notfound | timeout | too_many_fails |
    {n_val_violation, n_val()} |
    term().
-type put_error() ::
    {error, put_err_reason()} | {error, put_err_reason(), op_details()}.
-type put_result() ::
    ok |
    {ok, riak_object:riak_object()} |
    {ok, riak_object:riak_object(), op_details()} |
    put_error().

-type clone_err_reason() ::
    destination_not_empty | name_unchanged | src_out_of_date |
    get_err_reason() | put_err_reason().
-type clone_error() ::
    {error, clone_err_reason()} | {error, clone_err_reason(), op_details()}.
-type clone_result() ::
    {ok, riak_object:riak_object()} |
    {ok, riak_object:riak_object(), op_details()} |
    {ok, riak_object:riak_object(), del_err_reason()} |
    {ok, riak_object:riak_object(), del_err_reason(), op_details()} |
    clone_error().

-type copy_err_reason() :: clone_err_reason().
-type copy_error() ::
    {error, copy_err_reason()} | {error, copy_err_reason(), op_details()}.
-type copy_result() ::
    {ok, riak_object:riak_object()} |
    {ok, riak_object:riak_object(), op_details()} |
    copy_error().

-type move_err_reason() :: clone_err_reason().
-type move_error() ::
    {error, move_err_reason()} | {error, move_err_reason(), op_details()}.
-type move_result() ::
    {ok, riak_object:riak_object()} |
    {ok, riak_object:riak_object(), op_details()} |
    {ok, riak_object:riak_object(), del_err_reason()} |
    {ok, riak_object:riak_object(), del_err_reason(), op_details()} |
    move_error().


-spec new(Node :: node(), ClientId :: client_id()) -> riak_client().
%% @doc Return a riak client instance.
new(Node, ClientId) ->
    {?MODULE, [Node, ClientId]}.

-spec get(
    Bucket :: riak_object:bucket(),
    Key :: riak_object:key(),
    This :: riak_client() ) ->  get_result().
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as the default
%%      R-value for the nodes have responded with a value or error.
%% @equiv get(Bucket, Key, [], This)
get(Bucket, Key, {?MODULE, [_Node, _ClientId]} = This) ->
    get(Bucket, Key, [], This).

normal_get(Bucket, Key, Options, {?MODULE, [Node, _ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    case node() of
        Node ->
            riak_kv_get_fsm:start({raw, ReqId, Me}, Bucket, Key, Options);
        _ ->
            %% Still using the deprecated `start_link' alias for `start' here, in
            %% case the remote node is pre-2.2:
            proc_lib:spawn_link(Node, riak_kv_get_fsm, start_link,
                                [{raw, ReqId, Me}, Bucket, Key, Options])
    end,
    %% TODO: Investigate adding a monitor here and eliminating the timeout.
    Timeout = recv_timeout(Options),
    wait_for_reqid(ReqId, Timeout).

consistent_get(Bucket, Key, Options, {?MODULE, [Node, _ClientId]}) ->
    BKey = {Bucket, Key},
    Ensemble = ensemble(BKey),
    Timeout = recv_timeout(Options),
    StartTS = os:timestamp(),
    Result = case riak_ensemble_client:kget(Node, Ensemble, BKey, Timeout) of
                 {error, _}=Err ->
                     Err;
                 {ok, Obj} ->
                     case riak_object:get_value(Obj) of
                         notfound ->
                             {error, notfound};
                         _ ->
                             {ok, Obj}
                     end
             end,
    maybe_update_consistent_stat(Node, consistent_get, Bucket, StartTS, Result),
    Result.

maybe_update_consistent_stat(Node, Stat, Bucket, StartTS, Result) ->
    case node() of
        Node ->
            Duration = timer:now_diff(os:timestamp(), StartTS),
            ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
            ObjSize = case Result of
                          {ok, Obj} ->
                              riak_object:approximate_size(ObjFmt, Obj);
                          _ ->
                              undefined
                      end,
            ok = riak_kv_stat:update({Stat, Bucket, Duration, ObjSize});
        _ ->
            ok
    end.

%% @doc Find the active nodes in the cluster, and return the API IP/Port for
%% those nodes.  Used in peer discovery for nextgenrepl real-time.
-spec membership_request(pb|http) -> list({string(), pos_integer()}).
membership_request(Protocol) ->
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    lists:foldl(membership_request_fun(Protocol), [], UpNodes).

membership_request_fun(Protocol) ->
    fun(Node, Acc) ->
        case rpc:call(Node, application, get_env, [riak_api, Protocol]) of
            {ok, [{IP, Port}]} when is_integer(Port) ->
                [{IP, Port}|Acc];
            _ ->
                Acc
        end
    end.

%% @doc Reset the discovered peers on each up node, returning
%% a list of nodes to which the change was successfully applied
-spec replrtq_reset_all_peers(
    riak_kv_replrtq_snk:queue_name()) -> list(node()).
replrtq_reset_all_peers(QueueName) ->
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    lists:foldl(replrtq_resetpeer_fun(QueueName), [], UpNodes).

replrtq_resetpeer_fun(QueueN) ->
    fun(Node, Acc) ->
        B = rpc:call(Node, riak_kv_replrtq_peer, update_discovery, [QueueN]),
        if B -> [Node|Acc]; true -> Acc end
    end.

%% @doc Reset the worker count and per peer limit on each up node, returning
%% a list of nodes to which the change was successfully applied
-spec replrtq_reset_all_workercounts(
    non_neg_integer(),
    non_neg_integer()) -> list(node()).
replrtq_reset_all_workercounts(WorkerC, PerPeerL) ->
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    FoldFun =
        fun(Node, Acc) ->
            UpdateSuccess =
                rpc:call(
                    Node,
                    riak_kv_replrtq_peer,
                    update_workers,
                    [WorkerC, PerPeerL]),
            if UpdateSuccess -> [Node|Acc]; true -> Acc end
        end,
    lists:foldl(FoldFun, [], UpNodes).


%% @doc Fetch the next item from the replication queue
-spec fetch(riak_kv_replrtq_src:queue_name(), riak_client()) ->
            {ok, riak_object:riak_object()} |
            {ok, queue_empty} |
            {ok, {deleted, vclock:vclock(), riak_object:riak_object()}} |
            {error, timeout} |
            {error, not_yet_implemented} |
            {error, Err :: term()}.
fetch(QueueName, {?MODULE, [Node, _ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    Options = [deletedvclock, {pr, 1}, {r, 1}, {notfound_ok, false}],
    case node() of
        Node ->
            riak_kv_get_fsm:start({raw, ReqId, Me},
                                    queue_name, QueueName, Options);
        _ ->
            %% Still using the deprecated `start_link' alias for `start' here, in
            %% case the remote node is pre-2.2:
            proc_lib:spawn_link(Node, riak_kv_get_fsm, start_link,
                                [{raw, ReqId, Me},
                                queue_name, QueueName, Options])
    end,
    Timeout = recv_timeout(Options),
    wait_for_reqid(ReqId, Timeout).

%% @doc
%% Push a replicated object into Riak
-spec push(riak_object:riak_object()|binary(),
                boolean(), list(), riak_client()) ->
            {ok, erlang:timestamp()} |
            {error, too_many_fails} |
            {error, timeout} |
            {error, {n_val_violation, N::integer()}}.
push(RObjMaybeBin, IsDeleted, _Opts, {?MODULE, [Node, _ClientId]}) ->
    RObj =
        case riak_object:is_robject(RObjMaybeBin) of
            % May get pushed a riak object, or a riak object as a binary, but
            % only want to deal with a riak object
            true ->
                RObjMaybeBin;
            false ->
                riak_object:nextgenrepl_decode(RObjMaybeBin)
        end,
    Bucket = riak_object:bucket(RObj),
    Key = riak_object:key(RObj),
    Me = self(),
    ReqId = mk_reqid(),
    Options = [asis, disable_hooks, {update_last_modified, false},
                {w, 1}, {pw, 1}, {dw, 0}, {node_confirms, 1}],
        % asis - stops the PUT from being re-coordinated
        % disable_hooks - this makes this compatible with previous repl,
        % although this may no longer be necessary (no repl hook to disable)
        % w = 1 - allow for the repl worker to return fast to do more work
        % pw = 1 - in theory we don't need to wait for primaries, but if this
        % node cannot access any primaries it would be good to treat this as an
        % error and punish that peer relationship in the schedule (so that a
        % snk node with access to primaries will manage more of the
        % replication)

    true = riak_kv_util:is_x_deleted(RObj) == IsDeleted,

    case node() of
        Node ->
            riak_kv_put_fsm:start({raw, ReqId, Me}, RObj, Options);
        _ ->
            %% Still using the deprecated `start_link' alias for `start'
            %% here, in case the remote node is pre-2.2:
            proc_lib:spawn_link(Node, riak_kv_put_fsm, start_link,
                                [{raw, ReqId, Me}, RObj, Options])
    end,

    Timeout = recv_timeout(Options),
    R = wait_for_reqid(ReqId, Timeout),
    LMD =
        lists:max(
            lists:map(fun riak_object:get_last_modified/1,
                        riak_object:get_metadatas(RObj))),
    Reply = {R, LMD},

    case IsDeleted of
        true ->
            ReapReqId = mk_reqid(),
            ReapOptions = [{r, 1}],
            case node() of
                Node ->
                    riak_kv_get_fsm:start({raw, ReapReqId, Me},
                                            Bucket, Key, ReapOptions);
                _ ->
                    % Still using the deprecated `start_link' alias for
                    %`start' here, in case the remote node is pre-2.2:
                    proc_lib:spawn_link(Node, riak_kv_get_fsm, start_link,
                                        [{raw, ReapReqId, Me},
                                        Bucket, Key, ReapOptions])
            end,
            wait_for_reqid(ReapReqId, Timeout),
            Reply;
        false ->
            Reply
    end.


-spec get(Bucket :: riak_object:bucket(), Key :: riak_object:key(),
    OptionsOrR :: get_options() | rw_quorum(), This :: riak_client() )
        ->  get_result().
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as R-value for the nodes
%%      have responded with a value or error.
get(Bucket, Key, Options, {?MODULE, [Node, _ClientId]} = This)
        when    (is_binary(Bucket) orelse is_tuple(Bucket))
        andalso is_binary(Key)
        andalso is_list(Options) ->
    case consistent_object(Node, Bucket) of
        true ->
            consistent_get(Bucket, Key, Options, This);
        false ->
            normal_get(Bucket, Key, Options, This);
        {error,_}=Err ->
            Err
    end;
get(Bucket, Key, R, {?MODULE, [_Node, _ClientId]} = This)
        when    (is_binary(Bucket) orelse is_tuple(Bucket))
        andalso is_binary(Key) ->
    get(Bucket, Key, [{r, R}], This).

-spec get(
    Bucket :: riak_object:bucket(), Key :: riak_object:key(), R :: rw_quorum(),
    Timeout :: timeout(), This :: riak_client() )
        ->  get_result().
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as R
%%      nodes have responded with a value or error, or TimeoutMS passes.
%% @equiv get(Bucket, Key, [{r, R}, {timeout, Timeout}], This)
get(Bucket, Key, R, Timeout, {?MODULE, [_Node, _ClientId]} = This) ->
    get(Bucket, Key, [{r, R}, {timeout, Timeout}], This).


-spec put(
    RObj :: riak_object:riak_object(),
    This :: riak_client() ) ->  put_result().
%% @doc Store RObj in the cluster.
%%      Return as soon as the default W value number of nodes for this bucket
%%      nodes have received the request.
%% @equiv put(RObj, [], This)
put(RObj, {?MODULE, [_Node, _ClientId]} = This) ->
    put(RObj, [], This).


normal_put(RObj, Options, {?MODULE, [Node, ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    case ClientId of
        undefined ->
            case node() of
                Node ->
                    riak_kv_put_fsm:start({raw, ReqId, Me}, RObj, Options);
                _ ->
                    %% Still using the deprecated `start_link' alias for `start'
                    %% here, in case the remote node is pre-2.2:
                    proc_lib:spawn_link(Node, riak_kv_put_fsm, start_link,
                                        [{raw, ReqId, Me}, RObj, Options])
            end;
        _ ->
            UpdObj = riak_object:increment_vclock(RObj, ClientId),
            case node() of
                Node ->
                    riak_kv_put_fsm:start_link({raw, ReqId, Me}, UpdObj, [asis|Options]);
                _ ->
                    proc_lib:spawn_link(Node, riak_kv_put_fsm, start_link,
                                        [{raw, ReqId, Me}, RObj, [asis|Options]])
            end
    end,
    %% TODO: Investigate adding a monitor here and eliminating the timeout.
    Timeout = recv_timeout(Options),
    wait_for_reqid(ReqId, Timeout).

consistent_put(RObj, Options, {?MODULE, [Node, _ClientId]}) ->
    Bucket = riak_object:bucket(RObj),
    BKey = {Bucket, riak_object:key(RObj)},
    Ensemble = ensemble(BKey),
    NewObj = riak_object:apply_updates(RObj),
    Timeout = recv_timeout(Options),
    StartTS = os:timestamp(),
    Result = case consistent_put_type(RObj, Options) of
                 update ->
                     riak_ensemble_client:kupdate(Node, Ensemble, BKey, RObj, NewObj, Timeout);
                 put_once ->
                     riak_ensemble_client:kput_once(Node, Ensemble, BKey, NewObj, Timeout)
                %% TODO: Expose client option to explicitly request overwrite
                 %overwrite ->
                     %riak_ensemble_client:kover(Node, Ensemble, BKey, NewObj, Timeout)
             end,
    maybe_update_consistent_stat(Node, consistent_put, Bucket, StartTS, Result),
    ReturnBody = lists:member(returnbody, Options),
    case Result of
        {error, _}=Error ->
            Error;
        {ok, Obj} when ReturnBody ->
            {ok, Obj};
        {ok, _Obj} ->
            ok
    end.

consistent_put_type(RObj, Options) ->
    VClockGiven = (riak_object:vclock(RObj) =/= []),
    IfMissing = lists:member({if_none_match, true}, Options),
    if VClockGiven ->
            update;
       IfMissing ->
            put_once;
       true ->
            %% Defaulting to put_once here for safety.
            %% Our client API makes it too easy to accidently send requests
            %% without a provided vector clock and clobber your data.
            %% overwrite
            %% TODO: Expose client option to explicitly request overwrite
            put_once
    end.

-spec put(
    RObj :: riak_object:riak_object(),
    OptionsOrW :: put_options() | rw_quorum(),
    This :: riak_client() ) ->  put_result().
%% @doc Store RObj in the cluster.
put(RObj, Options, {?MODULE, [Node, _ClientId]} = This) when is_list(Options) ->
    case consistent_object(Node, riak_object:bucket(RObj)) of
        true ->
            consistent_put(RObj, Options, This);
        false ->
            maybe_normal_put(RObj, Options, This);
        {error, _} = Err ->
            Err
    end;
put(RObj, W, {?MODULE, [_Node, _ClientId]} = This) ->
    put(RObj, [{w, W}, {dw, W}], This).

-spec put(
    RObj::riak_object:riak_object(),
    W :: rw_quorum(), DW :: pd_quorum(),
    This :: riak_client() ) ->  put_result().
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend.
%% @equiv put(RObj, [{w, W}, {dw, DW}], This)
put(RObj, W, DW, {?MODULE, [_Node, _ClientId]} = This) ->
    put(RObj, [{w, W}, {dw, DW}], This).

-spec put(
    RObj::riak_object:riak_object(),
    W :: rw_quorum(), DW :: pd_quorum(),
    Timeout :: timeout(),
    This :: riak_client() ) ->  put_result().
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend, or
%%      TimeoutMS passes.
%% @equiv put(RObj, [{w, W}, {dw, DW}, {timeout, Timeout}], This)
put(RObj, W, DW, Timeout, {?MODULE, [_Node, _ClientId]} = This) ->
    put(RObj, [{w, W}, {dw, DW}, {timeout, Timeout}], This).

-spec put(
    RObj::riak_object:riak_object(),
    W :: rw_quorum(), DW :: pd_quorum(),
    Timeout :: timeout(),
    Options :: put_options(),
    This :: riak_client() ) ->  put_result().
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend, or
%%      Timeout passes.
%% @equiv put(RObj, [{w, W}, {dw, DW}, {timeout, Timeout} | Options], This)
put(RObj, W, DW, Timeout, Options, {?MODULE, [_Node, _ClientId]} = This) ->
    %% ToDo: Switch to the map-based version.
    %% So much simpler with proplists:to_map/1, but someone may still want to
    %% use OTP <24
    % OptsMap = proplists:to_map(Options),
    % PutOpts = proplists:from_map(OptsMap#{w => W, dw => DW, timeout => Timeout}),
    PutOpts = lists:foldl(
        fun({K, _V} = Rec, Proplist) ->
            lists:keystore(K, 1, Proplist, Rec)
        end,
        proplists:unfold(Options), [{w, W}, {dw, DW}, {timeout, Timeout}]),
    put(RObj, PutOpts, This).

maybe_normal_put(RObj, Options, {?MODULE, [Node, _ClientId]}=THIS) when is_list(Options) ->
    case write_once(Node, riak_object:bucket(RObj)) of
        true ->
            write_once_put(Node, RObj, Options, THIS);
        false ->
            normal_put(RObj, Options, THIS);
        {error,_}=Err ->
            Err
    end.

write_once_put(Node, RObj, Options, {?MODULE, [_Node, _ClientId]}) when Node =:= node()->
    riak_kv_w1c_worker:put(RObj, Options);
write_once_put(Node, RObj, Options, {?MODULE, [_Node, _ClientId]}) ->
    rpc:call(Node, riak_kv_w1c_worker, put, [RObj, Options]).

-spec delete(
    Bucket :: riak_object:bucket(), Key :: riak_object:key(),
    This :: riak_client() ) ->  del_result().
%% @doc Delete the object at Bucket/Key.  Return a value as soon as RW
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, [], default_timeout(), This)
delete(Bucket, Key, {?MODULE, [_Node, _ClientId]} = This) ->
    delete(Bucket, Key, [], ?DEFAULT_TIMEOUT, This).

-spec delete(
    Bucket :: riak_object:bucket(), Key :: riak_object:key(),
    OptionsOrRW :: del_options() |  rw_quorum(),
    This :: riak_client() ) ->  del_result().
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, Options, default_timeout(), This)
delete(Bucket, Key, Options, {?MODULE, [_Node, _ClientId]} = This)
        when is_list(Options) ->
    delete(Bucket, Key, Options, recv_timeout(Options), This);
delete(Bucket, Key, RW, {?MODULE, [_Node, _ClientId]} = This) ->
    delete(Bucket, Key, [{rw, RW}], ?DEFAULT_TIMEOUT, This).

-spec delete(
    Bucket :: riak_object:bucket(), Key :: riak_object:key(),
    OptionsOrRW :: del_options() | rw_quorum(),
    Timeout :: timeout(), This :: riak_client() ) ->  del_result().
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error, or TimeoutMS passes.
delete(Bucket, Key, Options, Timeout, {?MODULE, [Node, _ClientId]} = This)
        when is_list(Options) ->
    case consistent_object(Node, Bucket) of
        true ->
            consistent_delete(Bucket, Key, Options, Timeout, This);
        false ->
            normal_delete(Bucket, Key, Options, Timeout, This);
        {error, _} = Err ->
            Err
    end;
delete(Bucket, Key, RW, Timeout, {?MODULE, [_Node, _ClientId]} = This) ->
    delete(Bucket, Key, [{rw, RW}], Timeout, This).

normal_delete(Bucket, Key, Options, Timeout, {?MODULE, [Node, ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_delete_sup:start_delete(Node, [ReqId, Bucket, Key, Options, Timeout,
                                           Me, ClientId]),
    RTimeout = recv_timeout(Options),
    wait_for_reqid(ReqId, erlang:min(Timeout, RTimeout)).

consistent_delete(Bucket, Key, Options, _Timeout, {?MODULE, [Node, _ClientId]}) ->
    BKey = {Bucket, Key},
    Ensemble = ensemble(BKey),
    RTimeout = recv_timeout(Options),
    case riak_ensemble_client:kdelete(Node, Ensemble, BKey, RTimeout) of
        {error, _}=Err ->
            Err;
        {ok, Obj} when element(1, Obj) =:= r_object ->
            ok
    end.


-spec clone(
    SrcBucket :: riak_object:bucket(), SrcKey :: riak_object:key(),
    SrcVClock :: vclock:vclock() | undefined,
    DstBucket :: riak_object:bucket(), DstKey :: riak_object:key() | undefined,
    CloneOpts :: clone_options(), Client :: riak_client() )
        -> clone_result().
%%
%% @doc Copy or Move the source Bucket/Key to destination Bucket/Key.
%%
%% If `DstKey' is `undefined' a unique unused key will be generated and
%% returned in the result object.
%%
%% If SrcVClock is not `undefined' and does not match the current vclock of the
%% source record an error will be returned.
%%
%% @param SrcBucket The source `<<bucket>>' or `{<<bucket_type>>, <<bucket>>}'.
%% @param SrcKey The source `<<key>>'.
%% @param SrcVClock The required vclock of the source, or `undefined' to not check.
%% @param DstBucket The destination `<<bucket>>' or `{<<bucket_type>>, <<bucket>>}'.
%% @param DstKey The destination `<<key>>', or `undefined' to generate a random key.
%% @param CloneOpts Options affecting the clone operation and the Get/Put/Delete
%%                  operations it performs.
%% @param Client The opaque client handle.
%%
%% @returns <dl>
%%  <dt>`{ok, RiakObject :: riak_object:riak_object()}'</dt><dd>
%%      Success.
%%      The destination record as written is returned.
%%      The result object contains only metadata unless the `returnbody'
%%      option was given.</dd>
%%  <dt>`{ok, RiakObject, Details :: op_details()}'</dt><dd>
%%      Success.
%%      `RiakObject' is returned as above.
%%      `Details' is returned if the `details' option was given.</dd>
%%  <dt>`{ok, RiakObject, DelFailReason :: del_err_reason()}'</dt><dd>
%%      Partial success.
%%      The copy to the destination record succeeded but the subsequent
%%      deletion of the source record appears to have failed <i>(depending on
%%      options, that may or may not actually be the case)</i>.<br/>
%%      `RiakObject' is returned as above.
%%      `DelFailReason' is the error returned by the delete operation.</dd>
%%  <dt>`{ok, RiakObject, DelFailReason, Details}'</dt><dd>
%%      Partial success.
%%      `RiakObject', `DelFailReason', and `Details' are returned as above.</dd>
%%  <dt>`{error, notfound}'</dt><dd>
%%      The source record was not found.</dd>
%%  <dt>`{error, name_unchanged}'</dt><dd>
%%      The source and destination names are the same.</dd>
%%  <dt>`{error, destination_not_empty}'</dt><dd>
%%      The destination record already exists.</dd>
%%  <dt>`{error, src_out_of_date}'</dt><dd>
%%      The specified SrcVClock qualifier does not match the source record.</dd>
%%  <dt>`{error, Reason :: term()}'</dt><dd>Any other error occurred.</dd>
%%  <dt>`{error, Reason :: term(), Details}'</dt><dd>
%%      Any error may be returned with `Details' as desribed above.</dd>
%% </dl>
clone(SrcBucket, SrcKey, SrcVClock, DstBucket, DstKey,
            #{} = CloneOpts, {?MODULE, [_Node, _ClientId]} = Client)
        when    (erlang:is_binary(SrcBucket) orelse erlang:is_tuple(SrcBucket))
        andalso erlang:is_binary(SrcKey)
        andalso (erlang:is_binary(DstBucket) orelse erlang:is_tuple(DstBucket))
        andalso (erlang:is_binary(DstKey) orelse DstKey =:= undefined) ->

    StartTS = erlang:monotonic_time(),
    %% Everything from here on can assume 'timeout' is present and valid.
    %% We don't check for Timeout < 1 because that'd be silly to use in real
    %% operation, but it *is* used by riak_test => 'verify_clone'.
    Timeout = case CloneOpts of
        #{timeout := Val} ->
            Val;
        _ ->
            ?DEFAULT_CLONE_TIMEOUT
    end,
    State0 = #{
        start       => StartTS,
        timeout     => Timeout,
        client      => Client,
        srcbucket   => SrcBucket,
        srcvclock   => SrcVClock,
        srckey      => SrcKey,
        dstbucket   => DstBucket,
        dstkey      => DstKey
    },
    State1 = clone_init_details(CloneOpts, State0),
    %% Keep the get opts without timeout for the next step.
    GetOpts = clone_get_opts(State1),
    State2 = clone_details(clone_init, State1#{getopts => GetOpts}),
    {GetRes, State3} = clone_get(getsrc, SrcBucket, SrcKey, State2),
    Res = case GetRes of
        {ok, GetObj} ->
            case SrcVClock of
                undefined ->
                    GetRes;
                _ ->
                    case riak_object:vclock(GetObj) of
                        SrcVClock ->
                            GetRes;
                        _ ->
                            {error, src_out_of_date}
                    end
            end;
        _ ->
            GetRes
    end,
    case Res of
        {ok, SrcObj} ->
            clone_chkdst(clone_details(clone_get, State3#{srcobj => SrcObj}));
        _ ->
            clone_return(Res, clone_details(clone_get, State3))
    end.

-spec clone_init_details(OptsIn :: clone_options(), StateIn :: map()) -> map().
%% @hidden Return State with d_filter if appropriate and Opts without details.
clone_init_details(
        #{details := DetailsIn} = OptsIn, #{start := StartTS} = StateIn) ->
    OptsOut = maps:remove(details, OptsIn),
    State = StateIn#{opts => OptsOut},
    case DetailsIn of
        [_|_] = D1 ->
            %% Filter details to either 'true' or a list of unique affirmative
            %% atom() keys. Any istance of 'true' resets the predicate to
            %% "collect everything", otherwise only specified keys. 'false' is
            %% a valid key in at least one spec, but it's not clear whether it
            %% should negate all other keys so I've chosen to ignore it.
            Fun = fun
                (_, true = True) ->
                    True;
                (true = True, _Acc) ->
                    True;
                (false, Acc) ->
                    Acc;
                (Key, Acc) when erlang:is_atom(Key) ->
                    case lists:member(Key, Acc) of
                        true ->
                            Acc;
                        _ ->
                            [Key | Acc]
                    end;
                (_, Acc) ->
                    Acc
            end,
            %% All that remains of the original 'details' list is a predicate
            %% function for filtering sub-operations' detail options.
            case lists:foldl(Fun, [], D1) of
                true ->
                    State#{
                        d_filter => fun(_) -> true end,
                        details => [], lasttime => StartTS
                    };
                [_|_] = D2 ->
                    Pred = fun(DKey) -> lists:member(DKey, D2) end,
                    case Pred(timing) of
                        true ->
                            State#{
                                d_filter => Pred, details => [],
                                lasttime => StartTS
                            };
                        _ ->
                            State#{d_filter => Pred, details => []}
                    end;
                _ ->
                    %% Nothing made it through the filter, no details will
                    %% be collected.
                    State
            end;
        _ ->
            State
    end;
clone_init_details(OptsIn, StateIn) ->
    StateIn#{opts => OptsIn}.

-spec clone_get(
    GetLabel :: atom(),
    Bucket :: riak_object:bucket(),
    Key :: riak_object:key(),
    State :: clone_state() )
        -> {{atom(), term()}, clone_state()}.
%% @hidden Get operation surrogate, because we call Get 2+ times.
%% Returns {Result, NewState}
%% where:
%%  Result is {ok, RiakObject} or {error, Reason}.
%%  NewState is State updated with Get Details, if any.
clone_get(OpLabel, Bucket, Key, #{getopts := Opts, client := Client} = State) ->
    Timeout = clone_remain(State),
    case Timeout =:= infinity orelse Timeout > 0 of
        true ->
            GetOpts = [{timeout, Timeout} | Opts],
            GetRes = get(Bucket, Key, GetOpts, Client),
            case GetRes of
                {_OkErr, _ObjReason} ->
                    {GetRes, State};
                {OkErr, ObjReason, Details} ->
                    {{OkErr, ObjReason},
                        clone_details(OpLabel, Details, State)}
            end;
        _ ->
            {{error, timeout}, State}
    end.

-spec clone_remain(State :: clone_state()) -> timeout().
clone_remain(#{timeout := infinity}) ->
    infinity;
clone_remain(State) ->
    clone_remain(erlang:monotonic_time(), State).

%% At present clone_remain/2 is only called from clone_remain/1, so dialyzer
%% accurately warns that 'timeout' can never be 'infinity'. We keep the
%% pattern in place should it ever be called trough a different path.
-dialyzer({no_match, clone_remain/2}).

-spec clone_remain(
    NowNative :: nativetime(), State :: clone_state()) -> timeout().
clone_remain(_NowNative, #{timeout := infinity}) ->
    infinity;
clone_remain(NowNative, #{start := StartNative, timeout := Timeout}) ->
    Timeout - erlang:convert_time_unit(
        (NowNative - StartNative), native, millisecond).

-spec clone_details(Label :: atom(), State :: clone_state() )
        -> clone_state().
%% @hidden If timings are being recorded, causes a new phase duration record
%% to be added to State's details.
clone_details(Label, #{lasttime := _} = State) ->
    clone_details(Label, erlang:monotonic_time(), State);
clone_details(_label, State) ->
    State.

-spec clone_details(
    Label :: atom(), NowNativeOrRecord :: nativetime() | op_detail_recs(),
    State :: clone_state() ) -> clone_state().
%% @hidden If details are being recorded, adds the appropriate record as Label.
%% If NowNativeOrRecord is an integer AND timings are being recorded, it is
%% treated as the current native timestamp and a new duration record is
%% created; otherwise it is assumed to be a list of informational records to
%% be recorded as-is.
clone_details(
    Label, NowNative, #{details := Details, lasttime := LastNative} = State)
        when erlang:is_integer(NowNative) ->
    Record = {Label, duration_detail_rec(NowNative - LastNative)},
    State#{details := [Record | Details], lasttime := NowNative};
clone_details(Label, Record, #{details := Details} = State) ->
    State#{details := [{Label, Record} | Details]};
clone_details(_Label, _Info, State) ->
    State.

-spec duration_detail_rec(NativeDuration :: nativetime()) -> op_detail_recs().
duration_detail_rec(NativeDuration) ->
    MicroSecs = erlang:convert_time_unit(NativeDuration, native, microsecond),
    [{usec, MicroSecs}].

-spec clone_chkdst(State :: clone_state()) -> clone_result().
%% @hidden Ensures that the destination record does not exist,
%% generating a unique key if needed.
clone_chkdst(#{dstkey := undefined} = StateIn) ->
    {GenRes, State} = clone_genkey(StateIn),
    StateOut = clone_details(clone_genkey, State),
    case GenRes of
        ok ->
            clone_srcobj(StateOut);
        _ ->
            clone_return(GenRes, StateOut)
    end;
clone_chkdst(#{dstbucket := Bucket, dstkey := Key} = StateIn) ->
    {GetRes, State} = clone_get(getdst, Bucket, Key, StateIn),
    StateOut = clone_details(clone_chkdst, State),
    case GetRes of
        {error, notfound} ->
            clone_srcobj(StateOut);
        {ok, _} ->
            clone_return({error, destination_not_empty}, StateOut);
        _ ->
            clone_return(GetRes, StateOut)
    end.

-spec clone_genkey(clone_state()) -> {ok | {error, term()}, clone_state()}.
%% @hidden Genertaes a new, unique (unused) destination key.
clone_genkey(#{dstbucket := Bucket} = StateIn) ->
    Key = erlang:list_to_binary(riak_core_util:unique_id_62()),
    {GetRes, State} = clone_get(chkkey, Bucket, Key, StateIn),
    case GetRes of
        {error, notfound} ->
            {ok, State#{dstkey := Key}};
        {ok, _} ->
            clone_genkey(State);
        _ ->
            {GetRes, State}
    end.

-spec clone_srcobj(State :: clone_state()) -> clone_result().
%% @hidden Performs the actual copy operation.
clone_srcobj(#{dstbucket := Bucket, dstkey := Key,
        srcobj := SrcObj, client := Client} = State) ->
    %% all checks completed
    Timeout = clone_remain(State),
    case Timeout =:= infinity orelse Timeout > 0 of
        true ->
            case riak_object:clone(SrcObj, Bucket, Key) of
                {ok, DstObj} ->
                    PutOpts = clone_put_opts(State),
                    %% ToDo: Implement 'provmeta' actions here.
                    %% 'returnhead' would be ideal here; instead, simulate it.
                    {MetaOnly, Opts} = case
                            proplists:get_bool(returnbody, PutOpts) of
                        true ->
                            {false, PutOpts};
                        _ ->
                            {true, lists:keystore(
                                returnbody, 1, PutOpts, {returnbody, true})}
                    end,
                    {PutRes, State1} = case put(
                            DstObj, [{timeout, Timeout} | Opts], Client) of
                        {_, _} = R ->
                            {R, State};
                        {OkOrErr, ObjOrReason, Details} ->
                            {{OkOrErr, ObjOrReason},
                                clone_details(putdst, Details, State)}
                    end,
                    CloneRes = case PutRes of
                        {ok, RObj} when MetaOnly ->
                            Contents = riak_object:get_contents(RObj),
                            MetaContents = [{MD, <<>>} || {MD, _} <- Contents],
                            {ok, riak_object:set_contents(RObj, MetaContents)};
                        _ ->
                            PutRes
                    end,
                    State2 = clone_details(clone_srcobj, State1),
                    case CloneRes of
                        {ok, _} ->
                            clone_finish(CloneRes, State2);
                        _ ->
                            clone_return(CloneRes, State2)
                    end;
                ObjError ->
                    clone_return(ObjError, clone_details(clone_srcobj, State))
            end;
        _ ->
            clone_return({error, timeout}, clone_details(clone_srcobj, State))
    end.

-spec clone_finish(
    CloneRes :: {ok, riak_object:riak_object()} | {error, term()},
    State :: clone_state() )
        -> clone_result().
%% @hidden Finalizes the clone, possibly deleting the source record.
clone_finish({_Ok, ResObj} = CloneRes, #{opts := #{del_src := true},
        srcbucket := Bucket, srckey := Key, srcvclock := SrcVClock,
        client := Client} = State) ->
    Timeout = clone_remain(State),
    case Timeout =:= infinity orelse Timeout > 0 of
        true ->
            DelOpts = [{timeout, Timeout} | clone_del_opts(State)],
            DelRes = case SrcVClock of
                undefined ->
                    delete(Bucket, Key, DelOpts, Client);
                _ ->
                    delete_vclock(Bucket, Key, SrcVClock, DelOpts, Client)
            end,
            StateOut = clone_details(clone_delsrc, State),
            case DelRes of
                ok ->
                    clone_return(CloneRes, StateOut);
                {error, DelFail} ->
                    clone_return({ok, ResObj, DelFail}, StateOut)
            end;
        _ ->
            clone_return({ok, ResObj, timeout},
                clone_details(clone_delsrc, State))
    end;
clone_finish(CloneRes, State) ->
    clone_return(CloneRes, State).

-spec clone_return(
    CloneRes :: {ok, riak_object:riak_object()} |
                {ok, riak_object:riak_object(), term()} |
                {error, term()},
    State :: clone_state() ) -> clone_result().
%% @hidden Returns the clone result, possibly with accumulated details.
clone_return(CloneRes, #{lasttime := _, start := StartTS} = State) ->
    %% Only collect clone duration if 'lasttime' is present.
    DurationRec = duration_detail_rec(erlang:monotonic_time() - StartTS),
    #{details := DetailsAcc} = clone_details(clone, DurationRec, State),
    %% The compiler should optimize this away and jump to the next head's
    %% identical body ...
    Details = lists:reverse(DetailsAcc),
    case CloneRes of
        {OkOrError, ObjOrReason} ->
            {OkOrError, ObjOrReason, Details};
        {ok, DstObj, DelFail} ->
            {ok, DstObj, DelFail, Details}
    end;
clone_return(CloneRes, #{details := [_|_] = DetailsAcc}) ->
    Details = lists:reverse(DetailsAcc),
    case CloneRes of
        {OkOrError, ObjOrReason} ->
            {OkOrError, ObjOrReason, Details};
        {ok, DstObj, DelFail} ->
            {ok, DstObj, DelFail, Details}
    end;
clone_return(CloneRes, _State) ->
    CloneRes.

-spec clone_del_opts(State :: clone_state()) -> del_options().
%% @hidden Options in riak_kv_delete:option() less timeout.
%%clone_del_opts(#{opts := CloneOpts}) ->
clone_del_opts(State) ->
    Keys = [
        r, pr, rw, w, dw, pw, n_val,
        sloppy_quorum, recv_timeout
    ],
    maps:to_list(maps:with(Keys, maps:get(opts, State))).

-spec clone_get_opts(State :: clone_state()) -> get_options().
%% @hidden Options from riak_kv_get_fsm:option() less timeout.
clone_get_opts(#{opts := CloneOpts} = State) ->
    Keys = [
        r, pr, n_val,
        notfound_ok, basic_quorum, sloppy_quorum,
        recv_timeout, crdt_op
    ],
    Opts1 = maps:with(Keys, CloneOpts),
    Opts2 = clone_detail_opts([timing, vnodes], Opts1, State),
    maps:to_list(Opts2).

-spec clone_put_opts(State :: clone_state()) -> put_options().
%% @hidden Options from riak_kv_put_fsm:option() less timeout.
clone_put_opts(#{opts := CloneOpts} = State) ->
    Keys = [
        pw, w, dw, n_val,
        returnbody,
        asis, disable_hooks, sync_on_write,
        sloppy_quorum,
        retry_put_coordinator_failure, mbox_check,
        recv_timeout,
        counter_op, crdt_op
    ],
    Opts1 = maps:with(Keys, CloneOpts),
    Opts2 = clone_detail_opts([timing], Opts1, State),
    maps:to_list(Opts2).

-spec clone_detail_opts(
    Keys :: list(atom()), Opts :: map(), State :: clone_state() )
        -> map().
%% @hidden Figure out which, if any, of the specified sub-operation Keys is
%% selected by the 'details' option.
%% Returns the resulting sub-operation options, possibly modified.
clone_detail_opts(Keys, Opts, #{d_filter := Filter}) ->
    case lists:filter(Filter, Keys) of
        [_|_] = Match ->
            Opts#{details => Match};
        _ ->
            Opts
    end;
clone_detail_opts(_Keys, Opts, _State) ->
    Opts.


-spec copy(
    SrcBucket :: riak_object:bucket(), SrcKey :: riak_object:key(),
    DstBucket :: riak_object:bucket(), DstKey :: riak_object:key(),
    Client :: riak_client() ) -> copy_result().
%%
%% @doc Create a new copy of the source Bucket/Key as destination Bucket/Key.
%%
%% @equiv copy(SrcBucket, SrcKey, undefined, DstBucket, DstKey, #{}, Client)
%%
copy(SrcBucket, SrcKey, DstBucket, DstKey, Client) ->
    copy(SrcBucket, SrcKey, undefined, DstBucket, DstKey, #{}, Client).

-spec copy(
    SrcBucket :: riak_object:bucket(), SrcKey :: riak_object:key(),
    DstBucket :: riak_object:bucket(), DstKey :: riak_object:key(),
    CopyOpts :: copy_options(), Client :: riak_client() )
        -> copy_result().
%%
%% @doc Create a new copy of the source Bucket/Key as destination Bucket/Key.
%%
%% @equiv copy(SrcBucket, SrcKey, undefined,
%%          DstBucket, DstKey, CopyOpts, Client)
%%
copy(SrcBucket, SrcKey, DstBucket, DstKey, CopyOpts, Client) ->
    copy(SrcBucket, SrcKey, undefined,
        DstBucket, DstKey, CopyOpts, Client).

-spec copy(
    SrcBucket :: riak_object:bucket(), SrcKey :: riak_object:key(),
    SrcVClock :: vclock:vclock() | undefined,
    DstBucket :: riak_object:bucket(), DstKey :: riak_object:key(),
    CopyOpts :: copy_options(), Client :: riak_client() )
        -> copy_result().
%%
%% @doc Create a new copy of the source Bucket/Key as destination Bucket/Key.
%%
%% If `DstKey' is `undefined' a unique unused key will be generated and
%% returned in the result object.
%%
%% If SrcVClock is not `undefined' and does not match the current vclock of the
%% source record an error will be returned.
%%
%% @param SrcBucket The source `<<bucket>>' or `{<<bucket_type>>, <<bucket>>}'.
%% @param SrcKey The source `<<key>>'.
%% @param SrcVClock The required vclock of the source, or `undefined' to not check.
%% @param DstBucket The destination `<<bucket>>' or `{<<bucket_type>>, <<bucket>>}'.
%% @param DstKey The destination `<<key>>', or `undefined' to generate a random key.
%% @param CopyOpts Options affecting the copy operation and the Get/Put
%%          operations it performs.
%% @param Client The opaque client handle.
%%
%% @returns <dl>
%%  <dt>`{ok, RiakObject :: riak_object:riakc_obj()}'</dt><dd>
%%      Success.
%%      The destination record as written is returned.
%%      The result object contains only metadata unless the `returnbody'
%%      option was given.</dd>
%%  <dt>`{ok, RiakObject, Details :: op_details()}'</dt><dd>
%%      Success.
%%      `RiakObject' is returned as above.
%%      `Details' is returned if the `details' option was given.</dd>
%%  <dt>`{error, notfound}'</dt><dd>
%%      The source record was not found.</dd>
%%  <dt>`{error, name_unchanged}'</dt><dd>
%%      The source and destination names are the same.</dd>
%%  <dt>`{error, destination_not_empty}'</dt><dd>
%%      The destination record already exists.</dd>
%%  <dt>`{error, src_out_of_date}'</dt><dd>
%%      The specified SrcVClock qualifier does not match the source record.</dd>
%%  <dt>`{error, Reason :: term()}'</dt><dd>Any other error occurred.</dd>
%%  <dt>`{error, Reason :: term(), Details}'</dt><dd>
%%      Any error may be returned with `Details' as desribed above.</dd>
%% </dl>
copy(Bucket, Key, _SrcVClock, Bucket, Key, _CopyOpts, _Client) ->
    {error, name_unchanged};
copy(SrcBucket, SrcKey, SrcVClock, DstBucket, DstKey, CopyOpts, Client) ->
    %% CopyOpts should not contain the 'del_src' key, but make sure.
    clone(SrcBucket, SrcKey, SrcVClock,
        DstBucket, DstKey, maps:remove(del_src, CopyOpts), Client).


-spec move(
    SrcBucket :: riak_object:bucket(), SrcKey :: riak_object:key(),
    DstBucket :: riak_object:bucket(), DstKey :: riak_object:key(),
    Client :: riak_client() ) -> move_result().
%%
%% @doc Move the source Bucket/Key to destination Bucket/Key.
%%
%% @equiv move(SrcBucket, SrcKey, undefined, DstBucket, DstKey, #{}, Client)
%%
move(SrcBucket, SrcKey, DstBucket, DstKey, Client) ->
    move(SrcBucket, SrcKey, undefined, DstBucket, DstKey, #{}, Client).

-spec move(
    SrcBucket :: riak_object:bucket(), SrcKey :: riak_object:key(),
    DstBucket :: riak_object:bucket(), DstKey :: riak_object:key(),
    MoveOpts :: move_options(), Client :: riak_client() )
        -> move_result().
%%
%% @doc Move the source Bucket/Key to destination Bucket/Key.
%%
%% @equiv move(SrcBucket, SrcKey, undefined,
%%          DstBucket, DstKey, MoveOpts, Client)
%%
move(SrcBucket, SrcKey, DstBucket, DstKey, MoveOpts, Client) ->
    move(SrcBucket, SrcKey, undefined,
        DstBucket, DstKey, MoveOpts, Client).

-spec move(
    SrcBucket :: riak_object:bucket(), SrcKey :: riak_object:key(),
    SrcVClock :: vclock:vclock() | undefined,
    DstBucket :: riak_object:bucket(), DstKey :: riak_object:key(),
    MoveOpts :: move_options(), Client :: riak_client() )
        -> move_result().
%%
%% @doc Move the source Bucket/Key to destination Bucket/Key.
%%
%% If `DstKey' is `undefined' a unique unused key will be generated and
%% returned in the result object.
%%
%% If SrcVClock is not `undefined' and does not match the current vclock of the
%% source record an error will be returned.
%%
%% @param SrcBucket The source `<<bucket>>' or `{<<bucket_type>>, <<bucket>>}'.
%% @param SrcKey The source `<<key>>'.
%% @param SrcVClock The required vclock of the source, or `undefined' to not check.
%% @param DstBucket The destination `<<bucket>>' or `{<<bucket_type>>, <<bucket>>}'.
%% @param DstKey The destination `<<key>>', or `undefined' to generate a random key.
%% @param MoveOpts Options affecting the move operation and the Get/Put/Delete
%%          operations it performs.
%% @param Client The opaque client handle.
%%
%% @returns <dl>
%%  <dt>`{ok, RiakObject :: riak_object:riak_object()}'</dt><dd>
%%      Success.
%%      The destination record as written is returned.
%%      The result object contains only metadata unless the `returnbody'
%%      option was given.</dd>
%%  <dt>`{ok, RiakObject, Details :: op_details()}'</dt><dd>
%%      Success.
%%      `RiakObject' is returned as above.
%%      `Details' is returned if the `details' option was given.</dd>
%%  <dt>`{ok, RiakObject, DelFailReason :: del_err_reason()}'</dt><dd>
%%      Partial success.
%%      The copy to the destination record succeeded but the subsequent
%%      deletion of the source record appears to have failed <i>(depending on
%%      options, that may or may not actually be the case)</i>.<br/>
%%      `RiakObject' is returned as above.
%%      `DelFailReason' is the error returned by the delete operation.</dd>
%%  <dt>`{ok, RiakObject, DelFailReason, Details}'</dt><dd>
%%      Partial success.
%%      `RiakObject', `DelFailReason', and `Details' are returned as above.</dd>
%%  <dt>`{error, notfound}'</dt><dd>
%%      The source record was not found.</dd>
%%  <dt>`{error, name_unchanged}'</dt><dd>
%%      The source and destination names are the same.</dd>
%%  <dt>`{error, destination_not_empty}'</dt><dd>
%%      The destination record already exists.</dd>
%%  <dt>`{error, src_out_of_date}'</dt><dd>
%%      The specified SrcVClock qualifier does not match the source record.</dd>
%%  <dt>`{error, Reason :: term()}'</dt><dd>Any other error occurred.</dd>
%%  <dt>`{error, Reason :: term(), Details}'</dt><dd>
%%      Any error may be returned with `Details' as desribed above.</dd>
%% </dl>
move(SrcBucket, SrcKey, SrcVClock, DstBucket, DstKey, MoveOpts, Client) ->
    clone(SrcBucket, SrcKey, SrcVClock,
        DstBucket, DstKey, MoveOpts#{del_src => true}, Client).


-spec reap(riak_object:bucket(), riak_object:key(), riak_client())
                                                                -> boolean().
reap(Bucket, Key, Client) ->
    case normal_get(Bucket, Key, [deletedvclock], Client) of
        {error, {deleted, TombstoneVClock}} ->
            DeleteHash = riak_object:delete_hash(TombstoneVClock),
            reap(Bucket, Key, DeleteHash, Client);
        _Unexpected ->
            false
    end.

-spec reap(riak_object:bucket(), riak_object:key(), pos_integer(),
                                                riak_client()) -> boolean().
reap(Bucket, Key, DeleteHash, {?MODULE, [Node, _ClientId]}) ->
    case node() of
        Node ->
            riak_kv_reaper:direct_reap({{Bucket, Key}, DeleteHash});
        _ ->
            riak_core_util:safe_rpc(Node, riak_kv_reaper, direct_reap,
                                    [{{Bucket, Key}, DeleteHash}])
    end.

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock:vclock(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete_vclock(Bucket,Key,VClock,{?MODULE, [_Node, _ClientId]}=THIS) ->
    delete_vclock(Bucket,Key,VClock,[{rw,default}],?DEFAULT_TIMEOUT,THIS).

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock:vclock(),
%%                     RW :: integer(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete_vclock(Bucket,Key,VClock,Options,{?MODULE, [_Node, _ClientId]}=THIS) when is_list(Options) ->
    delete_vclock(Bucket,Key,VClock,Options,recv_timeout(Options),THIS);
delete_vclock(Bucket,Key,VClock,RW,{?MODULE, [_Node, _ClientId]}=THIS) ->
    delete_vclock(Bucket,Key,VClock,[{rw, RW}],?DEFAULT_TIMEOUT,THIS).

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock:vclock(), RW :: integer(),
%%           TimeoutMillisecs :: integer(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
delete_vclock(Bucket,Key,VClock,Options,Timeout,{?MODULE, [Node, _ClientId]}=THIS) when is_list(Options) ->
    case consistent_object(Node, Bucket) of
        true ->
            consistent_delete_vclock(Bucket, Key, VClock, Options, Timeout, THIS);
        false ->
            normal_delete_vclock(Bucket, Key, VClock, Options, Timeout, THIS);
        {error,_}=Err ->
            Err
    end;
delete_vclock(Bucket,Key,VClock,RW,Timeout,{?MODULE, [_Node, _ClientId]}=THIS) ->
    delete_vclock(Bucket,Key,VClock,[{rw, RW}],Timeout,THIS).

normal_delete_vclock(Bucket, Key, VClock, Options, Timeout, {?MODULE, [Node, ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_delete_sup:start_delete(Node, [ReqId, Bucket, Key, Options, Timeout,
                                           Me, ClientId, VClock]),
    RTimeout = recv_timeout(Options),
    wait_for_reqid(ReqId, erlang:min(Timeout, RTimeout)).

consistent_delete_vclock(Bucket, Key, VClock, Options, _Timeout, {?MODULE, [Node, _ClientId]}) ->
    BKey = {Bucket, Key},
    Ensemble = ensemble(BKey),
    Current = riak_object:set_vclock(riak_object:new(Bucket, Key, <<>>),
                                     VClock),
    RTimeout = recv_timeout(Options),
    case riak_ensemble_client:ksafe_delete(Node, Ensemble, BKey, Current, RTimeout) of
        {error, _}=Err ->
            Err;
        {ok, Obj} when element(1, Obj) =:= r_object ->
            ok
    end.

%% @spec list_keys(riak_object:bucket(), riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%% @equiv list_keys(Bucket, default_timeout()*8)
list_keys(Bucket, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_keys(Bucket, ?DEFAULT_TIMEOUT*8, THIS).

%% @spec list_keys(riak_object:bucket(), TimeoutMillisecs :: integer(), riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
list_keys(Bucket, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_keys(Bucket, none, Timeout, THIS).

%% @spec list_keys(riak_object:bucket(), Filter :: term(),
%% TimeoutMillisecs :: integer(), riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
list_keys(Bucket, Filter, Timeout0, {?MODULE, [Node, _ClientId]}) ->
    Timeout =
        case Timeout0 of
            T when is_integer(T) -> T;
            _ -> ?DEFAULT_TIMEOUT*8
        end,
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_keys_fsm_sup:start_keys_fsm(Node, [{raw, ReqId, Me}, [Bucket, Filter, Timeout]]),
    wait_for_listkeys(ReqId).

stream_list_keys(Bucket, {?MODULE, [_Node, _ClientId]}=THIS) ->
    stream_list_keys(Bucket, ?DEFAULT_TIMEOUT, THIS).

stream_list_keys(Bucket, undefined, {?MODULE, [_Node, _ClientId]}=THIS) ->
    stream_list_keys(Bucket, ?DEFAULT_TIMEOUT, THIS);
stream_list_keys(Bucket, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
    Me = self(),
    stream_list_keys(Bucket, Timeout, Me, THIS).

%% @spec stream_list_keys(riak_object:bucket(),
%%                        TimeoutMillisecs :: integer(),
%%                        Client :: pid(),
%%                        riak_client()) ->
%%       {ok, ReqId :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%%      The list will not be returned directly, but will be sent
%%      to Client in a sequence of {ReqId, {keys,Keys}} messages
%%      and a final {ReqId, done} message.
%%      None of the Keys lists will be larger than the number of
%%      keys in Bucket on any single vnode.
stream_list_keys(Input, Timeout, Client, {?MODULE, [Node, _ClientId]}) when is_pid(Client) ->
    ReqId = mk_reqid(),
    case Input of
        %% buckets with bucket types are also a 2-tuple, so be careful not to
        %% treat the bucket type like a filter
        {Bucket, FilterInput} when not is_binary(FilterInput) ->
            case riak_kv_mapred_filters:build_filter(FilterInput) of
                {error, _Error} ->
                    {error, _Error};
                {ok, FilterExprs} ->
                    riak_kv_keys_fsm_sup:start_keys_fsm(Node,
                                                        [{raw,
                                                          ReqId,
                                                          Client},
                                                         [Bucket,
                                                          FilterExprs,
                                                          Timeout]]),
                    {ok, ReqId}
            end;
        Bucket ->
            riak_kv_keys_fsm_sup:start_keys_fsm(Node,
                                                [{raw, ReqId, Client},
                                                 [Bucket,
                                                  none,
                                                  Timeout]]),
            {ok, ReqId}
    end.

%% @spec filter_keys(riak_object:bucket(), Fun :: function(), riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket,
%%      filtered at the vnode according to Fun, via lists:filter.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%% @equiv filter_keys(Bucket, Fun, default_timeout())
filter_keys(Bucket, Fun, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_keys(Bucket, Fun, ?DEFAULT_TIMEOUT, THIS).

%% @spec filter_keys(riak_object:bucket(), Fun :: function(), TimeoutMillisecs :: integer(),
%%                   riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket,
%%      filtered at the vnode according to Fun, via lists:filter.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
filter_keys(Bucket, Fun, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
            list_keys(Bucket, Fun, Timeout, THIS).

%% @spec list_buckets(riak_client()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
%% @equiv list_buckets(default_timeout())
list_buckets({?MODULE, [_Node, _ClientId]}=THIS) ->
    list_buckets(none, ?DEFAULT_TIMEOUT, <<"default">>, THIS).

%% @spec list_buckets(timeout(), riak_client()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
%% @equiv list_buckets(default_timeout())
list_buckets(undefined, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_buckets(none, ?DEFAULT_TIMEOUT*8, <<"default">>, THIS);
list_buckets(Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_buckets(none, Timeout, <<"default">>, THIS).

%% @spec list_buckets(TimeoutMillisecs :: integer(), Filter :: term(),
%% riak_client()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
list_buckets(Filter, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_buckets(Filter, Timeout, <<"default">>, THIS).

list_buckets(Filter, Timeout, Type, {?MODULE, [Node, _ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    {ok, _Pid} = riak_kv_buckets_fsm_sup:start_buckets_fsm(Node,
                                                           [{raw, ReqId, Me},
                                                            [Filter, Timeout,
                                                             false, Type]]),
    wait_for_listbuckets(ReqId).

%% @spec filter_buckets(Fun :: function(), riak_client()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Return a list of filtered buckets.
filter_buckets(Fun, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_buckets(Fun, ?DEFAULT_TIMEOUT, THIS).

stream_list_buckets({?MODULE, [_Node, _ClientId]}=THIS) ->
    stream_list_buckets(none, ?DEFAULT_TIMEOUT, THIS).

stream_list_buckets(undefined, {?MODULE, [_Node, _ClientId]}=THIS) ->
    stream_list_buckets(none, ?DEFAULT_TIMEOUT, THIS);
stream_list_buckets(Timeout, {?MODULE, [_Node, _ClientId]}=THIS)
  when is_integer(Timeout) ->
    stream_list_buckets(none, Timeout, THIS);
stream_list_buckets(Filter, {?MODULE, [_Node, _ClientId]}=THIS)
  when is_function(Filter) ->
    stream_list_buckets(Filter, ?DEFAULT_TIMEOUT, THIS).

stream_list_buckets(Filter, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
    Me = self(),
    stream_list_buckets(Filter, Timeout, Me, <<"default">>, THIS).

%% @spec stream_list_buckets(FilterFun :: fun(),
%%                           TimeoutMillisecs :: integer(),
%%                           Client :: pid(),
%%                           riak_client()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
stream_list_buckets(Filter, Timeout, Client,
                    {?MODULE, [_Node, _ClientId]}=THIS) when is_pid(Client) ->
    stream_list_buckets(Filter, Timeout, Client, <<"default">>, THIS);
stream_list_buckets(Filter, Timeout, Type,
                    {?MODULE, [_Node, _ClientId]}=THIS) ->
    Me = self(),
    stream_list_buckets(Filter, Timeout, Me, Type, THIS).

stream_list_buckets(Filter, Timeout, Client, Type,
                    {?MODULE, [Node, _ClientId]}) ->
    ReqId = mk_reqid(),
    {ok, _Pid} = riak_kv_buckets_fsm_sup:start_buckets_fsm(Node,
                                                           [{raw, ReqId,
                                                             Client},
                                                            [Filter, Timeout,
                                                             true, Type]]),
    {ok, ReqId}.


-spec aae_fold(riak_kv_clusteraae_fsm:query_definition())
                    -> {ok, any()}|{error, timeout}|{error, Err :: term()}.
aae_fold(Query) ->
    aae_fold(Query, riak_client:new(node(), adhoc_aaefold)).

%% @doc
%%
%% Run a cluster-wide AAE query - which can either access cached AAE
%% data across the cluster, or fold over ranges of the AAE store
%% (which in the case of Leveled can be the native AAE store.
-spec aae_fold(riak_kv_clusteraae_fsm:query_definition(), riak_client())
                    -> {ok, any()}|{error, timeout}|{error, Err :: term()}.
aae_fold(Query, {?MODULE, [Node, _ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    TimeOut =
        app_helper:get_env(
            riak_kv, riak_client_aaefold_timeout, ?DEFAULT_FOLD_TIMEOUT),
    Q0 = riak_kv_clusteraae_fsm:convert_fold(Query),
    case riak_kv_clusteraae_fsm:is_valid_fold(Q0) of
        true ->
            riak_kv_clusteraae_fsm_sup:start_clusteraae_fsm(
                Node, [{raw, ReqId, Me}, [Q0, TimeOut]]),
            wait_for_fold_results(ReqId, TimeOut);
        false ->
            {error, "Invalid AAE fold definition"}
    end.


-spec ttaaefs_fullsync(riak_kv_ttaaefs_manager:work_item()) -> ok.
ttaaefs_fullsync(WorkItem) ->
    ttaaefs_fullsync(WorkItem, 900).

%% @doc
%% Prompt a full-sync based on the current configuration, and using either
%% - null_check (a no op)
%% - all_check (sync over all time - only permissible sync if not bucket-based)
%% - hour_check (sync over past hour, only allowed if bucket-based sync)
%% - day_check (sync over past day, only allowed if bucket-based sync)
%% - range_check (sync over a range if one has been discovered by a previour sync)
%% - auto_check (sync over range if one is present, otherwise use all if within window, otherwise day)
-spec ttaaefs_fullsync(riak_kv_ttaaefs_manager:work_item(), integer()) -> ok.
ttaaefs_fullsync(WorkItem, SecsTimeout) ->
    ReqId = mk_reqid(),
    riak_kv_ttaaefs_manager:process_workitem(
        WorkItem, ReqId, os:timestamp()),
    wait_for_reqid(ReqId, SecsTimeout * 1000).

%% @doc
%% Intended for tests only
%% Allows for the view of now to be altered during a test.
-spec ttaaefs_fullsync(riak_kv_ttaaefs_manager:work_item(), integer(),
                                                    erlang:timestamp()) -> ok.
ttaaefs_fullsync(WorkItem, SecsTimeout, Now) ->
    ReqId = mk_reqid(),
    riak_kv_ttaaefs_manager:process_workitem(WorkItem, ReqId, Now),
    wait_for_reqid(ReqId, SecsTimeout * 1000).

-spec repair_node() -> ok.
repair_node() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    NodeToRepair = node(),
    PartitionsToRepair =
        lists:filtermap(
            fun({P, Node}) ->
                case Node of
                    NodeToRepair ->
                        {true, P};
                    _ ->
                        false
                end
            end,
            riak_core_ring:all_owners(Ring)),
    [riak_kv_vnode:repair(P) || P <- PartitionsToRepair],
    ok.

-spec tictacaae_suspend_node() -> ok.
tictacaae_suspend_node() ->
    application:set_env(riak_kv, tictacaae_suspend, true).

-spec tictacaae_resume_node() -> ok.
tictacaae_resume_node() ->
    application:set_env(riak_kv, tictacaae_suspend, false).

-spec participate_in_coverage(boolean()) -> ok.
participate_in_coverage(Participate) ->
    F =
        fun(R, _) ->
            {new_ring,
                riak_core_ring:update_member_meta(
                    node(), R, node(), participate_in_coverage, Participate)}
        end,
    {ok, _FinalRing} = riak_core_ring_manager:ring_trans(F, undefined),
    ok.

-spec remove_node_from_coverage() -> ok.
remove_node_from_coverage() ->
    participate_in_coverage(false).

-spec reset_node_for_coverage() -> ok.
reset_node_for_coverage() ->
    participate_in_coverage(
        app_helper:get_env(riak_core, participate_in_coverage)).

%% @doc
%% Run a hot backup - returns {ok, true} if successful
-spec hotbackup(string(), pos_integer(), pos_integer(), riak_client())
                                    -> {ok, boolean()}|{error, Err :: term()}.
hotbackup(BackupPath, DefaultNVal, PlanNVal, {?MODULE, [Node, _ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    TimeOut = ?DEFAULT_FOLD_TIMEOUT,
    riak_kv_hotbackup_fsm_sup:start_hotbackup_fsm(Node,
                                                    [{raw, ReqId, Me},
                                                    [BackupPath,
                                                        {DefaultNVal, PlanNVal},
                                                        TimeOut]]),
    wait_for_fold_results(ReqId, TimeOut).


%% @spec get_index(Bucket :: binary(),
%%                 Query :: riak_index:query_def(),
%%                 riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Run the provided index query.
get_index(Bucket, Query, {?MODULE, [_Node, _ClientId]}=THIS) ->
    get_index(Bucket, Query, [{timeout, ?DEFAULT_TIMEOUT}], THIS).

%% @spec get_index(Bucket :: binary(),
%%                 Query :: riak_index:query_def(),
%%                 TimeoutMillisecs :: integer(),
%%                 riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Run the provided index query.
get_index(Bucket, Query, Opts, {?MODULE, [Node, _ClientId]}) ->
    Timeout = proplists:get_value(timeout, Opts, ?DEFAULT_TIMEOUT),
    MaxResults = proplists:get_value(max_results, Opts, all),
    PgSort = proplists:get_value(pagination_sort, Opts),
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_index_fsm_sup:start_index_fsm(Node, [{raw, ReqId, Me}, [Bucket, none, Query, Timeout, MaxResults, PgSort]]),
    wait_for_query_results(ReqId, Timeout).

%% @doc Run the provided index query, return a stream handle.
-spec stream_get_index(Bucket :: binary(), Query :: riak_index:query_def(),
                       riak_client()) ->
    {ok, ReqId :: term(), FSMPid :: pid()} | {error, Reason :: term()}.
stream_get_index(Bucket, Query, {?MODULE, [_Node, _ClientId]}=THIS) ->
    stream_get_index(Bucket, Query, [{timeout, ?DEFAULT_TIMEOUT}], THIS).

%% @doc Run the provided index query, return a stream handle.
-spec stream_get_index(Bucket :: binary(), Query :: riak_index:query_def(),
                       Opts :: proplists:proplist(), riak_client()) ->
    {ok, ReqId :: term(), FSMPid :: pid()} | {error, Reason :: term()}.
stream_get_index(Bucket, Query, Opts, {?MODULE, [Node, _ClientId]}) ->
    Timeout = proplists:get_value(timeout, Opts, ?DEFAULT_TIMEOUT),
    MaxResults = proplists:get_value(max_results, Opts, all),
    PgSort = proplists:get_value(pagination_sort, Opts),
    Me = self(),
    ReqId = mk_reqid(),
    case riak_kv_index_fsm_sup:start_index_fsm(Node,
                                               [{raw, ReqId, Me},
                                                [Bucket, none,
                                                 Query, Timeout,
                                                 MaxResults, PgSort]]) of
        {ok, Pid} ->
            {ok, ReqId, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @spec set_bucket(riak_object:bucket(), [BucketProp :: {atom(),term()}], riak_client()) -> ok
%% @doc Set the given properties for Bucket.
%%      This is generally best if done at application start time,
%%      to ensure expected per-bucket behavior.
%% See riak_core_bucket for expected useful properties.
set_bucket(BucketName,BucketProps,{?MODULE, [Node, _ClientId]}) ->
    rpc:call(Node,riak_core_bucket,set_bucket,[BucketName,BucketProps]).
%% @spec get_bucket(riak_object:bucket(), riak_client()) -> [BucketProp :: {atom(),term()}]
%% @doc Get all properties for Bucket.
%% See riak_core_bucket for expected useful properties.
get_bucket(BucketName, {?MODULE, [Node, _ClientId]}) ->
    rpc:call(Node,riak_core_bucket,get_bucket,[BucketName]).
%% @spec reset_bucket(riak_object:bucket(), riak_client()) -> ok
%% @doc Reset properties for this Bucket to the default values
reset_bucket(BucketName, {?MODULE, [Node, _ClientId]}) ->
    rpc:call(Node,riak_core_bucket,reset_bucket,[BucketName]).
%% @spec reload_all(Module :: atom(), riak_client()) -> term()
%% @doc Force all Riak nodes to reload Module.
%%      This is used when loading new modules for map/reduce functionality.
reload_all(Module, {?MODULE, [Node, _ClientId]}) -> rpc:call(Node,riak_core_util,reload_all,[Module]).

%% @spec remove_from_cluster(ExitingNode :: atom(), riak_client()) -> term()
%% @doc Cause all partitions owned by ExitingNode to be taken over
%%      by other nodes.
remove_from_cluster(ExitingNode, {?MODULE, [Node, _ClientId]}) ->
    rpc:call(Node, riak_core_gossip, remove_from_cluster,[ExitingNode]).

get_stats(local, {?MODULE, [Node, _ClientId]}) ->
    [{Node, rpc:call(Node, riak_kv_stat, get_stats, [])}];
get_stats(global, {?MODULE, [Node, _ClientId]}) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    Nodes = riak_core_ring:all_members(Ring),
    [{N, rpc:call(N, riak_kv_stat, get_stats, [])} || N <- Nodes].

%% @doc Return the client id being used for this client
get_client_id({?MODULE, [_Node, ClientId]}) ->
    ClientId.

%% @private
%% This function exists only to avoid compiler errors (unused type).
%% Unfortunately, I can't figure out how to suppress the bogus "Contract for
%% function that does not exist" warning from Dialyzer, so ignore that one.
-spec for_dialyzer_only_ignore(term(), term(), riak_client()) -> riak_client().
for_dialyzer_only_ignore(_X, _Y, {?MODULE, [_Node, _ClientId]}=THIS) ->
    THIS.

-spec mk_reqid() -> req_id().
%% @private
mk_reqid() ->
    erlang:phash2({self(), os:timestamp()}). % only has to be unique per-pid

%% @private
wait_for_reqid(ReqId, Timeout) ->
    receive
        {ReqId, {error, overload}=Response} ->
            case app_helper:get_env(riak_kv, overload_backoff, undefined) of
                Msecs when is_number(Msecs) ->
                    timer:sleep(Msecs);
                undefined ->
                    ok
            end,
            Response;
        {ReqId, Response} -> Response
    after Timeout ->
            {error, timeout}
    end.

%% @private
wait_for_listkeys(ReqId) ->
    wait_for_listkeys(ReqId, []).
%% @private
wait_for_listkeys(ReqId, Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(Acc)};
        {ReqId, From, {keys, Res}} ->
            _ = riak_kv_keys_fsm:ack_keys(From),
            wait_for_listkeys(ReqId, [Res|Acc]);
        {ReqId,{keys,Res}} -> wait_for_listkeys(ReqId, [Res|Acc]);
        {ReqId, {error, Error}} ->
            {error, Error}
    end.

%% @private
wait_for_listbuckets(ReqId) ->
    receive
        {ReqId,{buckets, Buckets}} ->
            {ok, Buckets};
        {ReqId, {error, Error}} ->
            {error, Error}
    end.

%% @private
wait_for_query_results(ReqId, Timeout) ->
    wait_for_query_results(ReqId, Timeout, []).
%% @private
wait_for_query_results(ReqId, Timeout, Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(lists:reverse(Acc))};
        {ReqId,{results, Res}} -> wait_for_query_results(ReqId, Timeout, [Res | Acc]);
        {ReqId, Error} -> {error, Error}
    after Timeout ->
            {error, timeout}
    end.

%% @private
%% @doc
%% Only final result will be received, so do not expect a separate "done"
%% response.
wait_for_fold_results(ReqId, Timeout) ->
    receive
        {ReqId, {results, Results}} -> {ok, Results};
        {ReqId, Error} -> {error, Error}
    after Timeout ->
        {error, timeout}
    end.

-spec recv_timeout(Options :: proplists:proplist()) -> timeout().
recv_timeout(Options) ->
    case proplists:get_value(recv_timeout, Options) of
        undefined ->
            %% If no reply timeout given, use the FSM timeout + 100ms to give it a chance
            %% to respond.
            case proplists:get_value(timeout, Options, ?DEFAULT_TIMEOUT) of
                infinity ->
                    infinity;
                MilliSecs ->
                    MilliSecs + 100
            end;
        Timeout ->
            %% Otherwise use the directly supplied timeout.
            Timeout
    end.

ensemble(BKey={Bucket, _Key}) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    DocIdx = riak_core_util:chash_key(BKey),
    Partition = chashbin:responsible_index(DocIdx, CHBin),
    N = riak_core_bucket:n_val(riak_core_bucket:get_bucket(Bucket)),
    {kv, Partition, N}.

consistent_object(Node, Bucket) when Node =:= node() ->
    riak_kv_util:consistent_object(Bucket);
consistent_object(Node, Bucket) ->
    case rpc:call(Node, riak_kv_util, consistent_object, [Bucket]) of
        {badrpc, {'EXIT', {undef, _}}} ->
            false;
        {badrpc, _}=Err ->
            {error, Err};
        Result ->
            Result
    end.

write_once(Node, Bucket) when Node =:= node() ->
    riak_kv_util:get_write_once(Bucket);
write_once(Node, Bucket) ->
    case rpc:call(Node, riak_kv_util, get_write_once, [Bucket]) of
        {badrpc, {'EXIT', {undef, _}}} ->
            false;
        {badrpc, _}=Err ->
            {error, Err};
        Result ->
            Result
    end.
