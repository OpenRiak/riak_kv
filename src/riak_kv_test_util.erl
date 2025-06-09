%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
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
%%
%% @doc utilities for test scripts
%%
-module(riak_kv_test_util).

-ifdef(TEST).

-export([
    call_unused_fsm_funs/1,
    common_cleanup/1,
    common_cleanup/2,
    common_setup/1,
    common_setup/2,
    stop_process/1,
    wait_for_children/1,
    wait_for_pid/1,
    wait_for_unregister/1
]).

-include_lib("stdlib/include/assert.hrl").

-type abs_path()    :: nonempty_string().
-type test_phase()  :: load | start | stop.
-type phase_fun()   :: fun((test_phase()) -> term()).
-type test_fun()    :: fun(() -> term()).
-type test_name()   :: atom() | nonempty_string().

-define(SETUPTHUNK, fun(_) -> ok end).

%% Creates a setup function for tests that need Riak KV stood
%% up in an isolated fashion.
%% see setup/3
-spec common_setup(TestName :: test_name()) -> fun().
common_setup(T) when is_atom(T) ->
    common_setup(atom_to_list(T));
common_setup(TestName) ->
    common_setup(TestName, ?SETUPTHUNK).

-spec common_setup(TestName :: test_name(), SetupFun :: phase_fun()) -> test_fun().
common_setup(T, S) when is_atom(T) ->
    common_setup(atom_to_list(T), S);
common_setup(TestName, Setup) ->
    fun() -> setup(TestName, Setup) end.

%% Creates a cleanup function for tests that need Riak KV stood up in
%% an isolated fashion.
%% see cleanup/3
-spec common_cleanup(TestName :: test_name()) -> fun().
common_cleanup(T) when is_atom(T) ->
    common_cleanup(atom_to_list(T));
common_cleanup(TestName) ->
    common_cleanup(TestName, ?SETUPTHUNK).

-spec common_cleanup(TestName :: test_name(), CleanupFun :: phase_fun())
        -> phase_fun().
common_cleanup(T, C) when is_atom(T) ->
    common_cleanup(atom_to_list(T), C);
common_cleanup(TestName, Cleanup) ->
    fun(X) -> cleanup(TestName, Cleanup, X) end.

%% Calls gen_fsm functions that might not have been touched by a
%% test
-spec call_unused_fsm_funs(module()) -> any().
call_unused_fsm_funs(Mod) ->
    Mod:handle_event(event, statename, state),
    Mod:handle_sync_event(event, from, stateneame, state),
    Mod:handle_info(info, statename, statedata),
    Mod:terminate(reason, statename, state),
    Mod:code_change(oldvsn, statename, state, extra).

%% Stop a running pid - unlink and exit(kill) the process
stop_process(undefined) ->
    ok;
stop_process(RegName) when is_atom(RegName) ->
    stop_process(whereis(RegName));
stop_process(Pid) when is_pid(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    ok = wait_for_pid(Pid).

%% Wait for a pid to exit
wait_for_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mref, process, _, _} ->
            ok
    after
        5000 ->
            {error, didnotexit, Pid, erlang:process_info(Pid)}
    end.

%% Wait for registered process to exit.
-spec wait_for_unregister(Mod :: module())
        -> ok | {error, didnotexit, pid(), term()}.
wait_for_unregister(Mod) ->
    case whereis(Mod) of
        undefined ->
            ok;
        Pid ->
            case erlang:function_exported(Mod, stop, 0) of
                true ->
                    Mod:stop(),
                    wait_for_pid(Pid);
                false ->
                    stop_process(Pid)
            end
    end.

%% Wait for children that were spawned with proc_lib.
%% They have an '$ancestors' entry in their dictionary
wait_for_children(PPid) ->
    F = fun(CPid) ->
        case process_info(CPid, initial_call) of
            {initial_call, {proc_lib, init_p, 3}} ->
                case process_info(CPid, dictionary) of
                    {dictionary, Dict} ->
                        case proplists:get_value('$ancestors', Dict) of
                            undefined ->
                                %% Process dictionary not updated yet
                                true;
                            Ancestors ->
                                lists:member(PPid, Ancestors)
                        end;
                    undefined ->
                        %% No dictionary - should be one if proclib spawned it
                        true
                end;
            _ ->
                %% Not in proc_lib
                false
        end
    end,
    case lists:any(F, processes()) of
        true ->
            timer:sleep(1),
            wait_for_children(PPid);
        false ->
            ok
    end.

%% Performs generic, riak_kv-specific and test-specific setup
%% when used within a test fixture. This includes cleaning up any
%% leaky state from previous tests (internally calling `cleanup/3'),
%% loading dependent applications, starting distributed Erlang,
%% starting dependent applications, and waiting for riak_kv to become
%% available.
%%
%% The given `SetupFun' will be called first with the argument `stop'
%% before other applications are stopped (to cleanup leaky test
%% state), `load' after all other applications are loaded, and then
%% `start' after all other applications are started. It is generally
%% good practice to use the same function in the `SetupFun' as the
%% `CleanupFun' given to `cleanup/3'.
%%
%% see common_setup/2, dep_apps/2, do_dep_apps/2
-spec setup(TestName :: test_name(), SetupFun :: phase_fun()) -> ok.
setup(TestName, SetupFun) ->
    %% Cleanup in case a previous test did not
    cleanup(TestName, SetupFun, setup),
    %% Load application environments
    Deps = dep_apps(TestName, SetupFun),
    do_dep_apps(load, Deps),

    %% Start epmd
    _ = os:cmd("epmd -daemon"),

    %% Start erlang node
    {ok, Hostname} = inet:gethostname(),
    TestNode = list_to_atom(TestName ++ "@" ++ Hostname),
    net_kernel:start([TestNode, longnames]),

    %% Start dependent applications
    AllApps = do_dep_apps(start, Deps),

    %% Wait for KV to be ready
    riak_core:wait_for_application(riak_kv),
    riak_core:wait_for_service(riak_kv),
    AllApps.

%% Performs generic, riak_kv-specific and test-specific cleanup
%% when used within a test fixture. This includes stopping dependent
%% applications, stopping distributed Erlang, and killing pernicious
%% processes. The given `CleanupFun' will be called with the argument
%% `stop' before other components are stopped.
%%
%% see common_cleanup/2, dep_apps/2, do_dep_apps/2
-spec cleanup(
    Test :: test_name(),
    CleanupFun :: phase_fun(),
    SetupResult :: setup | list(atom())) -> ok.
cleanup(Test, CleanupFun, setup) ->
    %% Remove existing ring files so we have a fresh ring
    RingDir = filename:join(riak_core_test_util:get_test_dir(Test), "ring"),
    riak_core_test_util:ensure_no_file(RingDir),
    cleanup(Test, CleanupFun, []);
cleanup(Test, CleanupFun, StartedApps) ->
    Deps = lists:reverse(dep_apps(Test, CleanupFun)),
    Apps = Deps ++ lists:filtermap(
        fun(A) ->
            not lists:member(A, Deps)
        end, lists:reverse(StartedApps)),

    %% Stop the applications in reverse order.
    do_dep_apps(stop, Apps),

    %% Cleanup potentially runaway processes
    _ = catch exit(whereis(riak_kv_vnode_master), kill),
    _ = catch exit(whereis(riak_sysmon_filter), kill),
    %% Need to specifically wait for riak_kv_stat to unregister, since
    %% otherwise we get a specific error
    %% {{already_started,Pid},#child{...}}  from supervisor:start_child/2
    %% where riak_kv_stat is already started by another supervisor from a
    %% previous test.
    wait_for_unregister(riak_kv_stat),
    %% Stop distributed Erlang
    net_kernel:stop(),

    {ok, Hostname} = inet:gethostname(),
    _ = os:cmd("/bin/rm -rf *@" ++ Hostname),

    %% Reset the riak_core vnode_modules
    application:set_env(riak_core, vnode_modules, []),
    ok.

%% Calculates a list of dependent applications and functions that
%% can be passed to do_deps_apps/2 to perform the lifecycle phase on
%% them all at once. This ensures that applications start and stop in
%% the correct order and the test also has a chance to inject its own
%% setup and teardown code. Included in the sequence are two default
%% setup functions, one that silences SASL logging and redirects it to
%% a file, and one that configures some settings for riak_core and
%% lager.
%%
%% By passing the `Test' argument, the test's data and logging state
%% is also isolated to its own directory so as not to clobber other
%% tests.
%%
%% The `Extra' function takes an atom which represents the phase of
%% application lifecycle, one of `load', `start' or `stop'.
%%
%% see common_setup/2, common_cleanup/2
-spec dep_apps(Test :: test_name(), Extra :: phase_fun())
        -> [atom() | phase_fun()].
dep_apps(Test, Extra) ->
    Silencer = fun
        (load) ->
            riak_core_test_util:logger_silence();
        (_) ->
            ok
    end,
    DefaultSetupFun = fun
        (load) ->
            %% Set some missing env vars that are normally part of
            %% release packaging. These can be overridden by the
            %% Extra fun.
            TestDir = riak_core_test_util:get_test_dir(Test),
            application:set_env(
                riak_ensemble, data_root, filename:join(TestDir, "ensemble")),
            application:set_env(riak_core, ring_creation_size, 64),
            application:set_env(
                riak_core, ring_state_dir, filename:join(TestDir, "ring")),
            application:set_env(
                riak_core, platform_data_dir, filename:join(TestDir, "data")),
            %% pick a random handoff port
            application:set_env(riak_core, handoff_port, 0),
            %% @TODO this is wrong still as the deps dirs is a
            %% best guest in `get_deps_dir/0'
            SchemaDirWC = filename:join([get_deps_dir(), "*", "priv"]),
            application:set_env(riak_core, schema_dirs, [SchemaDirWC]),
            application:set_env(
                riak_kv, eraser_dataroot, filename:join(TestDir, "kv_eraser")),
            application:set_env(
                riak_kv, reaper_dataroot, filename:join(TestDir, "kv_reaper")),
            application:set_env(
                riak_kv, reader_dataroot, filename:join(TestDir, "kv_reader"));
        (_) ->
            ok
    end,
    [
        Silencer, exometer_core, runtime_tools,
        mochiweb, webmachine, sidejob, poolboy, basho_stats, bitcask,
        eleveldb, riak_core, riak_pipe, riak_api, riak_dt, riak_pb,
        riak_kv, DefaultSetupFun, Extra
    ].


%% Runs the application-lifecycle phase across all of the given
%% applications and functions.
%% see dep_apps/2
-spec do_dep_apps(test_phase(), list(atom() | test_phase())) -> list().
do_dep_apps(start, Apps) ->
    lists:foldl(fun do_dep_apps_fun/2, [], Apps);
do_dep_apps(LoadStop, Apps) ->
    lists:map(fun
        (A) when erlang:is_atom(A) ->
            case include_app_phase(LoadStop, A) of
                true ->
                    application:LoadStop(A);
                _ ->
                    ok
            end;
        (F) when erlang:is_function(F, 1) ->
            F(LoadStop)
    end, Apps).

do_dep_apps_fun(A, Acc) when erlang:is_atom(A) ->
    case include_app_phase(start, A) of
        true ->
            case start_app_and_deps(A, Acc) of
                {ok, Started} ->
                    Started;
                {error, Reason} ->
                    erlang:error(Reason, [A, Acc])
            end;
        _ ->
            Acc
    end;
do_dep_apps_fun(F, Acc) when erlang:is_function(F, 1) ->
    F(start),
    Acc.

%% Determines whether a given application should be modified in
%% the given phase. If this returns false, the application will not be
%% loaded, started, or stopped by `do_dep_apps/2'.
-spec include_app_phase(
    Phase :: test_phase(),
    Application :: atom()) -> boolean().
include_app_phase(stop, crypto) -> false;
include_app_phase(_Phase, _App) -> true.

%% Make sure an application and all of its dependent applications are started.
%% Similar to application:ensure_all_started/1 available in R16B02.
-spec start_app_and_deps(Application :: atom(), list(atom()))
        -> {ok, [atom()]} | {error, term()}.
start_app_and_deps(Application, Started) ->
    case lists:member(Application, Started) of
        true ->
            {ok, Started};
        _ ->
            _Apps = application:which_applications(),
            case application:start(Application) of
                ok ->
                    {ok, [Application | Started]};
                {error, {already_started, Application}} ->
                    {ok, Started};
                {error, {not_started, Dep}} ->
                    case start_app_and_deps(Dep, Started) of
                        {ok, NStarted} ->
                            start_app_and_deps(Application, NStarted);
                        Error ->
                            Error
                    end;
                {error, Reason} ->
                    [application:stop(App) || App <- Started],
                    {error, Reason}
            end
    end.

-spec get_deps_dir() -> abs_path().
get_deps_dir() ->
    PKey = {?MODULE, deps_dir},
    case persistent_term:get(PKey, undefined) of
        undefined ->
            DepsDir = case os:getenv("REBAR_DEPS_DIR") of
                false ->
                    guess_deps_dir();
                Dir ->
                    Dir
            end,
            persistent_term:put(PKey, DepsDir),
            DepsDir;
        DDVal ->
            DDVal
    end.

-spec guess_deps_dir() -> abs_path().
guess_deps_dir() ->
    {ok, CWD} = file:get_cwd(),
    DepsDir = case filename:rootname(CWD) == CWD of
        true ->
            %% not in .eunit, must be running from console
            BDL = filename:join([CWD, "_build", "default", "lib"]),
            case filelib:is_dir(BDL) of
                true ->
                    %% running as rebar3 from console
                    BDL;
                _ ->
                    Deps = filename:join(CWD, "deps"),
                    case filelib:is_dir(Deps) of
                        true ->
                            %% probably a root checkout
                            Deps;
                        _ ->
                            %% probably part of an applications deps
                            % ".."
                            filename:dirname(CWD)
                    end
            end;
        _ ->
            %% probably running in .eunit
            UpDeps = filename:join(filename:dirname(CWD), "deps"),
            case filelib:is_dir(UpDeps) of
                true ->
                    UpDeps;
                _ ->
                    %% maybe we're in a deps/* situation, worse case tests
                    %% fail, which is what they did before this hack
                    % "../.."
                    filename:dirname(filename:dirname(CWD))
            end
    end,
    % io:format(user, "~n*** Using deps at ~ts~n", [DepsDir]),
    DepsDir.

-endif. % TEST
