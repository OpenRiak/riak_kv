%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
%% Copyright (c) 2025 Workday, Inc.
%% Copyright (c) 2026 The OpenRiak Project.
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
%% @doc utility functions for interacting with the environment.
%% @end
%%
%% Implementation Notes:
%%
%%  This module requires OTP 24 as a minimum, for maps:from_keys/2.
%%
%%  Other than sysctl keys as discussed below, all strings destined for
%%  logging functions are literal flat lists because the io_lib functions
%%  convert all formats and strings to that form during processing.
%%
%%  Executable input and output is entirely as binaries for efficiency.
%%  In particular, the output of the sysctl executable is collected and
%%  handled as a single large binary() such that keys and values are all
%%  sub-binaries referencing the single output binary.
%%  For comparison efficiency, therefore, sysctl keys are literal binaries.
%%
-module(riak_kv_env).

-export([doc_env/0]).

-compile([
    no_auto_import,
    warn_export_vars,
    warn_missing_spec_all,
    warn_unused_import,
    %% Don't use global inlining, it'll generate a bunch of unused code.
    {inline, [
        compare_text/1,
        value_ok/3
    ]}
]).
-dialyzer([
    error_handling,
    unknown,
    unmatched_returns
]).

-ifdef(TEST).
-export([env_test/0]).
-include_lib("stdlib/include/assert.hrl").
-endif.

%% Define as 'true' to see the log records handled by doc_env/0.
-define(DEBUG_LOG_RECS, false).

-include_lib("kernel/include/logger.hrl").

%% ===================================================================
%% Public API
%% ===================================================================

-if(?DEBUG_LOG_RECS =/= true).
%% The compiler will do the right thing, but dialyzer will rightly complain.
-dialyzer({no_match, doc_env/0}).
-endif.

-spec doc_env() -> ok.
%% @doc Logs information about the runtime environment.
doc_env() ->
    %% 'Meta' won't actually ever be instantiated as defined, it'll just be
    %% used as the basis for map literals, with the MFA of this function
    %% instead of the anonymous log function.
    Meta = ?LOCATION,
    logger:log(info, "Environment and OS variables:", Meta#{line := ?LINE}),
    ELimits = erts_limits(),
    OSType  = os:type(),
    OLimits = os_limits(OSType),
    Ulimits = u_limits(OSType),

    %% The compiler will remove the case statement entirely if ?DEBUG_LOG_RECS
    %% is not exactly 'true', and just set LogFun to the 2nd definition.
    LogFun = case ?DEBUG_LOG_RECS andalso logger:allow(debug, ?MODULE) of
        %% If ?DEBUG_LOG_RECS is not 'true' then dialyzer will flag this
        %% - ignore it, the compiler does the right thing.
        true ->
            fun({Level, Fmt, Args} = Term) ->
                logger:macro_log(
                    Meta#{line := ?LINE}, debug, "Term: ~0tp", [Term]),
                logger:log(Level, Fmt, Args, Meta#{line := ?LINE})
            end;
        _ ->
            fun({Level, Fmt, Args}) ->
                logger:log(Level, Fmt, Args, Meta#{line := ?LINE})
            end
    end,
    lists:foreach(LogFun, Ulimits ++ ELimits ++ OLimits).

%% ===================================================================
%% Internal
%% ===================================================================

-type conf_key()    :: nonempty_string().
-type conf_value()  :: non_neg_integer() | atom().
-type erts_desc()   :: nonempty_string().
-type erts_key()    :: atom().
-type erts_param()  :: {erts_key(), conf_value(), erts_desc(), conf_key()}.
-type erts_params() :: list(erts_param()).
-type exe_arg()     :: binary().
-type exe_args()    :: list(exe_arg()).
-type exe_path()    :: binary().
-type log_args()    :: list(term()).
-type log_fmt()     :: nonempty_string().   % io:format()
-type log_rec()     :: {logger:level(), log_fmt(), log_args()}.
-type log_recs()    :: list(log_rec()).
-type os_type()     :: {unix | win32, atom()}.
-type sctl_key()    :: binary().
-type sctl_map()    :: #{sctl_key() => sctl_value() | undefined}.
-type sctl_param()  :: {sctl_key(), sctl_value(), val_compare()}.
-type sctl_params() :: list(sctl_param()).
-type sctl_value()  :: non_neg_integer().
-type sh_command()  :: list(binary()).

-type val_compare() :: eq | max | min.
%%  min - Actual Value must be `>=' Target Value.
%%  max - Actual Value must be `<=' Target Value.
%%  eq  - Actual Value must be `==' Target Value.

%% 2^10 Multipliers
-define(K(N),   (N * 1024)).
-define(M(N),   ?K(N * 1024)).
%% Milliseconds
-define(MS(S),  (S * 1000)).

%% erl +zdbbl - Distribution Buffer Busy limit
%% Threshold here is in raw bytes, as set in Cuttlefish.
%% https://www.erlang.org/doc/apps/erts/erl_cmd#+zdbbl
%% https://www.erlang.org/doc/apps/erts/erlang#system_info_dist_buf_busy_limit
%% Cuttlefish "erlang.distribution_buffer_size"
-define(ERTS_DBUF_BUSY,     ?M(32)).

%% erl +e - Maximum named ETS tables
%% https://www.erlang.org/doc/apps/erts/erl_cmd#+e
%% https://www.erlang.org/doc/apps/stdlib/ets#max_ets_tables
%% Cuttlefish "erlang.max_ets_tables"
%% This no longer sets a hard limit, instead serving as an optimization hint.
%% If more than this many named ETS tables are present in the VM performance
%% will be degraded.
-define(ERTS_ETS_MIN,       ?K(16)).

%% erl -env ERL_FULLSWEEP_AFTER
%% Fullsweep GC generations interval
%% https://www.erlang.org/doc/apps/erts/erlang#system_info_fullsweep_after
%% https://www.erlang.org/doc/apps/erts/erlang#system_flag/2
%% Cuttlefish "erlang.fullsweep_after"
%% TODO: Figure out recommended fullsweep range
%% The main consideration here are the long-lived VNode and backend processes
%% and the binaries they handle, complicated by their distribution between
%% process and binary heaps.
%% The assumption is that the higher the fullsweep interval, the longer the
%% actual GC will take when it's triggered, but until the issue is studied in
%% depth we're only guessing.
-define(ERTS_GCGEN_MAX,     ?K(16)).
-define(ERTS_GCGEN_MIN,     16).

%% erl +Q - Port Limit.
%% Should be a power of 2.
%% https://www.erlang.org/doc/apps/erts/erl_cmd#+Q
%% https://www.erlang.org/doc/apps/erts/erlang#system_info_port_limit
%% Cuttlefish "erlang.max_ports"
-define(ERTS_PORT_MIN,      ?K(64)).

%% erl +P - Process Limit.
%% Should be a power of 2.
%% https://www.erlang.org/doc/apps/erts/erl_cmd#+P
%% https://www.erlang.org/doc/apps/erts/erlang#system_info_process_limit
%% Cuttlefish "erlang.process_limit"
-define(ERTS_PROC_MIN,      ?K(512)).

%% Percentage schedulers/cores
%% Cuttlefish "erlang.schedulers", "erlang.schedulers.*"
-define(ERTS_SCHED_PCT,     95).

%% erl +A - Async Thread Pool size.
%% Mostly irrelevant, but wastes resources, log if higher than this threshold.
%% https://www.erlang.org/doc/apps/erts/erl_cmd#+A
%% https://www.erlang.org/doc/apps/erts/erlang#system_info_thread_pool_size
%% Cuttlefish "erlang.async_threads"
-define(ERTS_THRD_WARN,     16).

%% erl +C - Time Warp Mode.
%% https://www.erlang.org/doc/apps/erts/erl_cmd#+C
%% https://www.erlang.org/doc/apps/erts/erlang#system_info_time_warp_mode
%% Cuttlefish "erlang.time_warp_mode"
-define(ERTS_TIME_MODE,     multi_time_warp).

%% ulimit -c - Maximum Core Dump size.
%% In blocks of 512 bytes on POSIX-compliant platforms, or 1024 bytes on Linux.
-define(OS_CORE_MIN,        ?K(512)).

%% ulimit -n - Maximum Open Files limit.
%% Minimum OS open files limit.
-define(OS_FILE_MIN,        ?K(64)).

%% These will be logged in reverse order.
%% Type: erts_params()
-define(ERTS_LIMIT_PARAMS, [
    {time_warp_mode,        ?ERTS_TIME_MODE,
        "ERTS time warp mode",      "erlang.time_warp_mode"},
    {dist_buf_busy_limit,   ?ERTS_DBUF_BUSY,
        "ERTS dist buffer busy",    "erlang.distribution_buffer_size"},
    {thread_pool_size,      ?ERTS_THRD_WARN,
        "ERTS thread pool",         "erlang.async_threads"},
    {ets_limit,             ?ERTS_ETS_MIN,
        "ERTS ETS table",           "erlang.max_ets_tables"},
    {port_limit,            ?ERTS_PORT_MIN,
        "ERTS ports",               "erlang.max_ports"},
    {process_limit,         ?ERTS_PROC_MIN,
        "ERTS process",             "erlang.process_limit"}
]).

%% We assume all nodes are connected to a 1Gbps+ network with relatively low
%% latency.
%% All relevant TCP implementations provide auto-scaling, which should always
%% be enabled.
%% A valuable explanation of tuning across platforms can be found at:
%%  https://fasterdata.es.net/host-tuning/

%% Minimum recommended socket buffer limit.
-define(SYS_NET_SOBUF_MAX,     ?M(8)).

%% Minimum socket send/receive buffer sizes.
-define(SYS_NET_RECV_BUF,      ?M(4)).
-define(SYS_NET_SEND_BUF,      ?M(4)).

%% Minimum allowed pending connections on a socket.
-define(SYS_NET_SOCK_MAXCONN,  ?K(2)).

%% Minimum number of packets queued when the interface receives packets faster
%% than the kernel can process them.
-define(SYS_NET_DEV_MAXPKTS,   ?K(8)).

%% Minimum number of connections in the SYN_RECV queue.
-define(SYS_NET_SYN_BACKLOG,  ?K(16)).

%% Maximum seconds to wait for a final FIN packet before a socket is
%% forcibly closed.
-define(SYS_NET_FIN_TIMEOUT,      30).

%% Maximum seconds between packets that are sent to validate connections.
-define(SYS_NET_KEEPALV_INT,      90).

%% *_PARAMS lists will be logged in the order they're defined here.

%% See https://www.kernel.org/doc/Documentation/sysctl/
-define(LINUX_PARAMS, [
    {<<"net.core.netdev_max_backlog">>,     ?SYS_NET_DEV_MAXPKTS, min },
    {<<"net.core.rmem_default">>,              ?SYS_NET_RECV_BUF, min },
    {<<"net.core.rmem_max">>,                 ?SYS_NET_SOBUF_MAX, min },
    {<<"net.core.somaxconn">>,             ?SYS_NET_SOCK_MAXCONN, min },
    {<<"net.core.wmem_default">>,              ?SYS_NET_SEND_BUF, min },
    {<<"net.core.wmem_max">>,                 ?SYS_NET_SOBUF_MAX, min },
    {<<"net.ipv4.tcp_fin_timeout">>,        ?SYS_NET_FIN_TIMEOUT, max },
    {<<"net.ipv4.tcp_keepalive_intvl">>,    ?SYS_NET_KEEPALV_INT, max },
    {<<"net.ipv4.tcp_max_syn_backlog">>,    ?SYS_NET_SYN_BACKLOG, min },
    {<<"net.ipv4.tcp_moderate_rcvbuf">>,                       1,  eq },
    {<<"net.ipv4.tcp_sack">>,                                  1,  eq },
    {<<"net.ipv4.tcp_timestamps">>,                            1, min },
    {<<"net.ipv4.tcp_tw_reuse">>,                              1,  eq },
    {<<"net.ipv4.tcp_window_scaling">>,                        1,  eq },
    {<<"vm.swappiness">>,                                     10, max }
]).

%% See https://calomel.org/freebsd_network_tuning.html
-define(BSD_PARAMS, [
    {<<"kern.ipc.maxsockbuf">>,               ?SYS_NET_SOBUF_MAX, min },
    {<<"kern.ipc.somaxconn">>,             ?SYS_NET_SOCK_MAXCONN, min },
    {<<"net.inet.tcp.keepintvl">>,     ?MS(?SYS_NET_KEEPALV_INT), max },
    {<<"net.inet.tcp.recvbuf_auto">>,                          1,  eq },
    {<<"net.inet.tcp.recvbuf_max">>,          ?SYS_NET_SOBUF_MAX, min },
    {<<"net.inet.tcp.recvspace">>,             ?SYS_NET_RECV_BUF, min },
    {<<"net.inet.tcp.rfc1323">>,                               1,  eq },
    {<<"net.inet.tcp.sendbuf_auto">>,                          1,  eq },
    {<<"net.inet.tcp.sendbuf_max">>,          ?SYS_NET_SOBUF_MAX, min },
    {<<"net.inet.tcp.sendspace">>,             ?SYS_NET_SEND_BUF, min }
]).

%% See https://fasterdata.es.net/host-tuning/osx/
-define(DARWIN_PARAMS, [
    {<<"kern.ipc.maxsockbuf">>,               ?SYS_NET_SOBUF_MAX, min },
    {<<"kern.ipc.somaxconn">>,             ?SYS_NET_SOCK_MAXCONN, min },
    {<<"net.inet.tcp.autorcvbufmax">>,        ?SYS_NET_SOBUF_MAX, min },
    {<<"net.inet.tcp.autosndbufmax">>,        ?SYS_NET_SOBUF_MAX, min },
    {<<"net.inet.tcp.fin_timeout">>,   ?MS(?SYS_NET_FIN_TIMEOUT), max },
    {<<"net.inet.tcp.keepintvl">>,     ?MS(?SYS_NET_KEEPALV_INT), max },
    {<<"net.inet.tcp.recvspace">>,            ?SYS_NET_SOBUF_MAX, min },
    {<<"net.inet.tcp.sack">>,                                  1,  eq },
    {<<"net.inet.tcp.sendspace">>,            ?SYS_NET_SOBUF_MAX, min },
    {<<"net.inet.tcp.win_scale_factor">>,                      6, min }
]).

-spec erts_limits() -> log_recs().
%% @hidden
erts_limits() ->
    GCRec   = erts_fullsweep_rec(),
    SchRec  = erts_scheduler_rec(),
    erts_limits(?ERTS_LIMIT_PARAMS, [GCRec, SchRec]).

-spec erts_limits(Params :: erts_params(), Result :: log_recs())
        -> log_recs().
%% @hidden
%% `thread_pool_size' gets special handling because we only want to warn if it's
%% set to a high enough value that it might be wasting noticeable resources.
erts_limits([{thread_pool_size = VmKey, Max, Label, CfKey} | Params], Result) ->
    NextResult = case erlang:system_info(VmKey) of
        Val when Val > Max ->
            Rec = {notice,
                "~ts limit of ~b is high, not more than ~b is recommended."
                " Set with riak.conf: ~ts",
                [Label, Val, Max, CfKey]},
            [Rec | Result];
        _ ->
            Result
    end,
    erts_limits(Params, NextResult);
erts_limits([{VmKey, Min, Label, CfKey} | Params], Result) ->
    Rec = report_limit(Label,
        "riak.conf: " ++ CfKey, erlang:system_info(VmKey), Min),
    erts_limits(Params, [Rec | Result]);
erts_limits([], Result) ->
    Result.

-spec erts_fullsweep_rec() -> log_rec().
%% @hidden Generate ERTS fullsweep_after record.
erts_fullsweep_rec() ->
    {_, GCGens} = erlang:system_info(fullsweep_after),
    Relationship = if
        GCGens < ?ERTS_GCGEN_MIN ->
            "low";
        GCGens > ?ERTS_GCGEN_MAX ->
            "high";
        true ->
            ok
    end,
    case Relationship =:= ok of
        true ->
            {info, "ERTS fullsweep generations: ~b", [GCGens]};
        _ ->
            {notice,
                "ERTS fullsweep generations of ~b is ~s, ~b-~b is recommended."
                " Set with riak.conf: erlang.fullsweep_after",
                [GCGens, Relationship, ?ERTS_GCGEN_MIN, ?ERTS_GCGEN_MAX]}
    end.

-spec erts_scheduler_rec() -> log_rec().
%% @hidden Generate ERTS scheduler record.
erts_scheduler_rec() ->
    Scheds = erlang:system_info(schedulers),
    case erlang:system_info(logical_processors_available) of
        Cores when erlang:is_integer(Cores) ->
            SchMin = (Cores * ?ERTS_SCHED_PCT),
            case (Scheds * 100) of
                Sch when Sch >= SchMin ->
                    {info, "ERTS Schedulers: ~b for ~b CPU cores", [Scheds, Cores]};
                _ ->
                    {warning,
                        "Running ~b ERTS schedulers for ~b CPU cores,"
                        " at least ~b schedulers (~b%) recommended."
                        " Set with riak.conf: erlang.schedulers[...]",
                        [Scheds, Cores, (SchMin div 100), ?ERTS_SCHED_PCT]}
            end;
        _ ->
            {notice,
                "Running ~b ERTS schedulers for unknown CPU cores,"
                " at least ~b% of CPU cores recommended."
                " Set with riak.conf: erlang.schedulers[...]",
                [Scheds, ?ERTS_SCHED_PCT]}
    end.

-spec os_limits(os_type()) -> log_recs().
%% @hidden
os_limits({unix, linux}) ->
    %% https://man7.org/linux/man-pages/man8/sysctl.8.html
    %% -e:  Ignore errors about unknown keys.
    %% K/V delimited by " = ".
    report_sysctl(<<"/usr/sbin/sysctl">>, <<"-e">>, ?LINUX_PARAMS);
os_limits({unix, freebsd}) ->
    %% https://man.freebsd.org/cgi/man.cgi?sysctl(8)
    %% -i:  Ignore unknown OIDs.
    %% -q:  Suppress some warnings generated to standard error.
    %% K/V delimited by ": ".
    report_sysctl(<<"/sbin/sysctl">>, <<"-iq">>, ?BSD_PARAMS);
os_limits({unix, darwin}) ->
    %% https://ss64.com/mac/sysctl.html for switches only
    %% Local 'man sysctl' for key info.
    %% -i:  Ignore unknown OIDs.
    %% -q:  Suppress some warnings generated to standard error.
    %% K/V delimited by ": ".
    report_sysctl(<<"/usr/sbin/sysctl">>, <<"-iq">>, ?DARWIN_PARAMS);
os_limits({Fam, Name}) ->
    [{warning,
        "Unsupported OS ~ts:~ts, no platform-specific info", [Fam, Name]}].

-spec u_limits(
    ULimitExeOrOSType :: os_type() | exe_path() | sh_command()) -> log_recs().
%% We don't really care about anything but core dump and open file limits.
%% Note that on some supported platforms these values can be obtained with
%% fewer invocations of the external program, but it's not worth managing
%% separate implementations.  This implementation *should* work properly on
%% all platforms we care about.
u_limits({unix, linux}) ->
    %% Some brain-dead Linux distros don't include a standalone `ulimit'
    %% executable, only a shell builtin. Since POSIX expects `ulimit' to be a
    %% distinct command, it does not specify it as a `/bin/sh' command, hence
    %% we need to rely on a non-POSIX shell for it if it's not present, and
    %% the Linux standard is bash.
    case os:find_executable("/usr/bin/ulimit") of
        false ->
            u_limits([<<"/bin/bash">>, <<"ulimit">>]);
        _ ->
            u_limits(<<"/usr/bin/ulimit">>)
    end;
u_limits({unix, _}) ->
    u_limits(<<"/usr/bin/ulimit">>);
u_limits({Fam, Name}) ->
    [{warning, "Unsupported OS ~ts:~ts, no ulimit info", [Fam, Name]}];
u_limits(ULimitExe) ->
    OFLimit = case u_limit(ULimitExe, <<"-n">>) of
        unlimited ->
            u_limit(ULimitExe, <<"-Hn">>);
        UsrOF ->
            UsrOF
    end,
    OFRec = case OFLimit of
        unlimited ->
            {info, "Open files unlimited", []};
        _ ->
            report_limit("Open files", "ulimit -n", OFLimit, ?OS_FILE_MIN)
    end,
    CFLimit = case u_limit(ULimitExe, <<"-c">>) of
        unlimited ->
            u_limit(ULimitExe, <<"-Hc">>);
        UsrCF ->
            UsrCF
    end,
    CFRec = case CFLimit of
        unlimited ->
            {info, "Core dump size unlimited", []};
        0 ->
            {notice,
                "Core dumps are disabled, this may hinder debugging."
                " Enable with ulimit -c", []};
        _ ->
            report_limit("Core dump size", "ulimit -c", CFLimit, ?OS_CORE_MIN)
    end,
    [OFRec, CFRec].

-spec u_limit(Cmd :: exe_path() | sh_command(), Arg :: exe_arg())
        -> non_neg_integer() | unlimited.
%% @hidden
%% On POSIX-compliant OSes where there's a distinct `ulimit' executable,
%% passes the call (almost) directly to `u_limit_exec/2'.
%% If the OS only implements `ulimit' via the shell (sigh), fixes up the
%% arguments to do so transparently.
u_limit(Exe, Arg) when erlang:is_binary(Exe) ->
    u_limit_exec(Exe, [Arg]);
u_limit([Sh, Cmd], Arg) when erlang:is_binary(Cmd) ->
    u_limit_exec(Sh, [<<"-c">>, <<Cmd/binary, $\s, Arg/binary>>]);
u_limit([Sh | CmdArgs], Arg) ->
    u_limit([Sh, erlang:iolist_to_binary(lists:join(<<$\s>>, CmdArgs))], Arg).

-spec u_limit_exec(Cmd :: exe_path(), Args :: exe_args())
        -> non_neg_integer() | unlimited.
u_limit_exec(Cmd, Args) ->
    %% Relies on undocumented behavior of string:trim(...) whereby the
    %% returned value is the same type as the 1st parameter, so we can
    %% count on it being a binary.
    case string:trim(run_exe(Cmd, Args)) of
        <<"unlimited">> ->
            unlimited;
        Int ->
            erlang:binary_to_integer(Int)
    end.

-spec report_limit(
    Label :: nonempty_string(),
    CfgKey :: conf_key(),
    Val :: conf_value(),
    Tgt :: conf_value() )
        -> log_rec().
%% @hidden
report_limit(Label, CfgKey, Val, Min) when erlang:is_integer(Val), Val < Min ->
    {warning,
        "~ts limit of ~b is low, at least ~b is recommended. Set with ~ts",
        [Label, Val, Min, CfgKey]};
report_limit(Label, _CfgKey, Val, _Min) when erlang:is_integer(Val) ->
    {info, "~ts limit: ~0tp", [Label, Val]};
report_limit(Label, CfgKey, unknown, Min) ->
    {notice,
        "~ts limit is unknown, at least ~0tp is recommended. Set with ~ts",
        [Label, Min, CfgKey]};
report_limit(Label, _CfgKey, Val, Val) ->
    {info, "~ts: ~0tp", [Label, Val]};
report_limit(Label, CfgKey, Val, Tgt) ->
    {warning,
        "~ts is ~0tp, ~0tp is recommended. Set with ~ts",
        [Label, Val, Tgt, CfgKey]}.

-spec report_sysctl(
    SysctlExe :: exe_path(), ExeArg :: exe_arg(), Params :: sctl_params())
        -> log_recs().
%% Collect the log records for Params.
%% SysctlExe:
%%  The full path to the `sysctl` executable on the platform.
%% ExeArg:
%%  The modifier(s) required for SysctlExe to:
%%  *   Ignore unrecognized keys.
%%  *   Avoid extraneous output.
%%  *   Output Key/Value pairs one per line, delimited by newlines and
%%      separated by ':' or '=', with or without whitespace.
%% Params:
%%  The list of keys and associated target values.
report_sysctl(SysctlExe, ExeArg, Params) ->
    %% Use a single invocation of the sysctl executable because it's
    %% significantly faster than invoking it once per key.
    %% Using maps:from_keys/2 gives us an easy way to flag missing keys.
    PrmKeys = [erlang:element(1, P) || P <- Params],
    KeyMap  = maps:from_keys(PrmKeys, undefined),
    SCOut   = run_exe(SysctlExe, [ExeArg | PrmKeys]),
    SCLines = binary:split(SCOut, <<"\n">>, [global, trim_all]),
    %% The K/V delimiter is either ':' or '='.
    %% The whitespace characters get stripped as if by string:trim/1.
    Delims  = [<<":">>, <<"=">>, <<$\s>>, <<$\t>>, <<$\r>>],
    Split   = binary:compile_pattern(Delims),
    SCRecs  = [binary:split(L, Split, [global, trim_all]) || L <- SCLines],
    KVMap   = sysctl_maprecs(SCRecs, KeyMap),
    sysctl_report(Params, KVMap).

-spec sysctl_maprecs(list(list(binary())), KVMap :: sctl_map())
        -> sctl_map().
%% @hidden Populate the KV map from sysctl output strings.
%% @end
%% The builtin erlang:is_map_key/2 guard is faster than any implementation
%% using lists:member/2 in a case statement.
%% We're updating existing keys in KVMap, so what's left will flag missing
%% keys in the sysctl output for us.
sysctl_maprecs([[Key, Val] | Recs], KVMap) when erlang:is_map_key(Key, KVMap) ->
    sysctl_maprecs(Recs, KVMap#{Key := erlang:binary_to_integer(Val)});
sysctl_maprecs([_ | Recs], KVMap) ->
    %% This case shouldn't happen, but accommodate cruft in the sysctl output.
    sysctl_maprecs(Recs, KVMap);
sysctl_maprecs([], KVMap) ->
    KVMap.

-spec sysctl_report(Params :: sctl_params(), KVMap :: sctl_map())
        -> log_recs().
%% @hidden Maps sysctl parameter records to log records.
sysctl_report([{Param, Target, Compare} | Params], KVMap) ->
    Rec = case erlang:map_get(Param, KVMap) of
        undefined ->
            {notice, "sysctl ~ts is not supported on this platform", [Param]};
        Actual ->
            Relationship = compare_text(Compare),
            case value_ok(Compare, Actual, Target) of
                true ->
                    {info, "sysctl ~ts is ~0tp, ~ts ~0tp",
                        [Param, Actual, Relationship, Target]};
                _ ->
                    {warning, "sysctl ~ts is ~0tp, should be ~ts ~0tp",
                        [Param, Actual, Relationship, Target]}
            end
    end,
    [Rec | sysctl_report(Params, KVMap)];
sysctl_report([], _) ->
    [].

-spec value_ok(
    Compare :: val_compare(), Actual :: integer(), Target :: integer())
        -> boolean().
value_ok(eq, Actual, Target) ->
    Actual == Target;
value_ok(max, Actual, Target) ->
    Actual =< Target;
value_ok(min, Actual, Target) ->
    Actual >= Target.

-spec compare_text(Compare :: val_compare()) -> nonempty_string().
compare_text(eq) ->
    "equal to";
compare_text(max) ->
    "not more than";
compare_text(min) ->
    "at least".

%% ===================================================================
%% External Executables
%% ===================================================================
%% Everything in and out is binaries.
%% This is about 3x faster than os:cmd/1 for a simple command, with the speed
%% improvement going up as output increases by skipping the string() ->
%% binary() -> string() conversions. Invocations of sysctl with a dozen or more
%% keys are 8x faster, or more, while using a lot less heap memory.

-spec run_exe(Exe :: exe_path(), Args :: exe_args()) -> binary().
%% @hidden Runs Exe with Args and returns all output as a single binary.
run_exe(Exe, Args) ->
    Start = erlang:monotonic_time(),
    Port = erlang:open_port({spawn_executable, Exe},
        [{args, Args}, binary, hide, in, stderr_to_stdout, stream]),
    Mon = erlang:monitor(port, Port),
    Result = collect_output(Port, Mon, []),
    erlang:demonitor(Mon),
    ?LOG_DEBUG("Time: ~bµs to exec ~ts", [
        erlang:convert_time_unit((erlang:monotonic_time() - Start),
            native, microsecond), lists:join(" ", [Exe | Args])]),
    Result.

-spec collect_output(Port :: port(), Mon :: reference(), Result :: iolist())
        -> binary().
collect_output(Port, Mon, Result) ->
    receive
        {Port, {data, Data}} ->
            collect_output(Port, Mon, [Result, Data]);
        {'DOWN', Mon, _, _, _} ->
            collect_result(Port, Result)
    end.

-spec collect_result(Port :: port(), Result :: iolist()) -> binary().
collect_result(Port, Result) ->
    receive
        {'EXIT', Port, _} ->
            ok
    after
        0 ->
            ok
    end,
    erlang:iolist_to_binary(Result).

%% ===================================================================
%% Tests
%% ===================================================================

-ifdef(TEST).

-spec env_test() -> ok.
env_test() ->
    LogFile = riak_core_test_util:logger_redirect(?MODULE, ?MODULE_STRING ++ ".log"),
    LogLevel = maps:get(level, logger:get_primary_config()),
    ok = logger:set_primary_config(level, debug),
    ok = doc_env(),
    ok = riak_core_test_util:logger_restore(),
    ok = logger:set_primary_config(level, LogLevel),
    ok = io:fwrite(user, "~nLog: ~ts~n", [LogFile]).

-endif.
