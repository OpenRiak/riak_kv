%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
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
-module(reaper_eqc).

-compile([export_all, nowarn_export_all]).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").

%% -- State ------------------------------------------------------------------
initial_state() ->
  #{ reaps => [] }.

%% -- Operations -------------------------------------------------------------

%% --- Operation: start ---
start_pre(S) -> not maps:is_key(pid, S).

start_args(_S) ->
  [].

start() ->
    FilePath = riak_core_test_util:get_test_dir(?MODULE),
    {ok, Pid} = riak_kv_reaper:start_link(FilePath),
    ok = gen_server:call(Pid, {override_action, fun reaper/2}),
    Pid.

start_next(S, Pid, []) ->
  S#{ pid => Pid }.

%% --- Operation: reap ---
reap_pre(S) -> maps:is_key(pid, S).

reap_args(#{ pid := Pid }) ->
  [Pid, gen_reap_ref()].

reap(Pid, {Ref, Retries}) ->
  ets:insert(?MODULE, {Ref, Retries}),
  riak_kv_reaper:request_reap(Pid, Ref).

reap_next(S = #{ reaps := Ds }, _Value, [_Pid, {Ref, Retries}]) ->
  S#{ reaps := [{Ref, Retries} | Ds] }.

reap_features(_, [_, {_, N}], _) ->
  [{retries, N}].

%% -- Generators -------------------------------------------------------------
gen_reap_ref() ->
  ?LET(Ref, gen_reference(),
    weighted_default({4, {Ref, 1}}, {1, {Ref, choose(2, 5)}})).

gen_reference() ->
  os:timestamp().

%% -- Properties -------------------------------------------------------------

prop_reaper() ->
  application:set_env(riak_kv, reaper_redo_timeout, 20),
  ?FORALL(Cmds, commands(?MODULE),
  begin
    ets:new(?MODULE, [named_table, public]),
    {H, S, Res} = run_commands(Cmds),
    StopRes = stop_job(maps:get(pid, S, undefined)),
    ETSRes = ets:tab2list(?MODULE),
    ets:delete(?MODULE),
    aggregate(call_features(H),
      pretty_commands(?MODULE, Cmds, {H, S, Res},
        conjunction([
          {result, equals(Res, ok)},
          {stop,   equals(StopRes, ok)},
          {ets,    equals(ETSRes, [])}])))
  end).

stop_job(undefined) -> ok;
stop_job(Pid) ->
  MonRef = monitor(process, Pid),
  riak_kv_reaper:stop_job(Pid),
  receive
    {'DOWN', MonRef, _, _, _} -> ok
  after 1000 ->
    timeout_stop_job
  end.


reaper(Ref, _) ->
  case ets:lookup(?MODULE, Ref) of
    [{_, 1}] ->
      ets:delete(?MODULE, Ref), true;
    [{_, N}] ->
      ets:insert(?MODULE, {Ref, N - 1}), false
  end.
