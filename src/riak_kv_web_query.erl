%% -------------------------------------------------------------------
%%
%% Copyright (c) 2026 Martin Sumner.
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

-module(riak_kv_web_query).

-export([get_result_key/1]).

-define(ACCKEY_KEYS, <<"keys">>).
-define(ACCKEY_TERMS, <<"terms">>).
-define(ACCKEY_COUNT, <<"count">>).
-define(ACCKEY_TERMCOUNT, <<"term_with_count">>).
-define(ACCKEY_RAWKEYS, <<"raw_keys">>).
-define(ACCKEY_RAWTERMS, <<"raw_terms">>).
-define(ACCKEY_RAWCOUNT, <<"raw_count">>).
-define(ACCKEY_TERMRAWCOUNT, <<"term_with_rawcount">>).

-spec get_result_key(riak_kv_query:accumulation_option()) -> binary().
get_result_key(keys) -> ?ACCKEY_KEYS;
get_result_key(raw_keys) -> ?ACCKEY_RAWKEYS;
get_result_key(terms) -> ?ACCKEY_TERMS;
get_result_key(raw_terms) -> ?ACCKEY_RAWTERMS;
get_result_key(count) -> ?ACCKEY_COUNT;
get_result_key(raw_count) -> ?ACCKEY_RAWCOUNT;
get_result_key(term_with_count) -> ?ACCKEY_TERMCOUNT;
get_result_key(term_with_rawcount) -> ?ACCKEY_TERMRAWCOUNT.
