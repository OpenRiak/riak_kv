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

%% Constants used by the raw_http resources

%% Names of riak_object metadata fields
-define(MD_CTYPE,    <<"content-type">>).
-define(MD_CHARSET,  <<"charset">>).
-define(MD_ENCODING, <<"content-encoding">>).
-define(MD_VTAG,     <<"X-Riak-VTag">>).
-define(MD_LINKS,    <<"Links">>).
-define(MD_LASTMOD,  <<"X-Riak-Last-Modified">>).   %% erlang:timestamp()
-define(MD_USERMETA, <<"X-Riak-Meta">>).
-define(MD_INDEX,    <<"index">>).
-define(MD_DELETED,  <<"X-Riak-Deleted">>).
-define(MD_VAL_ENCODING, <<"X-Riak-Val-Encoding">>).
%% Provenance metadata - Copy/Move stores, Restore strips
-define(MD_PROV_OP,         <<"X-Riak-Prov-Operation">>).
-define(MD_PROV_OP_TIME,    <<"X-Riak-Prov-Op-Time">>).             %% erlang:timestamp()
-define(MD_PROV_SRC_B,      <<"X-Riak-Prov-Src-Bucket">>).
-define(MD_PROV_SRC_BT,     <<"X-Riak-Prov-Src-BucketType">>).
-define(MD_PROV_SRC_K,      <<"X-Riak-Prov-Src-Key">>).
-define(MD_PROV_SRC_LM,     <<"X-Riak-Prov-Src-Last-Modified">>).   %% erlang:timestamp()
-define(MD_PROV_SRC_VC,     <<"X-Riak-Prov-Src-VClock">>).
-define(MD_PROV_TOMB_VC,    <<"X-Riak-Prov-Tomb-VClock">>).

%% Names of HTTP header fields
-define(HEAD_CTYPE,           "Content-Type").
-define(HEAD_VCLOCK,          "X-Riak-Vclock").
-define(HEAD_LINK,            "Link").
-define(HEAD_ENCODING,        "Content-Encoding").
-define(HEAD_CLIENT,          "X-Riak-ClientId").
-define(HEAD_USERMETA_PREFIX, "x-riak-meta-").
-define(HEAD_INDEX_PREFIX,    "x-riak-index-").
-define(HEAD_DELETED,         "X-Riak-Deleted").
-define(HEAD_TIMEOUT,         "X-Riak-Timeout").
-define(HEAD_CRDT_CONTEXT,    "X-Riak-CRDT-Ctx").
-define(HEAD_IF_NOT_MODIFIED, "X-Riak-If-Not-Modified").
-define(HEAD_PROV_PREFIX,     "x-riak-prov-").

%% Names of JSON fields in bucket properties
-define(JSON_PROPS,   <<"props">>).
-define(JSON_BUCKETS, <<"buckets">>).
-define(JSON_KEYS,    <<"keys">>).
-define(JSON_LINKFUN, <<"linkfun">>).
-define(JSON_MOD,     <<"mod">>).
-define(JSON_FUN,     <<"fun">>).
-define(JSON_ARG,     <<"arg">>).
-define(JSON_CHASH,   <<"chash_keyfun">>).
-define(JSON_JSFUN,    <<"jsfun">>).
-define(JSON_JSANON,   <<"jsanon">>).
-define(JSON_JSBUCKET, <<"bucket">>).
-define(JSON_JSKEY,    <<"key">>).
-define(JSON_ALLOW_MULT, <<"allow_mult">>).
-define(JSON_EXTRACT, <<"search_extractor">>).
-define(JSON_EXTRACT_LEGACY, <<"rs_extractfun">>).
-define(JSON_DATATYPE, <<"datatype">>).
-define(JSON_HLL_PRECISION, <<"hll_precision">>).

%% Names of HTTP query parameters
-define(Q_PROPS, "props").
-define(Q_BUCKETS, "buckets").
-define(Q_KEYS,  "keys").
-define(Q_FALSE, "false").
-define(Q_TRUE, "true").
-define(Q_STREAM, "stream").
-define(Q_VTAG,  "vtag").
-define(Q_RETURNBODY, "returnbody").
-define(Q_2I_RETURNTERMS, "return_terms").
-define(Q_2I_MAX_RESULTS, "max_results").
-define(Q_2I_TERM_REGEX, "term_regex").
-define(Q_2I_CONTINUATION, "continuation").
-define(Q_2I_PAGINATION_SORT, "pagination_sort").
-define(Q_RESULTS,  "results").
-define(Q_RETURNVALUE, "returnvalue").
-define(Q_2I_MAPFOLD, "mapfold").
-define(Q_MF_MAPFOLDMOD, "mapfoldmod").
-define(Q_MF_MAPFOLDOPTS, "mapfoldoptions").
-define(Q_AAEFOLD_FILTER, "filter").
-define(Q_OBJECT_FORMAT, "object_format").
-define(Q_NVAL, "nval").
