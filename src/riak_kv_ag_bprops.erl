%% --------------------------------------------------------
%%
%% Copyright (c) 2026 Martin Sumner
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
%% @doc Handler for HTTP bucket properties fetch/store

-module(riak_kv_ag_bprops).

-include("riak_kv_web.hrl").

-behaviour(riak_api_web_handler).

-export(
    [
        match_route/3,
        check_permissions/5,
        parse_query_params/2,
        parse_request_headers/2,
        process_request/2,
        record_request/3
    ]
).

-record(context, {
    client = riak_client:new(node(), self()) :: riak_client:riak_client(),
    bucket :: riak_object:bucket(),
    op :: get | set | reset
}).

-type context() :: #context{}.

%% ===================================================================
%% Callback functions
%% ===================================================================

-spec match_route(
    riak_api_web_acceptor:method(),
    unicode:chardata(),
    list(unicode:chardata())
) ->
    nomatch
    | {method_not_allowed, list(riak_api_web_acceptor:method())}
    | {ok, riak_api_web_handler:limits(), context()}.
match_route('PUT', _, [<<"types">>, T, <<"buckets">>, B, <<"props">>]) ->
    {
        ok,
        {32, 2048, 64 * 1024},
        #context{bucket = riak_kv_web_common:set_bucket(T, B), op = set}
    };
match_route('GET', _, [<<"types">>, T, <<"buckets">>, B, <<"props">>]) ->
    {
        ok,
        {32, 2048, 0},
        #context{bucket = riak_kv_web_common:set_bucket(T, B), op = get}
    };
match_route('DELETE', _, [<<"types">>, T, <<"buckets">>, B, <<"props">>]) ->
    {
        ok,
        {32, 2048, 0},
        #context{bucket = riak_kv_web_common:set_bucket(T, B), op = reset}
    };
match_route(_, _, [<<"types">>, _T, <<"buckets">>, _B, <<"props">>]) ->
    {method_not_allowed, ['GET', 'PUT', 'DELETE']};
match_route(Method, Path, [<<"buckets">>, B, <<"props">>]) ->
    match_route(
        Method,
        Path,
        [<<"types">>, <<"default">>, <<"buckets">>, B, <<"props">>]
    );
match_route(_, _, _) ->
    nomatch.

%% @doc check_permissions for using this module or route
-spec check_permissions(
    riak_api_web_headers:headers(),
    riak_api_web_socket:scheme(),
    riak_api_web_handler:peer_ip(),
    riak_api_web_handler:peer_cert(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
check_permissions(ReqHeaders, Scheme, Peer, _Cert, Ctx) ->
    Grant =
        case Ctx#context.op of
            get ->
                "riak_core.get_bucket";
            Op when Op == set; Op == reset ->
                "riak_core.set_bucket"
        end,
    Check =
        riak_kv_web_common:check_permissions(
            ReqHeaders,
            Scheme,
            Peer,
            Ctx#context.bucket,
            Grant
        ),
    case Check of
        true ->
            {ok, Ctx};
        HaltResponse ->
            HaltResponse
    end.

%% @doc parse and validate query params, passed as a map
-spec parse_query_params(
    riak_api_web_handler:query_params(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
parse_query_params(_Params, Ctx) ->
    {ok, Ctx}.

%% @doc parse and validate the request headers
-spec parse_request_headers(
    riak_api_web_headers:headers(),
    context()
) ->
    {ok, context()} | riak_api_web_acceptor:halt_response().
parse_request_headers(ReqHeaders, #context{op = get} = Ctx) ->
    case riak_kv_web_common:accept_json_only(ReqHeaders) of
        ok ->
            {ok, Ctx};
        HaltResponse ->
            HaltResponse
    end;
parse_request_headers(_ReqHeaders, Ctx) ->
    {ok, Ctx}.

%% @doc Process the request and produce a response
-spec process_request(
    riak_api_web_body:req_body() | none,
    context()
) ->
    {
        ok,
        {
            riak_api_web_acceptor:response_code(),
            riak_api_web_headers:header_list(),
            riak_api_web_handler:response_body(),
            boolean(),
            riak_api_web_body:req_body() | none
        },
        context()
    }
    | riak_api_web_acceptor:halt_response().
process_request(none, #context{op = get} = Ctx) ->
    Props1 = riak_client:get_bucket(Ctx#context.bucket, Ctx#context.client),
    {ok, {200, [?TXT_HEADER], encode_properties(Props1), true, none}, Ctx};
process_request(none, #context{op = reset} = Ctx) ->
    riak_client:reset_bucket(Ctx#context.bucket, Ctx#context.client),
    {ok, {204, [], <<>>, true, none}, Ctx};
process_request(RqBdy, #context{op = set} = Ctx) when RqBdy =/= none ->
    case riak_api_web_body:get_body(RqBdy, all, 10000) of
        {error, content_too_large} ->
            {halt, 413, [], <<>>, []};
        {ObjBody, UpdRqBody} when is_binary(ObjBody) ->
            case safe_apply(ObjBody, Ctx) of
                ok ->
                    {ok, {204, [], <<>>, true, UpdRqBody}, Ctx};
                {error, Details} ->
                    JSON = iolist_to_binary(riak_kv_wm_json:encode(Details)),
                    {halt, 400, [?JSN_HEADER], JSON, []}
            end
    end.

%% @doc Record the output of the interaction
-spec record_request(
    riak_api_web_handler:timings(),
    riak_api_web_handler:completion(),
    context()
) ->
    ok.
record_request(_Timings, _Completion, _Ctx) ->
    ok.

%% ===================================================================
%% Internal Functions
%% ===================================================================

-spec safe_apply(binary(), context()) -> ok | {error, map()}.
safe_apply(ObjBody, Ctx) ->
    try
        ErlPropList = decode_properties(ObjBody),
        riak_client:set_bucket(
            Ctx#context.bucket,
            ErlPropList,
            Ctx#context.client
        )
    catch
        _:_ ->
            {error, #{error => <<"decode failure">>}}
    end.

-spec encode_properties(list({atom(), any() | {atom(), atom()}})) -> binary().
encode_properties(BucketProps) ->
    JsonReadyProps =
        lists:filter(
            fun(T) -> T =/= none end,
            lists:map(fun jsonify_bucket_prop/1, BucketProps)
        ),
    iolist_to_binary(
        riak_kv_wm_json:encode(
            #{<<"props">> => maps:from_list(JsonReadyProps)}
        )
    ).

-spec decode_properties(binary()) -> list({atom(), any() | {atom(), atom()}}).
decode_properties(Json) ->
    lists:map(
        fun erlify_bucket_prop/1,
        maps:to_list(
            maps:get(<<"props">>, riak_kv_wm_json:decode(Json))
        )
    ).

-spec jsonify_bucket_prop(
    {atom(), any() | {atom(), atom()}}
) ->
    {binary(), any()} | none.
jsonify_bucket_prop({linkfun, _}) ->
    none;
jsonify_bucket_prop({chash_keyfun, {Mod, Fun}}) when
    is_atom(Mod), is_atom(Fun)
->
    {
        ?JSON_CHASH,
        #{
            ?JSON_MOD => atom_to_binary(Mod, utf8),
            ?JSON_FUN => atom_to_binary(Fun, utf8)
        }
    };
jsonify_bucket_prop({rs_extractfun, _}) ->
    none;
jsonify_bucket_prop({search_extractor, _}) ->
    none;
jsonify_bucket_prop({name, {_T, B}}) when is_binary(B) ->
    {<<"name">>, B};
jsonify_bucket_prop({Prop, Value}) ->
    {atom_to_binary(Prop, utf8), Value}.

-spec erlify_bucket_prop(
    {binary(), any()}
) ->
    {atom(), any() | {atom(), atom()}}.
erlify_bucket_prop({?JSON_DATATYPE, Type}) when is_binary(Type) ->
    {datatype, binary_to_existing_atom(Type, utf8)};
erlify_bucket_prop({?JSON_CHASH, Props}) ->
    {
        chash_keyfun,
        {
            binary_to_existing_atom(maps:get(?JSON_MOD, Props)),
            binary_to_existing_atom(maps:get(?JSON_FUN, Props))
        }
    };
erlify_bucket_prop({Prop, Value}) when is_binary(Value) ->
    {
        binary_to_existing_atom(Prop),
        binary_to_existing_atom(Value)
    };
erlify_bucket_prop({Prop, Value}) when is_integer(Value); is_boolean(Value) ->
    {
        binary_to_existing_atom(Prop),
        Value
    }.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

circle_default_props_test() ->
    BucketProps =
        [
            {linkfun, {modfun, riak_kv_wm_link_walker, mapreduce_linkfun}},
            {old_vclock, 86400},
            {young_vclock, 20},
            {big_vclock, 50},
            {small_vclock, 50},
            {pr, 0},
            {r, quorum},
            {w, quorum},
            {pw, 0},
            {node_confirms, 0},
            {dw, quorum},
            {rw, quorum},
            {basic_quorum, false},
            {notfound_ok, true}
        ],
    Json = encode_properties(BucketProps),
    SupportedProps = lists:sort(lists:keydelete(linkfun, 1, BucketProps)),
    CircleProps = decode_properties(Json),
    ?assertMatch(SupportedProps, lists:sort(CircleProps)).

circle_special_props_test() ->
    BucketProps =
        [
            {rs_extractfun, modfun},
            {search_extractor, modfun},
            {chash_keyfun, {keymod, keymodfun}},
            {datatype, counter}
        ],
    SupportedProps =
        lists:sort(
            lists:keydelete(
                rs_extractfun,
                1,
                lists:keydelete(search_extractor, 1, BucketProps)
            )
        ),
    Json = encode_properties(BucketProps),
    CircleProps = decode_properties(Json),
    ?assertMatch(SupportedProps, lists:sort(CircleProps)).

bad_special_props_test() ->
    LinkJson =
        <<
            "{\"props\":"
            "[{\"linkfun\":{\"fun\":\"linkmodfun\",\"mod\":\"linkmod\"}}]}"
        >>,
    ?assertMatch(
        {error, #{error := <<"decode failure">>}},
        safe_apply(LinkJson, #context{bucket = <<"B">>, op = set})
    ).

-endif.
