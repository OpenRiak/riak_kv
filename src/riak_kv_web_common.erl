%% -------------------------------------------------------------------
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
%% @doc common functions used by web API handlers

-module(riak_kv_web_common).

-include("riak_kv_web.hrl").

-export(
    [
        check_permissions/5,
        set_bucket/2,
        check_type_exists/1,
        count_fold/3,
        boolean_fold/3,
        confirm_empty_body/1,
        get_client/0
    ]
).

-define(BAD_COUNT_PARAM_TEXT, <<
    "~0p query parameter must be an integer or "
    "one of the following words: 'one', 'quorum' or 'all'"
>>).

-define(BAD_BOOLEAN_PARAM_TEXT, <<
    "~0p query parameter must be true or false"
>>).

-spec confirm_empty_body(
    riak_api_web_body:req_body()
) ->
    {ok, riak_api_web_body:req_body()} | {error, content_too_large}.
confirm_empty_body(ReqBody) ->
    case riak_api_web_body:get_body(ReqBody, all, 10000) of
        {done, UpdBody} ->
            {ok, UpdBody};
        {<<>>, UpdBody} ->
            confirm_empty_body(UpdBody);
        {error, content_too_large} ->
            {error, content_too_large}
    end.

-spec check_permissions(
    riak_api_web_headers:headers(),
    riak_api_web_socket:scheme(),
    riak_api_web_handler:peer_ip(),
    riak_object:bucket(),
    atom()
) ->
    true | riak_api_web_acceptor:halt_response().
check_permissions(ReqHeaders, Scheme, Peer, Bucket, PermissionRequired) ->
    Authorised =
        riak_api_web_security:is_authorised(
            riak_core_security:is_enabled(),
            Scheme,
            ReqHeaders,
            Peer
        ),
    case Authorised of
        {ok, undefined} ->
            true;
        {ok, SecContext} ->
            PermissionGranted =
                riak_core_security:check_permission(
                    {PermissionRequired, Bucket},
                    SecContext
                ),
            case PermissionGranted of
                {true, _SecContext} ->
                    true;
                {false, Error, _SecContext} when is_binary(Error) ->
                    {halt, 403, [?TXT_HEADER], Error, []}
            end;
        {halt, RC, RH, RB, RS} ->
            {halt, RC, RH, RB, RS}
    end.

-spec set_bucket(binary(), binary()) -> riak_object:bucket().
set_bucket(<<"default">>, Bucket) ->
    Bucket;
set_bucket(BucketType, Bucket) ->
    {BucketType, Bucket}.

-spec check_type_exists(riak_object:bucket()) -> boolean().
check_type_exists({Type, _Bucket}) ->
    case riak_core_bucket_type:get(Type) of
        undefined ->
            % Not that this fetches the properties for the type from
            % the metadata - which may be an unnecessary cost.
            false;
        _ ->
            true
    end;
check_type_exists(Bucket) when is_binary(Bucket) ->
    true.

-spec count_fold(
    riak_api_web_handler:query_params(),
    list(binary()),
    #{atom() => term()}
) ->
    #{atom() => term()} | riak_api_web_acceptor:halt_response().
count_fold([], _CountKeys, Opts) ->
    Opts;
count_fold([{PK, PV} | Rest], CountKeys, Opts) when is_map(Opts) ->
    case lists:member(PK, CountKeys) of
        false ->
            count_fold(Rest, CountKeys, Opts);
        true ->
            case normalise_rw_param(PV) of
                bad_param ->
                    {halt, 401, [?TXT_HEADER], ?BAD_COUNT_PARAM_TEXT, [PK]};
                V ->
                    count_fold(
                        Rest,
                        CountKeys,
                        maps:put(binary_to_existing_atom(PK), V, Opts)
                    )
            end
    end.

-spec boolean_fold(
    riak_api_web_handler:query_params(),
    list(binary()),
    #{atom() => term()}
) ->
    #{atom() => term()} | riak_api_web_acceptor:halt_response().
boolean_fold([], _BoolKeys, Opts) ->
    Opts;
boolean_fold([{PK, PV} | Rest], BoolKeys, Opts) when is_map(Opts) ->
    case lists:member(PK, BoolKeys) of
        false ->
            boolean_fold(Rest, BoolKeys, Opts);
        true ->
            case normalise_boolean_param(PV) of
                bad_param ->
                    {halt, 401, [?TXT_HEADER], ?BAD_BOOLEAN_PARAM_TEXT, [PK]};
                V ->
                    boolean_fold(
                        Rest,
                        BoolKeys,
                        maps:put(binary_to_existing_atom(PK), V, Opts)
                    )
            end
    end.

normalise_rw_param(<<"default">>) ->
    default;
normalise_rw_param(<<"one">>) ->
    one;
normalise_rw_param(<<"quorum">>) ->
    quorum;
normalise_rw_param(<<"all">>) ->
    all;
normalise_rw_param(V) when is_binary(V) ->
    try
        case binary_to_integer(V) of
            I when I >= 0 ->
                I
        end
    catch
        _:_ ->
            bad_param
    end;
normalise_rw_param(_) ->
    bad_param.

normalise_boolean_param(true) ->
    true;
normalise_boolean_param(V) when is_binary(V) ->
    case string:casefold(V) of
        <<"true">> ->
            true;
        <<"false">> ->
            false;
        <<"default">> ->
            default;
        _ ->
            bad_param
    end.

get_client() ->
    {ok, C} = riak:local_client(),
    C.
