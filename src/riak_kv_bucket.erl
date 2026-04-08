%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011-2016 Basho Technologies, Inc.
%% Copyright (c) 2024-2025 Workday, Inc.
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
%% @doc KV Bucket validation functions
%%
-module(riak_kv_bucket).

-export([validate/4]).

%% helper functions exports
-export([allow_mult/1]).

-export_type([props/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("riak_kv_types.hrl").

-type propvalue() :: PropValue::any().
-type prop() :: {PropName::atom(), propvalue()}.
-type error_reason() :: atom() | string().
-type error() :: {PropName::atom(), ErrorReason::error_reason()}.
-type props() :: [prop()].
-type errors() :: [error()].
-type datatype_names() :: [map|set|counter|pncounter|register|flag|string()].
-type dt_props_check() :: {datatype|allow_mult,
                           ValidFun :: fun((propvalue(), propvalue()) ->
                                                  ok|false|error|undefined|
                                                  atom()) |
                                       fun((propvalue(), propvalue(),
                                            DT_MOD::module()) ->
                                                  ok|false|error|undefined|
                                                  atom()),
                           string()|fun((...) -> string())}.

-type validate_dt_props_return() :: {UnvalidatedProps :: props(),
                                     ValidatedProps :: props(),
                                     ErrorsGenerated :: errors(),
                                     ExisitingProps :: props()}.
-type validate_props_return() :: {UnvalidatedProps :: props(),
                                  ValidatedProps :: props(),
                                  ErrorsGenerated :: errors()}.
-define(ERROR_ALLOW_MULT_CREATE, "Data Type buckets must be" ++
            " allow_mult=true").
-define(DT_PROPS_CHECK_CREATE, [{datatype, fun datatype/2,
                                fun error_dt_create/1},
                                {allow_mult, fun allow_mult/3,
                                 ?ERROR_ALLOW_MULT_CREATE}]).


%% @doc called by riak_core in a few places to ensure bucket
%%  properties are sane. The arguments combinations have the following
%%  meanings:-
%%
%% The first argument is the `Phase' of the bucket/bucket type
%% mutation and can be either `create' or `update'.
%%
%% `create' always means that we are creating a new bucket type or
%% updating an inactive bucket type. In the first case `Existing' is
%% the atom `undefined', in the second it is a list of the valid
%% properties returned from the first invocation of `validate/4'. The
%% value of `Bucket' will only ever be a two-tuple of `{binary(),
%% undefined}' for create, as it is only used on bucket types. The
%% final argument `BucketProps' is a list of the properties the user
%% provided for type creation merged with the default properties
%% defined in `riak_core_bucket_type:defaults/0' The job of the
%% function is to validate the given `BucketProps' and return a two
%% tuple `{Good, Bad}' where the first element is the list of valid
%% properties and the second a list of `error()' tuples. Riak_Core
%% will store the `Good' list in metadata iif the `Bad' list is the
%% empty list. It is worth noting that on `create' we must ignore the
%% `Existing' argument altogether.
%%
%% `update' means that we are either updating a bucket type or a
%% bucket. If `Bucket' is a `binary()' or a tuple `{binary(),
%% binary()}' then, a bucket is being updated. If `bucket' is a two
%% tuple of `{binary(), undefined}' then a bucket type is being
%% updated. When `validate/4' is called with `update' as the phase
%% then `Existing' will be the set of properties stored in metadata
%% for this bucket (the set returned as `Good' from the `create'
%% phase) and `BucketProps' will be ONLY the properties that user has
%% supplied as those to update (note: update may mean adding new
%% properties.) The job of `validate/4' in this case is to validate
%% the new properties and return a complete set of bucket properties
%% (ie the new properties merged with the existing propeties) in
%% `Good', riak will then persist these `Good' properties, providing
%% `Bad' is empty.
%%
%% `validate/4' can be used to enforce immutable or co-invariant bucket
%% properties, like "only non-default bucket types can have a
%% `datatype' property", and that "`datatype' buckets must be
%% allow_mult" and "once set, `datatype' cannot be changed".
%%
%% There is no way to _remove_ a property
%%
%% @see validate_create_dt_props/1
%% @see validate_udpate_dt_props/2
%% @see validate_dt_props/2
%% @see assert_no_datatype/1
-spec validate(create | update,
               {riak_core_bucket_type:bucket_type(), undefined | binary()} | binary(),
               undefined | props(),
               props()) -> {props(), errors()}.
validate(create, _Bucket, _Existing, BucketProps) when is_list(BucketProps) ->
    validate_create_bucket_type(BucketProps);
validate(update, {_TypeName, undefined}, Existing, New) when is_list(Existing),
                                                            is_list(New) ->
    validate_update_bucket_type(Existing, New);
validate(update, {Type, Name}, Existing, New) when is_list(Existing),
                                                   is_list(New),
                                                   is_binary(Name),
                                                   Type /= <<"default">> ->
    validate_update_typed_bucket(Existing, New);
validate(update, _Bucket, Existing, New) when is_list(Existing),
                                             is_list(New) ->
    validate_default_bucket(Existing, New).

%% @private bucket creation time validation
-spec validate_create_bucket_type(props()) -> {props(), errors()}.
validate_create_bucket_type(BucketProps) ->
    {Unvalidated, Valid, Errors} = validate_create_dt_props(BucketProps),
    {Good, Bad} = validate(Unvalidated, Valid, Errors),
    validate_post_merge(Good, Bad).

%% @private update phase of bucket type. Merges properties from
%% existing with valid new properties. Existing can be assumed valid,
%% since they were validated by the `create' phase.
-spec validate_update_bucket_type(props(), props()) -> {props(), errors()}.
validate_update_bucket_type(Existing, New) ->
    Type = type(Existing),
    {Unvalidated, Valid, Errors} = validate_update_type(Type, Existing, New),
    {Good, Bad} = validate(Unvalidated, Valid, Errors),
    validate_post_merge(merge(Good, Existing), Bad).

%% @private pick the validation function depending on existing type.
-spec validate_update_type(Type :: datatype | default,
                           Existing :: props(),
                           New :: props()) ->
                                  {Unvalidated :: props(),
                                   Valid  :: props(),
                                   Errors :: props()}.
validate_update_type(datatype, Existing, New) ->
    validate_update_dt_props(Existing, New);
validate_update_type(default, _Existing, New) ->
    validate_update_default_props(New).

%% @private figure out what `type' the existing bucket is.  NOTE: only
%% call with validated props from existing buckets!!
-spec type(props()) -> default | datatype.
type(Props) ->
    case proplists:get_value(datatype, Props, false) of
        false ->
            default;
        true ->
            datatype
    end.

%% @private just delegates, but I added it to illustrate the many
%% possible type of validation.
-spec validate_update_typed_bucket(props(), props()) -> {props(), errors()}.
validate_update_typed_bucket(Existing, New) ->
    {Good, Bad} = validate_update_bucket_type(Existing, New),
    validate_post_merge(Good, Bad).

%% @private as far as datatypes go, default buckets are free to do as
%% they please, the datatypes API only works on typed buckets. Go
%% wild!
-spec validate_default_bucket(props(), props()) -> {props(), errors()}.
validate_default_bucket(Existing, New) ->
    {Good, Bad} = validate(New, [], []),
    validate_post_merge(merge(Good, Existing), Bad).

%% @private properties in new overwrite those in old
-spec merge(props(), props()) -> props().
merge(New, Old) ->
    riak_core_bucket_props:merge(New, Old).

%% @private general property validation
-spec validate(InProps::props(), ValidProps::props(), Errors::errors()) ->
                      {props(), errors()}.
validate([], ValidProps, Errors) ->
    {ValidProps, Errors};
validate([{BoolProp, MaybeBool}|T], ValidProps, Errors)
        when 
            is_atom(BoolProp), BoolProp =:= allow_mult
            orelse BoolProp =:= basic_quorum
            orelse BoolProp =:= last_write_wins
            orelse BoolProp =:= notfound_ok
            orelse BoolProp =:= stat_tracked
            orelse BoolProp =:= aae_tree_exclude ->
    case coerce_bool(MaybeBool) of
        error ->
            validate(T, ValidProps, [{BoolProp, not_boolean}|Errors]);
        Bool ->
            validate(T, [{BoolProp, Bool}|ValidProps], Errors)
    end;
validate([{IntProp, MaybeInt}=Prop | T], ValidProps, Errors) when IntProp =:= big_vclock
                                                                  orelse IntProp =:= n_val
                                                                  orelse IntProp =:= old_vclock
                                                                  orelse IntProp =:= small_vclock ->
    case is_integer(MaybeInt) of
        true when MaybeInt > 0 ->
            validate(T, [Prop | ValidProps], Errors);
        _ ->
            validate(T, ValidProps, [{IntProp, not_integer} | Errors])
    end;
validate([{QProp, MaybeQ}=Prop | T], ValidProps, Errors) when  QProp =:= r
                                                              orelse QProp =:= rw
                                                              orelse QProp =:= w ->
    case is_quorum(MaybeQ) of
        true ->
            validate(T, [Prop | ValidProps], Errors);
        false ->
            validate(T, ValidProps, [{QProp, not_valid_quorum} | Errors])
    end;
validate([{QProp, MaybeQ}=Prop | T], ValidProps, Errors) when QProp =:= dw
                                                              orelse QProp =:= pw
                                                              orelse QProp =:= node_confirms
                                                              orelse QProp =:= pr ->
    case is_opt_quorum(MaybeQ) of
        true ->
            validate(T, [Prop | ValidProps], Errors);
        false ->
            validate(T, ValidProps, [{QProp, not_valid_quorum} | Errors])
    end;
validate([{sync_on_write, MaybeSync}=Prop | T], ValidProps, Errors) ->
    case is_valid_sync_param(MaybeSync) of
        true ->
            validate(T, [Prop | ValidProps], Errors);
        false ->
            validate(T, ValidProps, [{sync_on_write, not_valid_sync_param} | Errors])
    end;
validate([Prop|T], ValidProps, Errors) ->
    validate(T, [Prop|ValidProps], Errors).


-spec is_quorum(term()) -> boolean().
is_quorum(Q) when is_integer(Q), Q > 0 ->
    true;
is_quorum(Q)  when Q =:= quorum
                   orelse Q =:= one
                   orelse Q =:= all
                   orelse Q =:= <<"quorum">>
                   orelse Q =:= <<"one">>
                   orelse Q =:= <<"all">> ->
    true;
is_quorum(_) ->
    false.

%% validation of sync parameters
%% one = sync coordinating node only
%% all = sync all nodes
%% backend = take sync value for all nodes from backend config (don't override)
-spec is_valid_sync_param(term()) -> boolean().
is_valid_sync_param(SP) when SP =:= one
                        orelse SP =:= all
                        orelse SP =:= backend
                        orelse SP =:= <<"one">>
                        orelse SP =:= <<"all">>
                        orelse SP =:= <<"backend">> ->
   true;
is_valid_sync_param(_) ->
   false.

%% @private some quorum options can be zero
-spec is_opt_quorum(term()) -> boolean().
is_opt_quorum(Q) when is_integer(Q), Q >= 0 ->
    true;
is_opt_quorum(Q) ->
    is_quorum(Q).

-spec coerce_bool(any()) -> boolean() | error.
coerce_bool(true) ->
    true;
coerce_bool(false) ->
    false;
coerce_bool(MaybeBool) when is_atom(MaybeBool) ->
     coerce_bool(atom_to_list(MaybeBool));
coerce_bool(MaybeBool) when is_binary(MaybeBool) ->
    coerce_bool(binary_to_list(MaybeBool));
coerce_bool(Int) when is_integer(Int), Int =< 0 ->
    false;
coerce_bool(Int) when is_integer(Int) , Int > 0 ->
    true;
coerce_bool(MaybeBool) when is_list(MaybeBool) ->
    Lower = string:to_lower(MaybeBool),
    Atom = (catch list_to_existing_atom(Lower)),
    case Atom of
        true -> true;
        false -> false;
        _ -> error
    end;
coerce_bool(_) ->
    error.

%% @private riak datatype support requires a bucket type of `datatype'
%% and `allow_mult' set to `true'. These function enforces those
%% properties, as well as specific ones for certain datatypes
%%
%% We take the presence of a `datatype' property as indication that
%% this bucket type is a special type, somewhere to store CRDTs. I
%% realise this slightly undermines the reason for bucket types (no
%% magic names) but there has to be some way to indicate intent, and
%% that way is the "special" property name `datatype'.
%%
%% Since we don't ever want sibling CRDT types (though we can handle
%% them: see riak_kv_crdt), `datatype' is an immutable property. Once
%% you create a bucket with a certain datatype you can't change
%% it. The `update' bucket type path enforces this. It doesn't
%% validate the correctness of the type, since it assumes that was
%% done at creation, only that it is either the same as existing or
%% not present.
%%
%% For creation, we fold over a proplist of a 3-tuples, with each 3-tuple
%% consisting of a property, a function to validate that property, and
%% an error (either an error string or function to return an error) to return
%% if the property is invalid.
%%
%% Example proplist to fold over:
%%
%% [{datatype, fun datatype/2, fun error_dt_create/1},
%%  {allow_mult, fun allow_mult/3, "Bad Bad Bad"]
%%
%% @see dt_props_check/0 for properties we handle currently,
%%      function-aritys/inputs/outputs.
%%
%% And, our accumulator is a tuple consiting of our *New*, unvalidated,
%% bucket props, and empty lists ready to accumulate valid and error
%% properties. The fourth empty list is for existing properties, but
%% create won't deal with this.
-spec validate_create_dt_props(NewProps :: props()) -> validate_props_return().
validate_create_dt_props(New) ->
    case proplists:get_value(datatype, New) of
        undefined -> {New, [], []};
        _ ->
            {Unvalidated, Valid, Errors, _} =
                lists:foldl(fun validate_dt_props/2, {New, [], [], []},
                            ?DT_PROPS_CHECK_CREATE),
            {Unvalidated, Valid, Errors}
    end.

%% @private generalized validation function for checking multiple datatype
%%          properties.
%%
%% *API*
%%
%% This Function takes in tuples with a
%% - property (e.g. datatype, allow_mult)
%% - a validation function that must return either ok, false, error, or
%%   undefined
%% - and an Error to return that may be a string or a function
%%   (for varying errors)
%%
%% Right now, this allows for taking in a function for validation that is of
%% an arity 2 or 3, with the 2-arity being specific for our is a defined
%% datatype-check.
%%
%% The validation function has 5 possible returns, some w/ similar meanings:
%% - ok or undefined -> let it pass, is not to be accumulated
%% - error or false -> not a valid property, return an error and accumulate
%%   that prop with *Errors*
%% - a value -> the value of the property that we want to accumulate in our
%%   *Valid* list
-spec validate_dt_props(dt_props_check(),
                        {NewOrUnvalidatedProps :: props(),
                         ValidatedProps :: props(),
                         ErrorsGenerated :: errors(),
                         ExisitingProps :: props()})
                       -> validate_dt_props_return().
validate_dt_props(PropCheck, {Unvalidated0, Valid, Errors, Existing}) ->
    {Prop, Fun, Err0} = PropCheck,
    PropVal = proplists:get_value(Prop, Unvalidated0),
    ExistingVal = proplists:get_value(Prop, Existing),
    Unvalidated1 = lists:keydelete(Prop, 1, Unvalidated0),
    FunVal = case Prop of
                 datatype ->
                     %% Call are 2-arity, defined datatype function
                     Fun(PropVal, ExistingVal);
                 _ ->
                     DataTypeMod = riak_kv_crdt:to_mod(
                                     proplists:get_value(
                                       datatype, Valid,
                                       proplists:get_value(datatype, Existing))
                                    ),
                     Fun(PropVal, ExistingVal, DataTypeMod)
             end,
    case {FunVal==ok orelse FunVal==undefined,
          FunVal==error orelse FunVal==false} of
        {true, _} ->
            {Unvalidated1, Valid, Errors, Existing};
        {_, false} ->
            {Unvalidated1, [{Prop, FunVal} | Valid], Errors, Existing};
        {_, true} ->
            Err1 = case is_function(Err0) of
                       true ->
                           Err0(PropVal);
                       false ->
                           Err0
                   end,
            {Unvalidated1, Valid, [{Prop, Err1} | Errors], Existing}
    end.

%% @private checks that a bucket that is not a special immutable type
%% is not attempting to become one.
-spec validate_update_default_props(New :: props()) ->
                                           {Unvalidated :: props(),
                                            Valid :: props(),
                                            Error :: props()}.
validate_update_default_props(New) ->
    %% Only called if not already a datatype
    %% bucket. Check that none of those are being set to `true'/valid
    %% datatypes.
    ensure_not_present(
        New, [], [], [{datatype, "`datatype` must not be defined."}]
    ).

%% @private Check specifically against existing vs "new" datatype updates,
%% which are not allowed, then call validate_dt_props/2 for the
%% dt-property fold over.
%%
%% @see validate_create_dt_props/1 for more comments and information.
%%
%% This function is treated much like the *create-version*, but we know
%% have Existing properties to handle along with our *Newer*, unvalidated
%% properties.
-spec validate_update_dt_props(ExistingProps :: props(),
                               NewProps :: props()) -> validate_props_return().
validate_update_dt_props(Existing, New) ->
    {Unvalidated, Valid, Errors, _} =
        lists:foldl(fun validate_dt_props/2, {New, [], [], Existing},
                    ?DT_PROPS_CHECK_CREATE),
    {Unvalidated, Valid, Errors}.

%% @private any property in `InvalidPropsSpec' present in
%% `Unvalidated' will be added to `Errors'. Returned is the as yet
%% unvalidated remainder properties from `Unvalidated', the properties
%% from `InvalidPropsSpec' that were present and not invalid added to
%% `Valid' and the accumulated errors added to `Errors'.
-spec ensure_not_present(props(), props(), props(), [{atom(), term(), string()} |
                                                     {atom(), term()}]) ->
                                {props(), props(), props()}.
ensure_not_present(Unvalidated, Valid, Errors, InvalidPropsSpec) ->
    lists:foldl(fun({Key, NotAllowed, ErrorMessage}, {U, V, E}) ->
                        case lists:keytake(Key, 1, U) of
                            false ->
                                {U, V, E};
                            {value, {Key, Val}, U2} ->
                                Val2 = coerce_bool(Val),
                                if Val2 == NotAllowed ->
                                        {U2, V, [{Key, ErrorMessage} | E]};
                                   true ->
                                        {U, V, E}
                                end
                        end;
                   ({Key, ErrorMessage}, {U, V, E}) ->
                        case lists:keytake(Key, 1, U) of
                            false -> {U, V, E};
                            {value, {Key, _Val}, U2} ->
                                {U2, V, [{Key, ErrorMessage} | E]}
                        end
                end,
                {Unvalidated, Valid, Errors},
                InvalidPropsSpec).

%% Validate properties after they have all been individually validated, merged,
%% and resolved to their final values. This allows for identifying invalid
%% combinations of properties, such as `last_write_wins=true' and
%% `dvv_enabled=true'.
-spec validate_post_merge(props(), errors()) -> {props(), errors()}.
validate_post_merge(Props, Errors) ->
    %% Currently, we only have one validation rule to apply at this stage, so
    %% just call the validation function directly. If more are added in the
    %% future, it would be good to use function composition to compose the
    %% individual validation functions into a single function.
    validate_last_write_wins_implies_not_dvv_enabled({Props, Errors}).

%% If `last_write_wins' is true, `dvv_enabled' must not also be true.
-spec validate_last_write_wins_implies_not_dvv_enabled({props(), errors()}) -> {props(), errors()}.
validate_last_write_wins_implies_not_dvv_enabled({Props, Errors}) ->
    case {last_write_wins(Props), dvv_enabled(Props)} of
        {true, true} ->
            {lists:keydelete(dvv_enabled, 1, Props),
             [{dvv_enabled,
               "If last_write_wins is true, dvv_enabled must be false"}
              |Errors]};
        {_, _} ->
            {Props, Errors}
    end.

%% @doc See if datatype is valid, if so return the datatype, otherwise
%%      false for handling.
-spec datatype(props()|datatype_names(), props()|datatype_names()) ->
                      datatype_names() | false.
datatype(PropsNew, PropsOld) when is_list(PropsOld), is_list(PropsNew) ->
    datatype(proplists:get_value(datatype, PropsNew),
             proplists:get_value(datatype, PropsOld));
datatype(DataTypeNew, undefined) ->
    case riak_kv_crdt:supported(riak_kv_crdt:to_mod(DataTypeNew)) of
        true ->
            DataTypeNew;
        false ->
            false
    end;
datatype(undefined, DataTypeOld) ->
    DataTypeOld;
datatype(DataTypeNew, DataTypeOld) ->
    case DataTypeNew =:= DataTypeOld of
        true -> DataTypeNew;
        false -> false
    end.

%% @doc Just grab the allow_mult value if it exists
-spec allow_mult(props()) -> boolean() | undefined | error.
allow_mult(Props) when is_list(Props) ->
    MultProp = proplists:get_value(allow_mult, Props),
    allow_mult(MultProp, undefined, undefined).
-spec allow_mult(propvalue(), undefined, undefined) ->
                        boolean() | undefined | error.
allow_mult(Prop, _PropOld, _Mod) ->
    case Prop of
        undefined ->
            undefined;
        MaybeBool ->
            coerce_bool(MaybeBool)
    end.

%% @doc Error function for datatype creation.
-spec error_dt_create(datatype_names()) -> string().
error_dt_create(DataType) ->
    lists:flatten(io_lib:format("~p not supported for bucket datatype property",
                                [DataType])).

%% Boolean value of the `last_write_wins' property, or `undefined' if not present.
-spec last_write_wins(props()) -> boolean() | 'undefined' | 'error'.
last_write_wins(Props) ->
    get_boolean(last_write_wins, Props).

%% Boolean value of the `dvv_enabled' property, or `undefined' if not present.
-spec dvv_enabled(props()) -> boolean() | 'undefined' | 'error'.
dvv_enabled(Props) ->
    get_boolean(dvv_enabled, Props).

%% @private coerce the value under key to be a boolean, if defined; undefined, otherwise.
-spec get_boolean(PropName::atom(), props()) -> boolean() | 'undefined' | 'error'.
get_boolean(Key, Props) ->
    case proplists:get_value(Key, Props) of
        undefined ->
            undefined;
        MaybeBool ->
            coerce_bool(MaybeBool)
    end.

%%
%% EUNIT tests...
%%

-ifdef (TEST).

coerce_bool_test_ () ->
    [?_assertEqual(false, coerce_bool(false)),
     ?_assertEqual(true, coerce_bool(true)),
     ?_assertEqual(true, coerce_bool("True")),
     ?_assertEqual(false, coerce_bool("fAlSE")),
     ?_assertEqual(false, coerce_bool(<<"FAlse">>)),
     ?_assertEqual(true, coerce_bool(<<"trUe">>)),
     ?_assertEqual(true, coerce_bool(1)),
     ?_assertEqual(true, coerce_bool(234567)),
     ?_assertEqual(false, coerce_bool(0)),
     ?_assertEqual(false, coerce_bool(-1234)),
     ?_assertEqual(false, coerce_bool('FALSE')),
     ?_assertEqual(true, coerce_bool('TrUe')),
     ?_assertEqual(error, coerce_bool("Purple")),
     ?_assertEqual(error, coerce_bool(<<"frangipan">>)),
     ?_assertEqual(error, coerce_bool(erlang:make_ref()))
    ].

-endif.
