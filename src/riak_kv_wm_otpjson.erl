%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2024-2024. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% %CopyrightEnd%
%% % @format
%%
-module(riak_kv_wm_otpjson).

-dialyzer(no_improper_lists).

-export([
    encode/1, encode/2,
    encode_value/2,
    encode_atom/2,
    encode_integer/1,
    encode_float/1,
    encode_list/2,
    encode_map/2,
    encode_map_checked/2,
    encode_key_value_list/2,
    encode_key_value_list_checked/2,
    encode_binary/1,
    encode_binary_escape_all/1
]).
-export_type([encoder/0, simple_encode_value/0]).

-export([
    decode/1, decode/3
]).
-export_type([
    from_binary_fun/0,
    array_start_fun/0,
    array_push_fun/0,
    array_finish_fun/0,
    object_start_fun/0,
    object_push_fun/0,
    object_finish_fun/0,
    decoders/0
]).

-compile(warn_missing_spec).

-compile(
    {inline, [
        encode_atom/2,
        encode_integer/1,
        encode_float/1,
        encode_object/1,
        escape/1,
        escape_binary/1,
        escape_all/1,
        utf8t/0,
        utf8s/0,
        utf8s0/0,
        multibyte_size/1
    ]}
).

-include("riak_kv_wm_otpjson.hrl").
-define(UTF8_ACCEPT, 0).
-define(UTF8_REJECT, 12).

%%
%% Encoding implementation
%%

-if(?OTP_RELEASE < 26).
-type dynamic() :: term().
-endif.

-type encoder() :: fun((dynamic(), encoder()) -> iodata()).

-type simple_encode_value() ::
    integer()
    | float()
    | boolean()
    | null
    | binary()
    | atom()
    | list(simple_encode_value())
    | #{binary() | atom() | integer() => simple_encode_value()}.

% @doc
% Generates JSON corresponding to `Term`.

% Supports basic data mapping:

% | **Erlang**            | **JSON** |
% |-----------------------|----------|
% | integer() \\| float() | Number   |
% | true \\| false        | Boolean  |
% | null                  | Null     |
% | binary()              | String   |
% | atom()                | String   |
% | list()                | Array    |
% | #{binary() => _}      | Object   |
% | #{atom() => _}        | Object   |
% | #{integer() => _}     | Object   |

% This is equivalent to `encode(Term, fun json:encode_value/2)`.

-spec encode(simple_encode_value()) -> iodata().
encode(Term) -> encode(Term, fun do_encode/2).

% @doc
% Generates JSON corresponding to `Term`.

% Can be customised with the `Encoder` callback.
% The callback will be recursively called for all the data
% to be encoded and is expected to return the corresponding
% encoded values.

% Various `encode_*` functions in this module can be used
% to help in constructing such callbacks.

-spec encode(dynamic(), encoder()) -> iodata().
encode(Term, Encoder) when is_function(Encoder, 2) ->
    Encoder(Term, Encoder).

-spec encode_value(dynamic(), encoder()) -> iodata().
encode_value(Value, Encode) ->
    do_encode(Value, Encode).

-spec do_encode(dynamic(), encoder()) -> iodata().
do_encode(Value, Encode) when is_atom(Value) ->
    encode_atom(Value, Encode);
do_encode(Value, _Encode) when is_binary(Value) ->
    escape_binary(Value);
do_encode(Value, _Encode) when is_integer(Value) ->
    encode_integer(Value);
do_encode(Value, _Encode) when is_float(Value) ->
    encode_float(Value);
do_encode(Value, Encode) when is_list(Value) ->
    do_encode_list(Value, Encode);
do_encode(Value, Encode) when is_map(Value) ->
    do_encode_map(Value, Encode);
do_encode(Other, _Encode) ->
    error({unsupported_type, Other}).

-spec encode_atom(atom(), encoder()) -> iodata().
encode_atom(null, _Encode) -> <<"null">>;
encode_atom(true, _Encode) -> <<"true">>;
encode_atom(false, _Encode) -> <<"false">>;
encode_atom(Other, Encode) -> Encode(atom_to_binary(Other, utf8), Encode).

-spec encode_integer(integer()) -> iodata().
encode_integer(Integer) -> integer_to_binary(Integer).

-spec encode_float(float()) -> iodata().
-if(?OTP_RELEASE >= 25).
encode_float(Float) -> float_to_binary(Float, [short]).
-else.
% Results may be unpredictable for encioding floats
% Bodged as no float encoding required in Riak
encode_float(Float) -> float_to_binary(Float).
-endif.


-spec encode_list(list(), encoder()) -> iodata().
encode_list(List, Encode) when is_list(List) ->
    do_encode_list(List, Encode).

do_encode_list([], _Encode) ->
    <<"[]">>;
do_encode_list([First | Rest], Encode) when is_function(Encode, 2) ->
    [$[, Encode(First, Encode) | list_loop(Rest, Encode)].

list_loop([], _Encode) -> "]";
list_loop([Elem | Rest], Encode) -> [$,, Encode(Elem, Encode) | list_loop(Rest, Encode)].

-spec encode_map(map(), encoder()) -> iodata().
encode_map(Map, Encode) when is_map(Map) ->
    do_encode_map(Map, Encode).

do_encode_map(Map, Encode) when is_function(Encode, 2) ->
    encode_object(
        maps:fold(
            fun(Key, Value, Acc) ->
                [[$,, key(Key, Encode), $: | Encode(Value, Encode)]|Acc]
            end,
            [],
            Map)
    ).

-spec encode_map_checked(map(), encoder()) -> iodata().
encode_map_checked(Map, Encode) ->
    do_encode_checked(maps:to_list(Map), Encode).

-spec encode_key_value_list([{term(), term()}], encoder()) -> iodata().
encode_key_value_list(List, Encode) when is_function(Encode, 2) ->
    encode_object([[$,, key(Key, Encode), $: | Encode(Value, Encode)] || {Key, Value} <- List]).

-spec encode_key_value_list_checked([{term(), term()}], encoder()) -> iodata().
encode_key_value_list_checked(List, Encode) ->
    do_encode_checked(List, Encode).

do_encode_checked(List, Encode) when is_function(Encode, 2) ->
    do_encode_checked(List, Encode, #{}).

do_encode_checked([{Key, Value} | Rest], Encode, Visited0) ->
    EncodedKey = iolist_to_binary(key(Key, Encode)),
    case is_map_key(EncodedKey, Visited0) of
        true ->
            error({duplicate_key, Key});
        _ ->
            Visited = Visited0#{EncodedKey => true},
            [$,, EncodedKey, $:, Encode(Value, Encode) | do_encode_checked(Rest, Encode, Visited)]
    end;
do_encode_checked([], _, _) ->
    [].

%% Dispatching any value through `Encode` could allow incorrect
%% JSON to be emitted (with keys not being strings). To avoid this,
%% the default encoder only supports binaries, atoms, and numbers.
%% Customisation is possible by overriding how maps are encoded in general.
key(Key, Encode) when is_binary(Key) -> Encode(Key, Encode);
key(Key, Encode) when is_atom(Key) -> Encode(atom_to_binary(Key, utf8), Encode);
key(Key, _Encode) when is_integer(Key) -> [$", encode_integer(Key), $"];
key(Key, _Encode) when is_float(Key) -> [$", encode_float(Key), $"].

encode_object([]) -> <<"{}">>;
encode_object([[_Comma | Entry] | Rest]) -> ["{", Entry, Rest, "}"].

-spec encode_binary(binary()) -> iodata().
encode_binary(Bin) when is_binary(Bin) ->
    escape_binary(Bin).

-spec encode_binary_escape_all(binary()) -> iodata().
encode_binary_escape_all(Bin) when is_binary(Bin) ->
    escape_all(Bin).

escape_binary(Bin) -> escape_binary_ascii(Bin, [$\"], Bin, 0, 0).

escape_binary_ascii(Binary, Acc, Orig, Skip, Len) ->
    case Binary of
        <<B1, B2, B3, B4, B5, B6, B7, B8, Rest/binary>> when ?are_all_ascii_plain(B1, B2, B3, B4, B5, B6, B7, B8) ->
            escape_binary_ascii(Rest, Acc, Orig, Skip, Len + 8);
        Other ->
            escape_binary(Other, Acc, Orig, Skip, Len)
    end.

escape_binary(<<Byte, Rest/binary>>, Acc, Orig, Skip, Len) when ?is_ascii_plain(Byte) ->
    %% we got here because there were either less than 8 bytes left
    %% or we have an escape in the next 8 bytes,
    %% escape_binary_ascii would fail and dispatch here anyway
    escape_binary(Rest, Acc, Orig, Skip, Len + 1);
escape_binary(<<Byte, Rest/binary>>, Acc, Orig, Skip0, Len) when ?is_ascii_escape(Byte) ->
    Escape = escape(Byte),
    Skip = Skip0 + Len + 1,
    case Len of
        0 ->
            escape_binary_ascii(Rest, [Acc | Escape], Orig, Skip, 0);
        _ ->
            Part = binary_part(Orig, Skip0, Len),
            escape_binary_ascii(Rest, [Acc, Part | Escape], Orig, Skip, 0)
    end;
escape_binary(<<Byte, Rest/binary>>, Acc, Orig, Skip, Len) ->
    case element(Byte - 127, utf8s0()) of
        ?UTF8_REJECT -> invalid_byte(Orig, Skip + Len);
        %% all accept cases are ASCII, already covred above
        State -> escape_binary_utf8(Rest, Acc, Orig, Skip, Len, State)
    end;
escape_binary(_, _Acc, Orig, 0, _Len) ->
    [$", Orig, $"];
escape_binary(_, Acc, Orig, Skip, Len) ->
    Part = binary_part(Orig, Skip, Len),
    [Acc, Part, $\"].

escape_binary_utf8(<<Byte, Rest/binary>>, Acc, Orig, Skip, Len, State0) ->
    Type = element(Byte + 1, utf8t()),
    case element(State0 + Type, utf8s()) of
        ?UTF8_ACCEPT -> escape_binary_ascii(Rest, Acc, Orig, Skip, Len + 2);
        ?UTF8_REJECT -> invalid_byte(Orig, Skip + Len + 1);
        State -> escape_binary_utf8(Rest, Acc, Orig, Skip, Len + 1, State)
    end;
escape_binary_utf8(_, _Acc, Orig, Skip, Len, _State) ->
    unexpected(Orig, Skip + Len + 1).

multibyte_size(Char) when Char =< 16#7FF -> 2;
multibyte_size(Char) when Char =< 16#FFFF -> 3;
multibyte_size(_Char) -> 4.

escape_all(Bin) -> escape_all_ascii(Bin, [$"], Bin, 0, 0).

escape_all_ascii(Binary, Acc, Orig, Skip, Len) ->
    case Binary of
        <<B1, B2, B3, B4, B5, B6, B7, B8, Rest/binary>> when ?are_all_ascii_plain(B1, B2, B3, B4, B5, B6, B7, B8) ->
            escape_all_ascii(Rest, Acc, Orig, Skip, Len + 8);
        Other ->
            escape_all(Other, Acc, Orig, Skip, Len)
    end.

escape_all(<<Byte, Rest/binary>>, Acc, Orig, Skip, Len) when ?is_ascii_plain(Byte) ->
    escape_all(Rest, Acc, Orig, Skip, Len + 1);
escape_all(<<Byte, Rest/bits>>, Acc, Orig, Skip, Len) when ?is_ascii_escape(Byte) ->
    Escape = escape(Byte),
    case Len of
        0 ->
            escape_all(Rest, [Acc | Escape], Orig, Skip + 1, 0);
        _ ->
            Part = binary_part(Orig, Skip, Len),
            escape_all(Rest, [Acc, Part | Escape], Orig, Skip + Len + 1, 0)
    end;
escape_all(<<Char/utf8, Rest/bits>>, Acc, Orig, Skip, 0) ->
    escape_char(Rest, Acc, Orig, Skip, Char);
escape_all(<<Char/utf8, Rest/bits>>, Acc, Orig, Skip, Len) ->
    Part = binary_part(Orig, Skip, Len),
    escape_char(Rest, [Acc | Part], Orig, Skip + Len, Char);
escape_all(<<>>, _Acc, Orig, 0, _Len) ->
    [$", Orig, $"];
escape_all(<<>>, Acc, Orig, Skip, Len) ->
    Part = binary_part(Orig, Skip, Len),
    [Acc, Part, $"];
escape_all(_Other, _Acc, Orig, Skip, Len) ->
    invalid_byte(Orig, Skip + Len).

escape_char(<<Rest/bits>>, Acc, Orig, Skip, Char) when Char =< 16#FF ->
    Acc1 = [Acc, "\\u00" | integer_to_binary(Char, 16)],
    escape_all(Rest, Acc1, Orig, Skip + 2, 0);
escape_char(<<Rest/bits>>, Acc, Orig, Skip, Char) when Char =< 16#7FF ->
    Acc1 = [Acc, "\\u0" | integer_to_binary(Char, 16)],
    escape_all(Rest, Acc1, Orig, Skip + 2, 0);
escape_char(<<Rest/bits>>, Acc, Orig, Skip, Char) when Char =< 16#FFF ->
    Acc1 = [Acc, "\\u0" | integer_to_binary(Char, 16)],
    escape_all(Rest, Acc1, Orig, Skip + 3, 0);
escape_char(<<Rest/bits>>, Acc, Orig, Skip, Char) when Char =< 16#FFFF ->
    Acc1 = [Acc, "\\u" | integer_to_binary(Char, 16)],
    escape_all(Rest, Acc1, Orig, Skip + 3, 0);
escape_char(<<Rest/bits>>, Acc, Orig, Skip, Char0) ->
    Char = Char0 - 16#10000,
    First = integer_to_binary(16#800 bor (Char bsr 10), 16),
    Second = integer_to_binary(16#C00 bor (Char band 16#3FF), 16),
    Acc1 = [Acc, "\\uD", First, "\\uD" | Second],
    escape_all(Rest, Acc1, Orig, Skip + 4, 0).

-spec escape(byte()) -> binary() | no.
escape($\x00) -> <<"\\u0000">>;
escape($\x01) -> <<"\\u0001">>;
escape($\x02) -> <<"\\u0002">>;
escape($\x03) -> <<"\\u0003">>;
escape($\x04) -> <<"\\u0004">>;
escape($\x05) -> <<"\\u0005">>;
escape($\x06) -> <<"\\u0006">>;
escape($\x07) -> <<"\\u0007">>;
escape($\b) -> <<"\\b">>;
escape($\t) -> <<"\\t">>;
escape($\n) -> <<"\\n">>;
escape($\x0b) -> <<"\\u000B">>;
escape($\f) -> <<"\\f">>;
escape($\r) -> <<"\\r">>;
escape($\x0e) -> <<"\\u000E">>;
escape($\x0f) -> <<"\\u000F">>;
escape($\x10) -> <<"\\u0010">>;
escape($\x11) -> <<"\\u0011">>;
escape($\x12) -> <<"\\u0012">>;
escape($\x13) -> <<"\\u0013">>;
escape($\x14) -> <<"\\u0014">>;
escape($\x15) -> <<"\\u0015">>;
escape($\x16) -> <<"\\u0016">>;
escape($\x17) -> <<"\\u0017">>;
escape($\x18) -> <<"\\u0018">>;
escape($\x19) -> <<"\\u0019">>;
escape($\x1A) -> <<"\\u001A">>;
escape($\x1B) -> <<"\\u001B">>;
escape($\x1C) -> <<"\\u001C">>;
escape($\x1D) -> <<"\\u001D">>;
escape($\x1E) -> <<"\\u001E">>;
escape($\x1F) -> <<"\\u001F">>;
escape($") -> <<"\\\"">>;
escape($\\) -> <<"\\\\">>;
escape(_) -> no.

%% This is an adapted table from "Flexible and Economical UTF-8 Decoding" by Bjoern Hoehrmann.
%% http://bjoern.hoehrmann.de/utf-8/decoder/dfa/

%% erlfmt-ignore
%% Map character to character class
utf8t() ->
    {
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,  9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,
        7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,  7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
        8,8,2,2,2,2,2,2,2,2,2,2,2,2,2,2,  2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,
       10,3,3,3,3,3,3,3,3,3,3,3,3,4,3,3, 11,6,6,6,5,8,8,8,8,8,8,8,8,8,8,8
    }.

%% erlfmt-ignore
%% Transition table mapping combination of state & class to next state
utf8s() ->
    {
           12,24,36,60,96,84,12,12,12,48,72, 12,12,12,12,12,12,12,12,12,12,12,12,
        12, 0,12,12,12,12,12, 0,12, 0,12,12, 12,24,12,12,12,12,12,24,12,24,12,12,
        12,12,12,12,12,12,12,24,12,12,12,12, 12,24,12,12,12,12,12,12,12,24,12,12,
        12,12,12,12,12,12,12,36,12,36,12,12, 12,36,12,12,12,12,12,36,12,36,12,12,
        12,36,12,12,12,12,12,12,12,12,12,12
    }.

%% erlfmt-ignore
%% Optimisation for 1st byte direct state lookup,
%% we know starting state is 0 and ASCII bytes were already handled
utf8s0() ->
    {
        12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,
        12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,
        12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,
        12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,
        12,12,24,24,24,24,24,24,24,24,24,24,24,24,24,24,
        24,24,24,24,24,24,24,24,24,24,24,24,24,24,24,24,
        48,36,36,36,36,36,36,36,36,36,36,36,36,60,36,36,
        72,84,84,84,96,12,12,12,12,12,12,12,12,12,12,12
    }.

-if(?OTP_RELEASE >= 24).
invalid_byte(Bin, Skip) ->
    Byte = binary:at(Bin, Skip),
    error({invalid_byte, Byte}, none, error_info(Skip)).

error_info(Skip) ->
    [{error_info, #{cause => #{position => Skip}}}].
-else.
invalid_byte(Bin, Skip) ->
    Byte = binary:at(Bin, Skip),
    error({invalid_byte, Byte}, none).
-endif.

%%
%% Decoding implementation
%%

% We use integers instead of atoms for better codegen
-define(ARRAY, 0).
-define(OBJECT, 1).

-define(is_1_to_9(X), X =:= $1; X =:= $2; X =:= $3; X =:= $4; X =:= $5; X =:= $6; X =:= $7; X =:= $8; X =:= $9).
-define(is_0_to_9(X), X =:= $0; ?is_1_to_9(X)).
-define(is_ws(X), X =:= $\s; X =:= $\t; X =:= $\r; X =:= $\n).

-type from_binary_fun() :: fun((binary()) -> dynamic()).
-type array_start_fun() :: fun((Acc :: dynamic()) -> ArrayAcc :: dynamic()).
-type array_push_fun() :: fun((Value :: dynamic(), Acc :: dynamic()) -> NewAcc :: dynamic()).
-type array_finish_fun() :: fun((ArrayAcc :: dynamic()) -> dynamic()).
-type object_start_fun() :: fun((Acc :: dynamic()) -> ObjectAcc :: dynamic()).
-type object_push_fun() :: fun((Key :: dynamic(), Value :: dynamic(), Acc :: dynamic()) -> NewAcc :: dynamic()).
-type object_finish_fun() :: fun((ObjectAcc :: dynamic()) -> dynamic()).

-type decoders() :: #{
    empty_array => term(),
    array_start => array_start_fun(),
    array_push => array_push_fun(),
    array_finish => array_finish_fun(),
    empty_object => term(),
    object_start => object_start_fun(),
    object_push => object_push_fun(),
    object_finish => object_finish_fun(),
    float => from_binary_fun(),
    integer => from_binary_fun(),
    string => from_binary_fun(),
    null => term()
}.

-record(decode, {
    empty_array = [] :: term(),
    array_start :: array_start_fun() | undefined,
    array_push :: array_push_fun() | undefined,
    array_finish = fun lists:reverse/1 :: array_finish_fun(),
    empty_object = #{} :: term(),
    object_start :: object_start_fun() | undefined,
    object_push :: object_push_fun() | undefined,
    object_finish = fun maps:from_list/1 :: object_finish_fun(),
    float = fun erlang:binary_to_float/1 :: from_binary_fun(),
    integer = fun erlang:binary_to_integer/1 :: from_binary_fun(),
    string :: from_binary_fun() | undefined,
    null = null :: term()
}).

-type acc() :: dynamic().
-type stack() :: [?ARRAY | ?OBJECT | binary() | acc()].
-type decode() :: #decode{}.

-type simple_decode_value() ::
    integer()
    | float()
    | boolean()
    | null
    | binary()
    | list(simple_decode_value())
    | #{binary() => simple_decode_value()}.

% @doc 
% Parses a JSON value from `Binary`.

% Supports basic data mapping:

% | **JSON** | **Erlang**            |
% |----------|-----------------------|
% | Number   | integer() \\| float() |
% | Boolean  | true \\| false        |
% | Null     | null                  |
% | String   | binary()              |
% | Object   | #{binary() => _}      |

-spec decode(binary()) -> simple_decode_value().
decode(Binary) when is_binary(Binary) ->
    case value(Binary, Binary, 0, ok, [], #decode{}) of
        {Result, _Acc, <<>>} -> Result;
        {_, _, Rest} -> unexpected(Rest, 0)
    end.

% @doc
% Parses a JSON value from `Binary`.

% Similar to `decode/1` except the decoding process
% can be customized with the callbacks specified in
% `Decoders`. The callbacks will use the `Acc` value
% as the initial accumulator.

% Any leftover, unparsed data in `Binary` will be returned.

% ## Example

% Decoding object keys as atoms:

% ```erlang
% Push = fun(Key, Value, Acc) -> [{binary_to_existing_atom(Key), Value} | Acc] end,
% json:decode(Data, ok, #{object_push => Push})
% ```

-spec decode(binary(), dynamic(), decoders()) ->
    {Result :: dynamic(), Acc :: dynamic(), binary()}.
decode(Binary, Acc, Decoders) when is_binary(Binary) ->
    Decode = maps:fold(fun parse_decoder/3, #decode{}, Decoders),
    value(Binary, Binary, 0, Acc, [], Decode).

parse_decoder(empty_array, Value, Decode) ->
    Decode#decode{empty_array = Value};
parse_decoder(array_start, Fun, Decode) when is_function(Fun, 1) ->
    Decode#decode{array_start = Fun};
parse_decoder(array_push, Fun, Decode) when is_function(Fun, 2) ->
    Decode#decode{array_push = Fun};
parse_decoder(array_finish, Fun, Decode) when is_function(Fun, 1) ->
    Decode#decode{array_finish = Fun};
parse_decoder(empty_object, Value, Decode) ->
    Decode#decode{empty_object = Value};
parse_decoder(object_start, Fun, Decode) when is_function(Fun, 1) ->
    Decode#decode{object_start = Fun};
parse_decoder(object_push, Fun, Decode) when is_function(Fun, 3) ->
    Decode#decode{object_push = Fun};
parse_decoder(object_finish, Fun, Decode) when is_function(Fun, 1) ->
    Decode#decode{object_finish = Fun};
parse_decoder(float, Fun, Decode) when is_function(Fun, 1) ->
    Decode#decode{float = Fun};
parse_decoder(integer, Fun, Decode) when is_function(Fun, 1) ->
    Decode#decode{integer = Fun};
parse_decoder(string, Fun, Decode) when is_function(Fun, 1) ->
    Decode#decode{string = Fun};
parse_decoder(null, Null, Decode) ->
    Decode#decode{null = Null}.

value(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode) ->
    case Byte of
        _ when ?is_ws(Byte) -> value(Rest, Original, Skip + 1, Acc, Stack, Decode);
        $0 -> number_zero(Rest, Original, Skip, Acc, Stack, Decode, 1);
        _ when ?is_1_to_9(Byte) -> number(Rest, Original, Skip, Acc, Stack, Decode, 1);
        $- -> number_minus(Rest, Original, Skip, Acc, Stack, Decode);
        $t -> true(Rest, Original, Skip, Acc, Stack, Decode);
        $f -> false(Rest, Original, Skip, Acc, Stack, Decode);
        $n -> null(Rest, Original, Skip, Acc, Stack, Decode);
        $" -> string(Rest, Original, Skip + 1, Acc, Stack, Decode, 0);
        $[ -> array_start(Rest, Original, Skip, Acc, Stack, Decode);
        ${ -> object_start(Rest, Original, Skip, Acc, Stack, Decode);
        _ -> unexpected(Original, Skip)
    end;
value(<<>>, Original, Skip, _Acc, _Stack, _Decode) ->
    unexpected(Original, Skip).

true(<<"rue", Rest/bits>>, Original, Skip, Acc, Stack, Decode) ->
    continue(Rest, Original, Skip + 4, Acc, Stack, Decode, true);
true(_Rest, Original, Skip, _Acc, _Stack, _Decode) ->
    unexpected(Original, Skip + 1).

false(<<"alse", Rest/bits>>, Original, Skip, Acc, Stack, Decode) ->
    continue(Rest, Original, Skip + 5, Acc, Stack, Decode, false);
false(_Rest, Original, Skip, _Acc, _Stack, _Decode) ->
    unexpected(Original, Skip + 1).

null(<<"ull", Rest/bits>>, Original, Skip, Acc, Stack, Decode) ->
    continue(Rest, Original, Skip + 4, Acc, Stack, Decode, Decode#decode.null);
null(_Rest, Original, Skip, _Acc, _Stack, _Decode) ->
    unexpected(Original, Skip + 1).

number_minus(<<$0, Rest/bits>>, Original, Skip, Acc, Stack, Decode) ->
    number_zero(Rest, Original, Skip, Acc, Stack, Decode, 2);
number_minus(<<Num, Rest/bits>>, Original, Skip, Acc, Stack, Decode) when ?is_1_to_9(Num) ->
    number(Rest, Original, Skip, Acc, Stack, Decode, 2);
number_minus(_Rest, Original, Skip, _Acc, _Stack, _Decode) ->
    unexpected(Original, Skip + 1).

number_zero(<<$., Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) ->
    number_frac(Rest, Original, Skip, Acc, Stack, Decode, Len + 1);
number_zero(<<E, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) when E =:= $E; E =:= $e ->
    number_exp_copy(Rest, Original, Skip, Acc, Stack, Decode, Len + 1, <<"0">>);
number_zero(Rest, Original, Skip, Acc, Stack, Decode, Len) ->
    continue(Rest, Original, Skip + Len, Acc, Stack, Decode, 0).

number(<<Num, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) when ?is_0_to_9(Num) ->
    number(Rest, Original, Skip, Acc, Stack, Decode, Len + 1);
number(<<$., Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) ->
    number_frac(Rest, Original, Skip, Acc, Stack, Decode, Len + 1);
number(<<E, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) when E =:= $E; E =:= $e ->
    Prefix = binary_part(Original, Skip, Len),
    number_exp_copy(Rest, Original, Skip, Acc, Stack, Decode, Len + 1, Prefix);
number(Rest, Original, Skip, Acc, Stack, Decode, Len) ->
    Int = (Decode#decode.integer)(binary_part(Original, Skip, Len)),
    continue(Rest, Original, Skip + Len, Acc, Stack, Decode, Int).

number_frac(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) when ?is_0_to_9(Byte) ->
    number_frac_cont(Rest, Original, Skip, Acc, Stack, Decode, Len + 1);
number_frac(_, Original, Skip, _Acc, _Stack, _Decode, Len) ->
    unexpected(Original, Skip + Len).

number_frac_cont(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) when ?is_0_to_9(Byte) ->
    number_frac_cont(Rest, Original, Skip, Acc, Stack, Decode, Len + 1);
number_frac_cont(<<E, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) when E =:= $e; E =:= $E ->
    number_exp(Rest, Original, Skip, Acc, Stack, Decode, Len + 1);
number_frac_cont(Rest, Original, Skip, Acc, Stack, Decode, Len) ->
    Token = binary_part(Original, Skip, Len),
    float_decode(Rest, Original, Skip, Acc, Stack, Decode, Len, Token).

float_decode(<<Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len, Token) ->
    try (Decode#decode.float)(Token) of
        Float ->
            continue(Rest, Original, Skip + Len, Acc, Stack, Decode, Float)
    catch
        _:_ -> unexpected_sequence(Token, Skip)
    end.

number_exp(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) when ?is_0_to_9(Byte) ->
    number_exp_cont(Rest, Original, Skip, Acc, Stack, Decode, Len + 1);
number_exp(<<Sign, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) when Sign =:= $+; Sign =:= $- ->
    number_exp_sign(Rest, Original, Skip, Acc, Stack, Decode, Len + 1);
number_exp(_, Original, Skip, _Acc, _Stack, _Decode, Len) ->
    unexpected(Original, Skip + Len).

number_exp_sign(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) when ?is_0_to_9(Byte) ->
    number_exp_cont(Rest, Original, Skip, Acc, Stack, Decode, Len + 1);
number_exp_sign(_, Original, Skip, _Acc, _Stack, _Decode, Len) ->
    unexpected(Original, Skip + Len).

number_exp_cont(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) when ?is_0_to_9(Byte) ->
    number_exp_cont(Rest, Original, Skip, Acc, Stack, Decode, Len + 1);
number_exp_cont(Rest, Original, Skip, Acc, Stack, Decode, Len) ->
    Token = binary_part(Original, Skip, Len),
    float_decode(Rest, Original, Skip, Acc, Stack, Decode, Len, Token).

number_exp_copy(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len, Prefix) when ?is_0_to_9(Byte) ->
    number_exp_cont(Rest, Original, Skip, Acc, Stack, Decode, Len, Prefix, 1);
number_exp_copy(<<Sign, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len, Prefix) when Sign =:= $+; Sign =:= $- ->
    number_exp_sign(Rest, Original, Skip, Acc, Stack, Decode, Len, Prefix, 1);
number_exp_copy(_, Original, Skip, _Acc, _Stack, _Decode, Len, _Prefix) ->
    unexpected(Original, Skip + Len).

number_exp_sign(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len, Prefix, ExpLen) when ?is_0_to_9(Byte) ->
    number_exp_cont(Rest, Original, Skip, Acc, Stack, Decode, Len, Prefix, ExpLen + 1);
number_exp_sign(_, Original, Skip, _Acc, _Stack, _Decode, Len, _Prefix, ExpLen) ->
    unexpected(Original, Skip + Len + ExpLen).

number_exp_cont(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len, Prefix, ExpLen) when ?is_0_to_9(Byte) ->
    number_exp_cont(Rest, Original, Skip, Acc, Stack, Decode, Len, Prefix, ExpLen + 1);
number_exp_cont(Rest, Original, Skip, Acc, Stack, Decode, Len, Prefix, ExpLen) ->
    Suffix = binary_part(Original, Skip + Len, ExpLen),
    Token = <<Prefix/binary, ".0e", Suffix/binary>>,
    float_decode(Rest, Original, Skip, Acc, Stack, Decode, Len + ExpLen, Token).

-spec string(binary(), binary(), integer(), acc(), stack(), decode(), integer()) -> dynamic().
string(<<$", Rest/bits>>, Original, Skip0, Acc, Stack, Decode, Len) ->
    Value = binary_part(Original, Skip0, Len),
    Skip = Skip0 + Len + 1,
    case Decode#decode.string of
        undefined -> continue(Rest, Original, Skip, Acc, Stack, Decode, Value);
        Fun -> continue(Rest, Original, Skip, Acc, Stack, Decode, Fun(Value))
    end;
string(<<$\\, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) ->
    Part = binary_part(Original, Skip, Len),
    SAcc = <<>>,
    escape(Rest, Original, Skip, Acc, Stack, Decode, Len, <<SAcc/binary, Part/binary>>);
string(<<Byte, _/bits>>, Original, Skip, _Acc, _Stack, _Decode, Len) when Byte =< 16#1F ->
    unexpected(Original, Skip + Len);
string(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) when Byte =< 16#7F ->
    string(Rest, Original, Skip, Acc, Stack, Decode, Len + 1);
string(<<Char/utf8, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len) ->
    string(Rest, Original, Skip, Acc, Stack, Decode, Len + multibyte_size(Char));
string(_, Original, Skip, _Acc, _Stack, _Decode, Len) ->
    unexpected(Original, Skip + Len).

-spec string(binary(), binary(), integer(), acc(), stack(), decode(), integer(), binary(), integer()) -> dynamic().
string(<<$", Rest/bits>>, Original, Skip0, Acc, Stack, Decode, Len, SAcc, PLen) ->
    Part = binary_part(Original, Skip0 + Len, PLen),
    Value = <<SAcc/binary, Part/binary>>,
    Skip = Skip0 + Len + PLen + 1,
    case Decode#decode.string of
        undefined -> continue(Rest, Original, Skip, Acc, Stack, Decode, Value);
        Fun -> continue(Rest, Original, Skip, Acc, Stack, Decode, Fun(Value))
    end;
string(<<$\\, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len, SAcc, PLen) ->
    Part = binary_part(Original, Skip + Len, PLen),
    escape(Rest, Original, Skip, Acc, Stack, Decode, Len + PLen, <<SAcc/binary, Part/binary>>);
string(<<Byte, _/bits>>, Original, Skip, _Acc, _Stack, _Decode, Len, _SAcc, PLen) when Byte =< 16#1F ->
    unexpected(Original, Skip + Len + PLen);
string(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len, SAcc, PLen) when Byte =< 16#7F ->
    string(Rest, Original, Skip, Acc, Stack, Decode, Len, SAcc, PLen + 1);
string(<<Char/utf8, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len, SAcc, PLen) ->
    string(Rest, Original, Skip, Acc, Stack, Decode, Len, SAcc, PLen + multibyte_size(Char));
string(_, Original, Skip, _Acc, _Stack, _Decode, Len, _SAcc, PLen) ->
    unexpected(Original, Skip + Len + PLen).

escape(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len, SAcc) ->
    Val =
        case Byte of
            $b -> $\b;
            $f -> $\f;
            $n -> $\n;
            $r -> $\r;
            $t -> $\t;
            $" -> $";
            $\\ -> $\\;
            $/ -> $/;
            $u -> unicode;
            _ -> unexpected(Original, Skip + Len + 1)
        end,
    case Val of
        unicode -> escapeu(Rest, Original, Skip, Acc, Stack, Decode, Len, SAcc);
        Int -> string(Rest, Original, Skip, Acc, Stack, Decode, Len + 2, <<SAcc/binary, Int>>, 0)
    end;
escape(_, Original, Skip, _Acc, _Stack, _Decode, Len, _SAcc) ->
    unexpected(Original, Skip + Len + 1).

escapeu(<<Escape:4/binary, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len, SAcc) ->
    try binary_to_integer(Escape, 16) of
        CP when CP >= 16#D800, CP =< 16#DBFF ->
            escape_surrogate(Rest, Original, Skip, Acc, Stack, Decode, Len, SAcc, CP);
        CP ->
            try <<SAcc/binary, CP/utf8>> of
                SAcc1 -> string(Rest, Original, Skip, Acc, Stack, Decode, Len + 6, SAcc1, 0)
            catch
                _:_ -> unexpected_sequence(binary_part(Original, Skip + Len, 6), Skip + Len)
            end
    catch
        _:_ ->
            unexpected_sequence(binary_part(Original, Skip + Len, 6), Skip + Len)
    end;
escapeu(_, Original, Skip, _Acc, _Stack, _Decode, Len, _SAcc) ->
    unexpected(Original, Skip + Len + 2).

escape_surrogate(<<"\\u", Escape:4/binary, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Len, SAcc, Hi) ->
    try binary_to_integer(Escape, 16) of
        Lo when Lo >= 16#DC00, Lo =< 16#DFFF ->
            CP = 16#10000 + ((Hi band 16#3FF) bsl 10) + (Lo band 16#3FF),
            try <<SAcc/binary, CP/utf8>> of
                SAcc1 -> string(Rest, Original, Skip, Acc, Stack, Decode, Len + 12, SAcc1, 0)
            catch
                _:_ -> unexpected_sequence(binary_part(Original, Skip + Len, 12), Skip + Len)
            end;
        _ ->
            unexpected_sequence(binary_part(Original, Skip + Len, 12), Skip + Len)
    catch
        _:_ -> unexpected_sequence(binary_part(Original, Skip + Len, 12), Skip + Len)
    end;
escape_surrogate(_, Original, Skip, _Acc, _Stack, _Decode, Len, _SAcc, _Hi) ->
    unexpected(Original, Skip + Len + 6).

array_start(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode) when ?is_ws(Byte) ->
    array_start(Rest, Original, Skip + 1, Acc, Stack, Decode);
array_start(<<"]", Rest/bits>>, Original, Skip, Acc, Stack, Decode) ->
    continue(Rest, Original, Skip + 2, Acc, Stack, Decode, Decode#decode.empty_array);
array_start(Rest, Original, Skip0, OldAcc, Stack, Decode) ->
    Skip = Skip0 + 1,
    case Decode#decode.array_start of
        undefined -> value(Rest, Original, Skip, [], [?ARRAY, OldAcc | Stack], Decode);
        Fun -> value(Rest, Original, Skip, Fun(OldAcc), [?ARRAY, OldAcc | Stack], Decode)
    end.

array_push(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Value) when ?is_ws(Byte) ->
    array_push(Rest, Original, Skip + 1, Acc, Stack, Decode, Value);
array_push(<<"]", Rest/bits>>, Original, Skip, Acc0, Stack0, Decode, Value) ->
    Acc =
        case Decode#decode.array_push of
            undefined -> [Value | Acc0];
            Fun -> Fun(Value, Acc0)
        end,
    ArrayValue = (Decode#decode.array_finish)(Acc),
    [_, OldAcc | Stack] = Stack0,
    continue(Rest, Original, Skip + 1, OldAcc, Stack, Decode, ArrayValue);
array_push(<<$,, Rest/bits>>, Original, Skip0, Acc, Stack, Decode, Value) ->
    Skip = Skip0 + 1,
    case Decode#decode.array_push of
        undefined -> value(Rest, Original, Skip, [Value | Acc], Stack, Decode);
        Fun -> value(Rest, Original, Skip, Fun(Value, Acc), Stack, Decode)
    end;
array_push(_, Original, Skip, _Acc, _Stack, _Decode, _Value) ->
    unexpected(Original, Skip).

object_start(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode) when ?is_ws(Byte) ->
    object_start(Rest, Original, Skip + 1, Acc, Stack, Decode);
object_start(<<"}", Rest/bits>>, Original, Skip, Acc, Stack, Decode) ->
    continue(Rest, Original, Skip + 2, Acc, Stack, Decode, Decode#decode.empty_object);
object_start(<<$", Rest/bits>>, Original, Skip0, OldAcc, Stack0, Decode) ->
    Stack = [?OBJECT, OldAcc | Stack0],
    Skip = Skip0 + 2,
    case Decode#decode.object_start of
        undefined ->
            string(Rest, Original, Skip, [], Stack, Decode, 0);
        Fun ->
            Acc = Fun(OldAcc),
            string(Rest, Original, Skip, Acc, Stack, Decode, 0)
    end;
object_start(_, Original, Skip, _Acc, _Stack, _Decode) ->
    unexpected(Original, Skip + 1).

object_value(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Key) when ?is_ws(Byte) ->
    object_value(Rest, Original, Skip + 1, Acc, Stack, Decode, Key);
object_value(<<$:, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Key) ->
    value(Rest, Original, Skip + 1, Acc, [Key | Stack], Decode);
object_value(_, Original, Skip, _Acc, _Stack, _Decode, _Key) ->
    unexpected(Original, Skip).

object_push(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode, Value, Key) when ?is_ws(Byte) ->
    object_push(Rest, Original, Skip + 1, Acc, Stack, Decode, Value, Key);
object_push(<<"}", Rest/bits>>, Original, Skip, Acc0, Stack0, Decode, Value, Key) ->
    Acc =
        case Decode#decode.object_push of
            undefined -> [{Key, Value} | Acc0];
            Fun -> Fun(Key, Value, Acc0)
        end,
    ObjectValue = (Decode#decode.object_finish)(Acc),
    [_, OldAcc | Stack] = Stack0,
    continue(Rest, Original, Skip + 1, OldAcc, Stack, Decode, ObjectValue);
object_push(<<$,, Rest/bits>>, Original, Skip, Acc0, Stack, Decode, Value, Key) ->
    case Decode#decode.object_push of
        undefined -> object_key(Rest, Original, Skip + 1, [{Key, Value} | Acc0], Stack, Decode);
        Fun -> object_key(Rest, Original, Skip + 1, Fun(Key, Value, Acc0), Stack, Decode)
    end;
object_push(_, Original, Skip, _Acc, _Stack, _Decode, _Value, _Key) ->
    unexpected(Original, Skip).

object_key(<<Byte, Rest/bits>>, Original, Skip, Acc, Stack, Decode) when ?is_ws(Byte) ->
    object_key(Rest, Original, Skip + 1, Acc, Stack, Decode);
object_key(<<$", Rest/bits>>, Original, Skip, Acc, Stack, Decode) ->
    string(Rest, Original, Skip + 1, Acc, Stack, Decode, 0);
object_key(_, Original, Skip, _Acc, _Stack, _Decode) ->
    unexpected(Original, Skip).

continue(<<Rest/bits>>, Original, Skip, Acc, Stack0, Decode, Value) ->
    case Stack0 of
        [] -> terminate(Rest, Original, Skip, Acc, Value);
        [?ARRAY | _] -> array_push(Rest, Original, Skip, Acc, Stack0, Decode, Value);
        [?OBJECT | _] -> object_value(Rest, Original, Skip, Acc, Stack0, Decode, Value);
        [Key | Stack] -> object_push(Rest, Original, Skip, Acc, Stack, Decode, Value, Key)
    end.

terminate(<<Byte, Rest/bits>>, Original, Skip, Acc, Value) when ?is_ws(Byte) ->
    terminate(Rest, Original, Skip + 1, Acc, Value);
terminate(<<Rest/bits>>, _Original, _Skip, Acc, Value) ->
    {Value, Acc, Rest}.

-spec unexpected(binary(), non_neg_integer()) -> no_return().
unexpected(Original, Skip) when byte_size(Original) =:= Skip ->
    error(unexpected_end);
unexpected(Original, Skip) ->
    invalid_byte(Original, Skip).

-spec unexpected_sequence(binary(), non_neg_integer()) -> no_return().
-if(?OTP_RELEASE >= 24).
unexpected_sequence(Value, Skip) ->
    error({unexpected_sequence, Value}, none, error_info(Skip)).
-else.
unexpected_sequence(Value, _Skip) ->
    error({unexpected_sequence, Value}, none).
-endif.

