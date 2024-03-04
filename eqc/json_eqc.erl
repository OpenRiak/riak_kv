-module(json_eqc).

-include_lib("eqc/include/eqc.hrl").
-compile([export_all, nowarn_export_all]).

prop_roundtrip() ->
    in_parallel(?FORALL(Obj, object(), equals(Obj, decode(encode(Obj))))).

prop_escape_all() ->
    in_parallel(
    ?FORALL(Str, object(),
            begin
               Encoded = iolist_to_binary(encode_escape_all(Str)),
               all_ascii(Encoded) andalso equals(Str, decode(Encoded))
            end)).

printable_string() ->
    Chars = oneof([
        oneof("\n\r\t\v\b\f\e\d"),
        choose(16#20, 16#7E),
        choose(16#A0, 16#D7FF),
        choose(16#E000, 16#FFFD),
        choose(16#10000, 16#10FFFF)
    ]),
    ?LET(L, list(Chars), unicode:characters_to_binary(L)).

object() -> ?SIZED(Size, object(Size)).

object(0) ->
    oneof([integer(), real(), printable_string(), bool(), null]);
object(Size) ->
    ?LAZY(oneof([object(0),
                 ?LETSHRINK(Objects, list(object(Size div 4)), Objects),
                 map(printable_string(), object(Size div 4))
                ])).

integer() -> choose(-16#8fffffff, 16#ffffffff).

%% Helpers

all_ascii(Bin) ->
    lists:all(fun(C) -> C < 128 end, binary_to_list(Bin)).

decode(IOList) ->
    riak_kv_wm_json:decode(iolist_to_binary(IOList)).

%% May produce an iolist
encode(Str) ->
    riak_kv_wm_json:encode(Str).

encode_escape_all(Term) ->
    Encode = fun
        (Binary, _) when is_binary(Binary) -> riak_kv_wm_json:encode_binary_escape_all(Binary);
        (Other, Encode) -> riak_kv_wm_json:encode_value(Other, Encode)
    end,
    riak_kv_wm_json:encode(Term, Encode).
