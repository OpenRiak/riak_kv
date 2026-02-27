%% -------------------------------------------------------------------
%%
%% riak_kv_query_buffer: Riak module for accumulating query results
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

-module(riak_kv_query_buffer).

-export([new/3, add/2, flush/1, aggregate/2, min_buffer/0]).

-define(VERSION, [{version, 2}]). % to be used in sets
-define(MIN_BUFFER, 16).

-record(buffer, 
    {
        max_size :: pos_integer(),
        count = 0 :: non_neg_integer(),
        reply_fun :: reply_fun(),
        rawkey_acc :: rawkey_accumulator()|none,
        key_acc :: key_accumulator()|none,
        rawterm_acc :: rawterm_accumulator()|none,
        term_acc :: term_accumulator()|none,
        agg :: buffer_agg()|none,
        type :: riak_kv_query:accumulation_option(),
        start_time = os:system_time(microsecond) :: pos_integer()
    }
).

-record(key_agg,
    {
        set = sets:new(?VERSION) :: keycount_aggregator()
    }
).

-record(termcount_agg,
    {
        map = maps:new() :: countby_aggregator()   
    }
).

-record(termkeycount_agg,
    {
        map = maps:new() :: keycountby_aggregator()
    }
).

-type rawkey_accumulator()
    :: list(binary()).
-type rawterm_accumulator()
    :: list({binary(), binary()}).
-type key_accumulator()
    :: list({binary()}).
-type term_accumulator()
    :: list({{binary(), binary()}}).
-type keycount_aggregator()
    :: sets:set(binary()).
-type countby_aggregator()
    :: #{binary() => non_neg_integer()}|#{}.
-type keycountby_aggregator()
    :: #{binary() => sets:set(binary())}|#{}.

-type reply_type()
    :: 
        {keys, key_accumulator()} |
        {raw_keys, rawkey_accumulator() } |
        {count, non_neg_integer()} |
        {raw_count, non_neg_integer()} |
        {queue_raw_keys, rawkey_accumulator()} |
        {terms, term_accumulator()} |
        {raw_terms, rawterm_accumulator()} |
        {term_with_rawcount, countby_aggregator()} |
        {term_with_count, countby_aggregator()} |
        {queue_raw_terms, rawterm_accumulator()}.
-type reply_fun()
    :: fun((reply_type()|ping) -> ok).

-type buffer_agg()
    ::
        #key_agg{} |
        #termcount_agg{} |
        #termkeycount_agg{}.

-type buffer() :: #buffer{}.

-export_type([reply_type/0, reply_fun/0, buffer/0]).

-spec new(
    {pos_integer(), non_neg_integer()},
    riak_kv_query:accumulation_option(),
    reply_fun())
        -> buffer().
new(Size, T, ReplyFun) when T == keys ->
    new_buffer(Size, ReplyFun, keys, none, T);
new(Size, T, ReplyFun) when T == raw_keys ->
    new_buffer(Size, ReplyFun, raw_keys, none, T);
new(Size, T, ReplyFun) when T == count->
    new_buffer(Size, ReplyFun, raw_keys, #key_agg{}, T);
new(Size, T, ReplyFun) when T == raw_count ->
    new_buffer(Size, ReplyFun, none, none, T);
new(Size, T, ReplyFun) when T == queue_raw_keys ->
    new_buffer(Size, ReplyFun, raw_keys, none, raw_keys);
new(Size, T, ReplyFun) when T == terms->
    new_buffer(Size, ReplyFun, terms, none, T);
new(Size, T, ReplyFun) when T == raw_terms->
    new_buffer(Size, ReplyFun, raw_terms, none, T);
new(Size, T, ReplyFun) when T == term_with_rawcount->
    new_buffer(Size, ReplyFun, terms, #termcount_agg{}, T);
new(Size, T, ReplyFun) when T == term_with_count ->
    new_buffer(Size, ReplyFun, terms, #termkeycount_agg{}, T);
new(Size, T, ReplyFun) when T == queue_raw_terms->
    new_buffer(Size, ReplyFun, raw_terms, none, raw_terms).

new_buffer({BufferSize, JitterSize}, ReplyFun, AccType, InitAgg, Type)
        when BufferSize >= ?MIN_BUFFER, JitterSize =< BufferSize ->
    {RawKeyAcc, KeyAcc, RawTermAcc, TermAcc} =
        case AccType of
            none -> {none, none, none, none};
            raw_keys -> {[], none, none, none};
            keys -> {none, [], none, none};
            terms -> {none, none, none, []};
            raw_terms -> {none, none, [], none}
        end,
    ActualBufferSize =
        case JitterSize of
            0 ->
                BufferSize;
            JS when JS > 0 ->
                (BufferSize - (JS div 2)) + rand:uniform(JS)
        end,
    #buffer{
        max_size = ActualBufferSize,
        reply_fun = ReplyFun,
        rawkey_acc = RawKeyAcc,
        key_acc = KeyAcc,
        rawterm_acc = RawTermAcc,
        term_acc = TermAcc,
        agg = InitAgg,
        type = Type
    }.

min_buffer() -> ?MIN_BUFFER.

-spec add(binary()|{binary(), binary()}, buffer()) -> buffer().
add(Key, #buffer{max_size = MS, count = MS} = Buffer) ->
    add(Key, merge(Buffer));
add(_Key,
    #buffer{
        rawkey_acc = none, key_acc = none, term_acc = none, rawterm_acc = none,
        count = C
    } = B) ->
    B#buffer{count = C + 1};
add(Key, #buffer{rawkey_acc = A, count = C} = Buffer)
        when A =/= none, is_binary(Key) ->
    Buffer#buffer{rawkey_acc = [Key|A], count = C + 1};
add(Key, #buffer{key_acc = A, count = C} = Buffer)
        when A =/= none, is_binary(Key) ->
    Buffer#buffer{key_acc = [{Key}|A], count = C + 1};
add({Term, Key}, #buffer{rawterm_acc = A, count = C} = Buffer)
        when A =/= none, is_binary(Term), is_binary(Key) ->
    Buffer#buffer{rawterm_acc = [{Term, Key}|A], count = C + 1};
add({Term, Key}, #buffer{term_acc = A, count = C} = Buffer)
        when A =/= none, is_binary(Term), is_binary(Key) ->
    Buffer#buffer{term_acc = [{{Term, Key}}|A], count = C + 1}.

-spec merge(buffer()) -> buffer().
merge(#buffer{count = C, key_acc = Acc, type = T} = Buffer)
        when Acc == none, T == raw_count ->
    reply(Buffer, {T, C}),
    Buffer#buffer{count = 0};
merge(#buffer{key_acc = Acc, agg = Agg = Agg, type = T} = Buffer)
        when 
            Acc =/= none, Agg == none, T == keys ->
    reply(Buffer, {T, Acc}),
    Buffer#buffer{key_acc = [], count = 0};
merge(#buffer{rawkey_acc = Acc, agg = Agg = Agg, type = T} = Buffer)
        when 
            Acc =/= none, Agg == none, T == raw_keys ->
    reply(Buffer, {T, Acc}),
    Buffer#buffer{rawkey_acc = [], count = 0};
merge(#buffer{term_acc = Acc, agg = Agg = Agg, type = T} = Buffer)
        when 
            Acc =/= none , Agg == none, T == terms ->
    reply(Buffer, {T, Acc}),
    Buffer#buffer{term_acc = [], count = 0};
merge(#buffer{rawterm_acc = Acc, agg = Agg = Agg, type = T} = Buffer)
        when 
            Acc =/= none, Agg == none, T == raw_terms ->
    reply(Buffer, {T, Acc}),
    Buffer#buffer{rawterm_acc = [], count = 0};
merge(#buffer{rawkey_acc = Acc, agg = Agg, type = T} = Buffer)
        when Acc =/= none, is_record(Agg, key_agg), T == count  ->
    reply(Buffer, ping),
    Buffer#buffer{
        count = 0,
        rawkey_acc = [],
        agg =
            #key_agg{
                set = 
                    sets:union(
                        sets:from_list(Acc, ?VERSION),
                        Agg#key_agg.set
                    )
            }
    };
merge(
    #buffer{
        term_acc = Acc, agg = #termcount_agg{map = TCMap}, type = T} = Buffer
    )
        when
            Acc =/= none,
            is_map(TCMap),
            T == term_with_rawcount ->
    reply(Buffer, ping),
    UpdTCMap =
        lists:foldl(
            fun({{Term, _Key}}, FoldAcc) when is_map(FoldAcc), is_binary(Term) ->
                maps:update_with(Term, fun(V) -> V + 1 end, 1, FoldAcc)
            end,
            TCMap,
            Acc
        ),
    Buffer#buffer{
        count = 0,
        term_acc = [],
        agg = #termcount_agg{map = UpdTCMap}
    };
merge(
    #buffer{
        term_acc = Acc, agg = #termkeycount_agg{map = TKSMap}, type = T} = Buffer
    )
    when
        Acc =/= none,
        is_map(TKSMap),
        T == term_with_count ->
    reply(Buffer, ping),
    UpdTKSMap =
        lists:foldl(
            fun({{Term, Key}}, FoldAcc)
                    when is_map(FoldAcc), is_binary(Term), is_binary(Key) ->
                maps:update_with(
                    Term, 
                    fun(S) -> sets:add_element(Key, S) end, 
                    sets:add_element(Key, sets:new(?VERSION)),
                    FoldAcc
                )
            end,
            TKSMap,
            Acc
        ),
    Buffer#buffer{
        count = 0,
        term_acc = [],
        agg = #termkeycount_agg{map = UpdTKSMap}
    }.



-spec flush(buffer()) -> ok.
flush(Buffer) ->
    do_flush(Buffer),
    Duration = os:system_time(microsecond) - Buffer#buffer.start_time,
    ok = riak_kv_stat:update({query_vnode_time, Duration}),
    ok.
    
do_flush(#buffer{count = C, type = T} = Buffer) when T == raw_count ->
    reply(Buffer, {T, C});
do_flush(#buffer{rawkey_acc = Acc, type = T} = Buffer)
        when Acc =/= none, T == raw_keys ->
    reply(Buffer, {T, Acc});
do_flush(#buffer{key_acc = Acc, type = T} = Buffer)
        when Acc =/= none, T == keys ->
    reply(Buffer, {T, Acc});
do_flush(#buffer{rawterm_acc = Acc, type = T} = Buffer)
        when Acc =/= none, T == raw_terms ->
    reply(Buffer, {T, Acc});
do_flush(#buffer{term_acc = Acc, type = T} = Buffer)
        when Acc =/= none, T == terms ->
    reply(Buffer, {T, Acc});
do_flush(#buffer{type = T} = Buffer) ->
    UpdB = merge(Buffer),
    Result = 
        case {T, UpdB#buffer.agg} of
            {count, Agg} when is_record(Agg, key_agg) ->
                sets:size(Agg#key_agg.set);
            {term_with_rawcount, Agg} when is_record(Agg, termcount_agg) ->
                Agg#termcount_agg.map;
            {term_with_count, Agg} when is_record(Agg, termkeycount_agg) ->
                maps:map(
                    fun(_MK, KS) -> sets:size(KS) end,
                    Agg#termkeycount_agg.map
                )
        end,
    reply(Buffer, {T, Result}).

-spec aggregate(reply_type(), reply_type()|none) -> reply_type().
aggregate(R, none) ->
    R;
aggregate({T, KL}, {T, AggKL}) when T == keys ->
    {T, lists:umerge(lists:usort(KL), AggKL)};
aggregate({T, TKL}, {T, AggTKL}) when T == terms ->
    {T, lists:umerge(lists:usort(TKL), AggTKL)};
aggregate({T, KL}, {T, AggKL}) when T == raw_keys; T == raw_terms ->
    {T, KL ++ AggKL};
aggregate({T, C}, {T, AggC}) when T == raw_count; T == count ->
    {T, AggC + C};
aggregate({T, TM}, {T, AggTM})
        when T == term_with_rawcount; T == term_with_count ->
    {T, maps:merge_with(fun(_K, C, AggC) -> AggC + C end, TM, AggTM)}.

-spec reply(buffer(), reply_type()|ping) -> ok.
reply(#buffer{reply_fun = R}, Result) ->
    R(Result).


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

aggregator(ReplyFun, Agg) ->
    receive
        ping ->
            ReplyFun(ok),
            aggregator(ReplyFun, Agg);
        stop ->
            ReplyFun(Agg);
        check ->
            ReplyFun(Agg),
            aggregator(ReplyFun, Agg);
        Results ->
            ReplyFun(ok),
            aggregator(ReplyFun,aggregate(Results, Agg))
    end.

start_aggregator() ->
    Me = self(),
    ReplyFun = fun(M) -> Me ! M end,
    spawn(fun() -> aggregator(ReplyFun, none) end).

to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

to_term(N) ->
    list_to_binary(io_lib:format("T~8..0B", [N])).

keys_test() ->
    A = start_aggregator(),
    ReplyFun =
        fun(M) -> 
            case M of
                {keys, KL} ->
                    A ! {keys, lists:map(fun({K}) -> K end, KL)};
                SimpleM ->
                    A ! SimpleM
            end,
            receive ok -> ok end
        end,
    B = new({64, 0}, keys, ReplyFun),
    UpdB =
        lists:foldl(
            fun(I, BAcc) -> add(to_key(I), BAcc) end,
            B,
            lists:reverse(lists:seq(201, 300)) ++ 
                lists:seq(301, 1000) ++
                lists:seq(1, 100)
        ),
    ExpectedSize = (900 div 64) * 64,
    A ! check,
    {keys, L1} = get_reply(),
    ?assertMatch(ExpectedSize, length(L1)),
    ok = flush(UpdB), 
    A ! check,
    {keys, L2} = get_reply(),
    ?assertMatch(900, length(L2)),
    A ! {keys, lists:map(fun to_key/1, lists:seq(101, 200))},
    ok = get_reply(),
    ExpectedList = lists:map(fun to_key/1, lists:seq(1,1000)),
    A ! stop,
    {keys, L3} = get_reply(),
    ?assertMatch([], ExpectedList -- L3),
    ?assertMatch([], L3 -- ExpectedList),
    ?assertMatch(L3, lists:sort(L3)).

rawterms_test() ->
    A = start_aggregator(),
    ReplyFun = fun(M) -> A ! M, receive ok -> ok end end,
    Type = raw_terms,
    B = new({64, 0}, Type, ReplyFun),
    UpdB =
        lists:foldl(
            fun(I, Bacc) -> add({to_term(I), to_key(I)}, Bacc) end,
            B,
            lists:seq(1, 200)
        ),
    ExpectedSize = (200 div 64) * 64,
    A ! check,
    {Type, C1} = get_reply(),
    ?assertMatch(ExpectedSize, length(C1)),
    ok = flush(UpdB),
    A ! check,
    {Type, C2} = get_reply(),
    ?assertMatch(200, length(C2)),
    A ! stop,
    {Type, C3} = get_reply(),
    ?assertMatch(200, length(C3)).

match_count_test() ->
    A = start_aggregator(),
    ReplyFun = fun(M) -> A ! M, receive ok -> ok end end,
    Type = raw_count,
    B = new({64, 0}, Type, ReplyFun),
    UpdB =
        lists:foldl(
            fun(I, BAcc) -> add(to_key(I), BAcc) end,
            B,
            lists:seq(1, 100) ++ lists:seq(201, 1000)
        ),
    ExpectedSize = (900 div 64) * 64,
    A ! check,
    {Type, C1} = get_reply(),
    ?assertMatch(ExpectedSize, C1),
    ok = flush(UpdB), 
    A ! check,
    {Type, C2} = get_reply(),
    ?assertMatch(900, C2),
    A ! {Type, 100},
    ok = get_reply(),
    ExpectedCount = 1000,
    A ! stop,
    {Type, C3} = get_reply(),
    ?assertMatch(ExpectedCount, C3).

key_count_basic_test() ->
    A = start_aggregator(),
    ReplyFun = fun(M) -> A ! M, receive ok -> ok end end,
    Type = count,
    B = new({64, 0}, Type, ReplyFun),
    UpdB =
        lists:foldl(
            fun(I, BAcc) -> add(to_key(I), BAcc) end,
            B,
            lists:seq(1, 100) ++ lists:seq(201, 1000)
        ),
    A ! check,
    none = get_reply(),
    ok = flush(UpdB), 
    A ! check,
    {Type, C2} = get_reply(),
    ?assertMatch(900, C2),
    A ! {Type, 100},
    ok = get_reply(),
    ExpectedCount = 1000,
    A ! stop,
    {Type, C3} = get_reply(),
    ?assertMatch(ExpectedCount, C3).

match_count_dup_test() ->
    duplicate_count_tester(raw_count, 1100).

key_count_dup_test() ->
    duplicate_count_tester(count, 1000).

duplicate_count_tester(Type, ExpectedCount) ->
    A = start_aggregator(),
    ReplyFun = fun(M) -> A ! M, receive ok -> ok end end,
    B = new({64, 0}, Type, ReplyFun),
    UpdB =
        lists:foldl(
            fun(I, BAcc) -> add(to_key(I), BAcc) end,
            B,
            lists:seq(1, 1000)
        ),
    UpdWithDupB =
        lists:foldl(
            fun(I, BAcc) -> add(to_key(I + 10), BAcc) end,
            UpdB,
            lists:seq(1, 100)
        ),
    ok = flush(UpdWithDupB),
    A ! stop,
    {Type, C1} = get_reply(),
    ?assertMatch(ExpectedCount, C1).

term_with_keys_test() ->
    A = start_aggregator(),
    ReplyFun =
        fun(M) -> 
            case M of
                {terms, TKL} ->
                    A !
                        {
                            terms,
                            lists:map(fun({{T, K}}) -> {T, K} end, TKL)
                        };
                SimpleM ->
                    A ! SimpleM
            end,
            receive ok -> ok end
        end,
    B = new({64, 0}, terms, ReplyFun),
    UpdB =
        lists:foldl(
            fun(I, BAcc) ->
                add({to_term(I), to_key(rand:uniform(1000))}, BAcc)
            end,
            B,
            lists:reverse(lists:seq(201, 300)) ++ 
                lists:seq(301, 1000) ++
                lists:seq(1, 100)
        ),
    ExpectedSize = (900 div 64) * 64,
    A ! check,
    {terms, L1} = get_reply(),
    ?assertMatch(ExpectedSize, length(L1)),
    ok = flush(UpdB), 
    A ! check,
    {terms, L2} = get_reply(),
    ?assertMatch(900, length(L2)),
    A ! 
        {
            terms,
            lists:map(
                fun(I) -> {to_term(I), to_key(rand:uniform(1000))} end,
                lists:seq(101, 200)
            )
        },
    ok = get_reply(),
    {L1DupsSubset, _Rest} = lists:split(10, L1),
    A ! {terms, L1DupsSubset},
    ok = get_reply(),
    
    A ! stop,
    {terms, L3} = get_reply(),
    L4 =
        lists:filter(
            fun({T, K}) -> is_binary(T) andalso is_binary(K) end,
            L3
        ),
    ?assertMatch(1000, length(L3)),
    ?assertMatch(1000, length(L4)),
    ?assertMatch(L3, lists:sort(L3)).

term_with_count_test() ->
    duplicate_term_count_tester(term_with_count, 10, 11).

term_with_rawcount_test() ->
    duplicate_term_count_tester(term_with_rawcount, 90, 91).

duplicate_term_count_tester(Type, C1, C2) ->
    A = start_aggregator(),
    ReplyFun = fun(M) -> A ! M, receive ok -> ok end end,
    B = new({64, 0}, Type, ReplyFun),
    UpdB =
        lists:foldl(
            fun(I, BAcc) ->
                add({to_term(I rem 10), to_key(I rem 100)}, BAcc)
            end,
            B,
            lists:seq(1, 100) ++ lists:seq(201, 1000)
        ),
    ok = flush(UpdB), 
    A ! check,
    {Type, KC2} = get_reply(),
    ExpMap2 = lists:map(fun(I) -> {to_term(I), C1} end, lists:seq(0, 9)),
    ?assertMatch(10, maps:size(KC2)),
    ?assertMatch(C1, maps:get(to_term(0), KC2)),
    ?assertMatch(C1, maps:get(to_term(1), KC2)),
    ?assertMatch(ExpMap2, lists:sort(maps:to_list(KC2))),
    A ! 
        {
            Type,
            maps:from_list(
                lists:map(
                    fun(I) -> {to_term(I), 1} end,
                    lists:seq(0, 9)
                )
            )
        },
    ok = get_reply(),
    A ! stop,
    {Type, KC3} = get_reply(),
    ExpMap3 = lists:map(fun(I) -> {to_term(I), C2} end, lists:seq(0, 9)),
    ?assertMatch(ExpMap3, lists:sort(maps:to_list(KC3))).

get_reply() ->
    receive Reply -> Reply end.

-endif.