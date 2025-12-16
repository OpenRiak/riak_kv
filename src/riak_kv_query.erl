%% -------------------------------------------------------------------
%%
%% riak_kv_query: Riak module for preparing complex queries
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

-module(riak_kv_query).

-include_lib("kernel/include/logger.hrl").

-export(
    [
        new/3,
        finalise_request/1,
        get_query_definition/1,
        get_bucket/1,
        get_maxresults/1,
        get_r/1,
        get_timeout_secs/1,
        get_accumulator/1,
        get_querytype/1,
        get_returnterms/1,
        get_reqid/1,
        get_reqid/0,
        get_clientpid/1,
        get_result_encodingfun/1,
        add_aggregation_expression/2,
        add_accumulation_option/2,
        add_accumulation_term/2,
        add_maxresults/2,
        add_continuation/2,
        add_result_encodingfun/2,
        add_queries/3,
        make_continuation/2,
        is_query/1
    ]
).

-define(CONT_SK, <<"start_key">>).
-define(CONT_ST, <<"start_term">>).

-type query_type()
    :: single_query | combo_query.
-type aggregation_function()
    :: fun((list(sets:set(riak_object:key()))) -> sets:set(riak_object:key())).
-type smpl_accumulator()
    :: keys|raw_keys|count|raw_count.
-type term_accumulator()
    :: raw_terms|terms|term_with_rawcount|term_with_count.
-type accumulation_option()
    :: smpl_accumulator()|term_accumulator().
-type query_index_name()
    :: binary().
-type index_limiter()
    :: binary(). % Only binary indexes supported
-type capture_value() :: binary()|integer().
-type query_filter_fun() ::
    fun((#{binary() => capture_value()}) -> boolean()).
-type query_eval_fun() ::
    fun((binary(), binary()) -> #{binary() => capture_value()}).
-type query_expression() :: 
    {query, query_eval_fun(), query_filter_fun()}.
-type plain_regex() :: iodata().
-type exported_regex() :: {exported, term()}.
    %% Not supported prior to OTP 28.1.  Then the type should be changed to
    %% {exported, re:exported()}.
-type compiled_regex() ::
    {node(), {re_pattern, term(), term(), term(), term()}} |
    exported_regex().
-type actual_regex() :: 
    {regex, plain_regex(), compiled_regex()|none}.
-type term_expression() ::
    actual_regex()|undefined|query_expression().

-type substitutions() ::
    #{binary() => binary()|integer()}|#{}.
-type aggregation_tag() ::
    pos_integer().
-type regular_expression() ::
    string()|undefined.
-type evaluation_expression() ::
    string()|undefined.
-type filter_expression() ::
    string()|undefined.
-type query_user_input() ::
    {
        aggregation_tag()|undefined,
        query_index_name(),
        index_limiter(),
        index_limiter(),
        regular_expression(), 
        evaluation_expression(),
        filter_expression()
    }.
-type evaluated_query() ::
    {
        query_index_name(),
        index_limiter(),
        index_limiter(),
        term_expression()
    }.
-type continuation_query() ::
    {
        query_index_name(),
        {index_limiter(), riak_object:key()},
        index_limiter(),
        term_expression()
    }.
-type query_definition() ::
    evaluated_query()|
    continuation_query()|
    {aggregation_function(), list({aggregation_tag(), evaluated_query()})}.
-type validation_stage() ::
    aggregation_expression|accumulation_option|accumulation_term|
        max_results|query_evaluation|apply_continuation.
-type validation_error() ::
    {error, validation_stage(), binary()}.
-type encoding_fun() ::
    fun((riak_kv_query_server:results()) -> binary()).

-record(riak_kv_query,
    {
        bucket
            :: riak_object:bucket(),
        type = single_query
            :: single_query | combo_query,
        timeout_secs
            :: pos_integer(),
        aggregation_expression = single_query
            :: single_query|aggregation_function(),
        accumulation_option = keys
            :: accumulation_option(),
        accumulation_term = <<"$term">>
            :: binary(),
        max_results = unlimited
            :: pos_integer()|unlimited,
        r = 1
            :: pos_integer(),
        query
            :: 
                undefined |
                evaluated_query() |
                continuation_query() |
                list({aggregation_tag(), evaluated_query()}),
        client_pid
            :: pid() | undefined,
        client_reqid
            :: non_neg_integer() | undefined,
        result_encodingfun = raw
            :: raw|encoding_fun()
    }
).

-type complex_query_definition() :: #riak_kv_query{}.

-export_type(
    [
        query_type/0,
        aggregation_function/0,
        aggregation_tag/0,
        accumulation_option/0,
        query_expression/0,
        term_expression/0,
        compiled_regex/0,
        plain_regex/0,
        evaluated_query/0,
        validation_error/0,
        query_definition/0,
        complex_query_definition/0,
        query_user_input/0,
        encoding_fun/0
    ]
).

-spec new(riak_object:bucket(), query_type(), pos_integer()) -> complex_query_definition().
new(Bucket, single_query, Timeout) ->
    #riak_kv_query{
        bucket = Bucket,
        type = single_query,
        timeout_secs = Timeout,
        aggregation_expression = single_query
    };
new(Bucket, combo_query, Timeout) ->
    #riak_kv_query{
        bucket = Bucket,
        type = combo_query,
        timeout_secs = Timeout
    }.

-spec finalise_request(
    complex_query_definition()) -> complex_query_definition().
finalise_request(Query) ->
    ClientPID = self(),
    ReqID = get_reqid(),
    Query#riak_kv_query{client_pid = ClientPID, client_reqid = ReqID}.

-spec get_query_definition(
    complex_query_definition()) -> query_definition().
get_query_definition(#riak_kv_query{type = Type} = Query)
        when Type == single_query ->
    case Query#riak_kv_query.query of
        EvaluatedQuery when is_tuple(EvaluatedQuery) ->
            EvaluatedQuery
    end;
get_query_definition(
    #riak_kv_query{type = Type, aggregation_expression = AE} = Query)
        when Type == combo_query, AE =/= single_query ->
    case Query#riak_kv_query.query of
        EvaluatedQueryList when is_list(EvaluatedQueryList) ->
            {AE, EvaluatedQueryList}
    end.

-spec get_bucket(complex_query_definition()) -> riak_object:bucket().
get_bucket(Query) -> Query#riak_kv_query.bucket.

-spec get_timeout_secs(complex_query_definition()) -> pos_integer().
get_timeout_secs(Query) -> Query#riak_kv_query.timeout_secs.

-spec get_reqid(complex_query_definition()) -> non_neg_integer().
get_reqid(#riak_kv_query{client_reqid = ReqID}) when ReqID =/= undefined ->
    ReqID.

-spec get_clientpid(complex_query_definition()) -> pid().
get_clientpid(#riak_kv_query{client_pid = PID}) when PID =/= undefined ->
    PID.

-spec get_accumulator(
    complex_query_definition()) ->
        smpl_accumulator()|term_accumulator().
get_accumulator(#riak_kv_query{accumulation_option= AccO}) ->
    AccO.

-spec get_returnterms(complex_query_definition()) -> boolean()|binary().
get_returnterms(#riak_kv_query{max_results = MR}) when is_integer(MR) ->
    true;
get_returnterms(
    #riak_kv_query{
        max_results = unlimited, accumulation_option = AO, accumulation_term = AT
    }) ->
    case AO of
        KeyOnly
            when 
                KeyOnly == keys;
                KeyOnly == raw_keys;
                KeyOnly == count;
                KeyOnly == raw_count ->
            false;
        _ ->
            case AT of
                Term when Term == <<"$term">> ->
                    true;
                Term when is_binary(Term) ->
                    Term
            end
    end.

-spec get_maxresults(complex_query_definition()) -> unlimited|pos_integer().
get_maxresults(Query) -> Query#riak_kv_query.max_results.

-spec get_r(complex_query_definition()) -> pos_integer().
get_r(Query) -> Query#riak_kv_query.r.

-spec get_querytype(complex_query_definition()) -> query_type().
get_querytype(Query) -> Query#riak_kv_query.type.

-spec get_result_encodingfun(complex_query_definition()) -> raw|encoding_fun().
get_result_encodingfun(Query) -> Query#riak_kv_query.result_encodingfun.

-spec add_aggregation_expression(
    complex_query_definition(), string()|undefined)
        -> {ok, complex_query_definition()}|validation_error().
add_aggregation_expression(
    #riak_kv_query{type = Type} = Query, AggregationString)
        when Type == combo_query, AggregationString =/= undefined ->
    case leveled_setop:generate_setop_function(AggregationString) of
        {error, ParseError} ->
            ?LOG_WARNING(
                "Invalid aggregation submitted ~s due to reason ~0p",
                [AggregationString, ParseError]
            ),
            {error, aggregation_expression, <<"Invalid function">>};
        AppFunction ->
            {
                ok,
                Query#riak_kv_query{aggregation_expression = AppFunction}
            }
    end;
add_aggregation_expression(
    #riak_kv_query{type = Type} = Query, undefined)
        when Type == single_query ->
    {ok, Query};
add_aggregation_expression(_Query, _AggregationString) ->
    {
        error,
        aggregation_expression,
        <<"Attempt to aggregate single query">>
    }.

-spec add_accumulation_option(
    complex_query_definition(), binary()|undefined) ->
        {ok, complex_query_definition()}|validation_error(). 
add_accumulation_option(
    #riak_kv_query{type = Type} = Query,
    AccumulationOption)
        when
            Type == combo_query andalso
            (
                AccumulationOption == <<"keys">> orelse
                AccumulationOption == <<"raw_keys">> orelse
                AccumulationOption == <<"raw_count">>
            ) ->
    {
        ok,
        Query#riak_kv_query{
            accumulation_option = decode_option(AccumulationOption)}
        
    };
add_accumulation_option(
    #riak_kv_query{type = Type} = Query,
    AccumulationOption)
        when
            AccumulationOption == <<"keys">>;
            AccumulationOption == <<"raw_keys">>;
            AccumulationOption == <<"count">>;
            AccumulationOption == <<"raw_count">>;
            AccumulationOption == <<"terms">>;
            AccumulationOption == <<"raw_terms">>;
            AccumulationOption == <<"term_with_rawcount">>;
            AccumulationOption == <<"term_with_count">> ->
    case Type of
        single_query ->
            {
                ok,
                Query#riak_kv_query{
                    accumulation_option = decode_option(AccumulationOption)}
            };
        combo_query ->
            {
                error,
                accumulation_option,
                <<"Unsupported option in combination query">>
            }
    end;
add_accumulation_option(Query, undefined) ->
    {ok, Query};
add_accumulation_option(_Q, BadOption) when is_binary(BadOption) ->
    {
        error,
        accumulation_option,
        iolist_to_binary(
            io_lib:format(<<"Unrecognised option ~0p">>, [BadOption])
        )
    }.

-spec add_accumulation_term(
    complex_query_definition(), binary()|undefined) ->
        {ok, complex_query_definition()}|validation_error().
add_accumulation_term(
    #riak_kv_query{accumulation_option = AccOpt} = Query,
    AccumulationTerm)
        when
            is_binary(AccumulationTerm) andalso
            (
                AccOpt == terms orelse
                AccOpt == raw_terms orelse
                AccOpt == term_with_rawcount orelse
                AccOpt == term_with_count
            ) ->
    {
        ok,
        Query#riak_kv_query{
            accumulation_term = AccumulationTerm}
    };
add_accumulation_term(Query, undefined) ->
    {ok, Query};
add_accumulation_term(
        #riak_kv_query{accumulation_option = AccOpt}, AccumulationTerm) ->
    {
        error,
        accumulation_term,
        iolist_to_binary(
            io_lib:format(
                <<"Bad term ~0p with option ~w">>, [AccumulationTerm, AccOpt])
        )
        
    }.

-spec add_maxresults(complex_query_definition(), pos_integer()) ->
    {ok, complex_query_definition()}|validation_error().
add_maxresults(#riak_kv_query{accumulation_option = AccOpt, type = T} = Q, MR)
        when is_integer(MR), MR > 0 ->
    case {AccOpt, T} of
        {AccOpt, single_query} when AccOpt == raw_keys; AccOpt == terms ->
            {ok, Q#riak_kv_query{max_results = MR}};
        {MaybeBadAccOpt, MaybeBadType} ->
            {
                error,
                max_results,
                iolist_to_binary(
                    io_lib:format(
                        "Invalid combination max_results ~0p "
                        "query_type ~w accumulation_option ~0p",
                        [MR, MaybeBadType, MaybeBadAccOpt]))
            }
    end;
add_maxresults(_Query, InvalidMax) ->
    {
        error,
        max_results,
        iolist_to_binary(
            io_lib:format(<<"Invalid max_results ~0p">>, [InvalidMax]))
    }.

-spec add_continuation(complex_query_definition(), binary()) ->
    {ok, complex_query_definition()}|validation_error().
add_continuation(#riak_kv_query{type = T, query = Q} = Query, Cont) ->
    try
        true = T == single_query,
        case riak_kv_wm_json:decode(base64:decode(Cont)) of
            QMap when is_map(QMap) ->
                {Idx, _ST, ET, Expr} = Q,
                case {maps:get(?CONT_SK, QMap), maps:get(?CONT_ST, QMap)} of
                    {SK, ST0} when is_binary(SK), is_binary(ST0) ->
                        {ok, Query#riak_kv_query{query = {Idx, {ST0, SK}, ET, Expr}}}
                end
        end
    catch
        error:Reason ->
            ?LOG_WARNING(
                "Invalid continuation failed due to Reason ~0p",
                [Reason]
            ),
            {error, apply_continuation, <<"Invalid continuation">>}
    end.

-spec add_result_encodingfun(complex_query_definition(), encoding_fun())
        -> {ok, complex_query_definition()}.
add_result_encodingfun(Query, EncodingFun) when is_function(EncodingFun) ->
    {ok, Query#riak_kv_query{result_encodingfun = EncodingFun}}.

-spec add_queries(
    complex_query_definition(),
    list(query_user_input()),
    substitutions())
        -> {ok, complex_query_definition()}|validation_error().
add_queries(Query, Queries, Subs) ->
    case validate_substitutions(Subs) of
        ok ->
            case Query#riak_kv_query.type of
                single_query when length(Queries) == 1 ->
                    [SingleQuery] = Queries,
                    case evaluate_query(single, SingleQuery, Subs) of
                        {ok, EvaluatedQuery} ->
                            {
                                ok,
                                Query#riak_kv_query{query = EvaluatedQuery}
                            };
                        Error ->
                            Error
                    end;
                combo_query when is_list(Queries) ->
                    case evaluate_combo_queries(Queries, Subs) of
                        {ok, EvaluatedQueries} ->
                            {
                                ok,
                                Query#riak_kv_query{query = EvaluatedQueries}
                            };
                        Error ->
                            Error
                    end;
                _ ->
                    {
                        error,
                        query_evaluation,
                        <<"Multiple queries passed without aggregation expression">>
                    }
            end;
        Error ->
            Error
    end.

-spec make_continuation(index_limiter(), riak_object:key()) -> string().
make_continuation(StartTerm, StartKeyExclusive) ->
    M = #{?CONT_ST => StartTerm, ?CONT_SK => StartKeyExclusive},
    base64:encode_to_string(iolist_to_binary(riak_kv_wm_json:encode(M))).

-spec validate_substitutions(substitutions()) -> ok|validation_error().
validate_substitutions(Subs) ->
    try
        maps:foreach(
            fun(K, V) when
                is_binary(K), (is_binary(V) orelse is_integer(V)) ->
                ok
            end,
            Subs
        ),
        ok
    catch
        error:Reason ->
            ?LOG_WARNING(
                "Invalid type within substitutions "
                "failed due to Reason ~0p",
                [Reason]
            ),
            {error, query_evaluation, <<"Invalid type in substitution">>}
    end.

evaluate_combo_queries(Queries, Subs) ->
    SortedQueries = lists:ukeysort(1, Queries),
    case length(SortedQueries) of
        L when L == length(Queries) ->
            evaluate_combo_queries(SortedQueries, Subs, []);
        _UL ->
            {error, query_evaluation, <<"Incorrect tagging of queries">>}
    end.

evaluate_combo_queries([], _Subs, Acc) ->
    {ok, lists:reverse(Acc)};
evaluate_combo_queries([Q|Rest], Subs, Acc) ->
    case evaluate_query(multi, Q, Subs) of
        {AT, {ok, EvaluatedQuery}} ->
            evaluate_combo_queries(Rest, Subs, [{AT, EvaluatedQuery}|Acc]);
        {_AT, Error} ->
            Error
    end.

evaluate_query(multi, {AT, IN, ST, ET, RE, EE, FE}, Subs) ->
    case AT of
        PI when is_integer(PI), PI >= 0 ->
            {AT, evaluate_query(single, {AT, IN, ST, ET, RE, EE, FE}, Subs)};
        _ ->
            {AT,
                {
                    error,
                    query_evaluation,
                    <<"Untagged query in combination request">>
                }
            }
    end;
evaluate_query(single, {_AT, IN, ST, ET, RE, EE, FE}, Subs) ->
    IndexL = string:length(IN),
    case string:slice(IN, IndexL - 4) of
        <<"_bin">> ->
            case {ST, ET} of
                {ST, ET} when is_binary(ST), is_binary(ET), ST =< ET ->
                    case {RE, EE, FE} of
                        {undefined, undefined, undefined} ->
                            {ok, {IN, ST, ET, undefined}};
                        {REOnly, undefined, undefined} ->
                            case re:compile(REOnly) of
                                {ok, MP} ->
                                    {
                                        ok,
                                        {
                                            IN,
                                            ST,
                                            ET,
                                            {regex, REOnly, {node(), MP}}
                                        }
                                    };
                                {error, ErrSpec} ->
                                    ?LOG_WARNING(
                                        "Invalid regular expression "
                                        "passed to query ~0p",
                                        [ErrSpec]
                                    ),
                                    {
                                        error,
                                        query_evaluation,
                                        <<"Invalid regex">>
                                    }
                            end;
                        {undefined, EE0, FE0}
                                when FE0 =/= undefined, EE0 =/= undefined ->
                            case evaluate_expression(EE0, FE0, Subs) of
                                {query, QEF, QFF} ->
                                    {ok, {IN, ST, ET, {query, QEF, QFF}}};
                                Error ->
                                    Error
                            end;
                        {REI, EEI, FEI} ->
                            ?LOG_WARNING(
                                "Invalid combination of ~p ~p ~p",
                                [REI, EEI, FEI]
                            ),
                            {
                                error,
                                query_evaluation,
                                <<"Invalid combination of regex/eval/filter">>
                            }
                    end;
                _ ->
                    {error, query_evaluation, <<"Invalid query range">>}
            end;
        _ ->
            {error, query_evaluation, <<"Invalid index name">>}
    end.


evaluate_expression(EvalExpr, FilterExpr, Subs) ->
    case leveled_eval:generate_eval_function(EvalExpr, Subs) of
        EF when is_function(EF, 2) ->
            case leveled_filter:generate_filter_function(FilterExpr, Subs) of
                FF when is_function(FF, 1) ->
                    {query, EF, FF};
                {error, Error} ->
                    ?LOG_WARNING(
                        "Invalid filter function ~s due to reason ~0p",
                        [FilterExpr, Error]
                    ),
                    {error, query_evaluation, <<"Invalid filter function">>}
            end;
        {error, Error} ->
            ?LOG_WARNING(
                "Invalid eval function ~s due to reason ~0p",
                [evalExpr, Error]
            ),
            {error, query_evaluation, <<"Invalid eval function">>}
    end.

-spec decode_option(binary()) -> accumulation_option().
decode_option(<<"keys">>) -> keys;
decode_option(<<"raw_keys">>) -> raw_keys;
decode_option(<<"count">>) -> count;
decode_option(<<"raw_count">>) -> raw_count;
decode_option(<<"terms">>) -> terms;
decode_option(<<"raw_terms">>) -> raw_terms;
decode_option(<<"term_with_rawcount">>) -> term_with_rawcount;
decode_option(<<"term_with_count">>) -> term_with_count.

-spec get_reqid() -> non_neg_integer().
get_reqid() ->
    erlang:phash2({self(), os:timestamp(), crypto:strong_rand_bytes(2)}).

-spec is_query(complex_query_definition()) -> boolean().
is_query(Query) -> is_record(Query, riak_kv_query).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

new(Bucket, Type) -> new(Bucket, Type, 60).

bad_aggregation_expression_test() ->
    QS = new({<<"Type">>, <<"Bucket">>}, single_query),
    ?assertMatch(
        {error, aggregation_expression, <<"Attempt to aggregate single query">>},
        add_aggregation_expression(QS, <<"$1 UNION $2">>)
    ),
    QC = new(<<"Bucket">>, combo_query),
    ?assertMatch(
        {error, aggregation_expression, <<"Invalid function">>},
        add_aggregation_expression(QC, <<"$1 UNION S2">>)
    ),
    ?assertMatch(
        {error, aggregation_expression, <<"Invalid function">>},
        add_aggregation_expression(QC, <<"$1 XOR $2">>)
    ),
    {ok, UpdQ} = add_aggregation_expression(QC, <<"$1 UNION $2">>),
    ?assert(is_record(UpdQ, riak_kv_query)).

bad_accumulation_option_test() ->
    QC = new(<<"Bucket">>, combo_query),
    ?assertMatch(
        {
            error,
            accumulation_option,
            <<"Unsupported option in combination query">>
        },
        add_accumulation_option(QC, <<"terms">>)
    ),
    QS = new({<<"Type">>, <<"Bucket">>}, single_query),
    ?assertMatch(
        {
            error,
            accumulation_option,
            <<"Unrecognised option <<\"trms\">>">>
        },
        add_accumulation_option(QS, <<"trms">>)
    ).

bad_maxresults_test() ->
    QS = new(<<"bucket">>, single_query),
    {ok, QS1} = add_accumulation_option(QS, <<"term_with_count">>),
    ?assertMatch(
        {
            error,
            max_results,
            <<
                "Invalid combination max_results 1000 "
                "query_type single_query "
                "accumulation_option term_with_count"
            >>
        },
        add_maxresults(QS1, 1000)
    ).

bad_accumulation_term_test() ->
    QS = new(<<"bucket">>, single_query),
    {ok, QS1} = add_accumulation_option(QS, <<"term_with_count">>),
    ?assertMatch(
        {
            error,
            accumulation_term,
            <<"Bad term 0 with option term_with_count">>
        },
        add_accumulation_term(QS1, 0)
    ),
    {ok, QS2} = add_accumulation_option(QS, <<"count">>),
    ?assertMatch(
        {
            error,
            accumulation_term,
            <<"Bad term <<\"$term\">> with option count">>
        },
        add_accumulation_term(QS2, <<"$term">>)
    ).

bad_singlequery_test() ->
    QS = new(<<"bucket">>, single_query),
    EE = <<"delim($term, :delim, ($surname, $dob, $dod, $gns, $pcs))">>,
    FE = 
        <<"($dob BETWEEN :low_dob AND :high_dob) "
            "AND (contains($gns, \"#Willow\") AND contains($pcs, \"#LS\"))">>,
    Subs =
        #{
            <<"delim">> => <<"|">>,
            <<"low_dob">> => <<"19740301">>,
            <<"high_dob">> => <<"19761030">>
        },
    {ok, R} = 
        add_queries(
            QS,
            [{
                undefined,
                <<"index_bin">>,
                <<"Will">>,
                <<"Wilm">>,
                undefined,
                EE,
                FE
            }],
            Subs
        ),
    ?assertMatch(
        {<<"index_bin">>, <<"Will">>, <<"Wilm">>, _QE},
        R#riak_kv_query.query
    ),
    BadSubs1 = maps:remove(<<"delim">>, Subs),
    ?assertMatch(
        {
            error,
            query_evaluation,
            <<"Invalid eval function">>
        },
        add_queries(
            QS,
            [{
                undefined,
                <<"index_bin">>,
                <<"Will">>,
                <<"Wilm">>,
                undefined,
                EE,
                FE
            }],
            BadSubs1
        )
    ),
    BadSubs2 = maps:put(<<"delim">>, "/", BadSubs1),
    ?assertMatch(
        {
            error,
            query_evaluation,
            <<"Invalid type in substitution">>
        },
        add_queries(
            QS,
            [{
                undefined,
                <<"index_bin">>,
                <<"Will">>,
                <<"Wilm">>,
                undefined,
                EE,
                FE
            }],
            BadSubs2
        )
    ),
    BadSubs3 = maps:remove(<<"low_dob">>, Subs),
    ?assertMatch(
        {
            error,
            query_evaluation,
            <<"Invalid filter function">>
        },
        add_queries(
            QS,
            [{
                undefined,
                <<"index_bin">>,
                <<"Will">>,
                <<"Wilm">>,
                undefined,
                EE,
                FE
            }],
            BadSubs3
        )
    ),
    QC = new(<<"Bucket">>, combo_query),
    ?assertMatch(
        {
            error,
            query_evaluation,
            <<"Untagged query in combination request">>
        },
        add_queries(
            QC,
            [{
                undefined,
                <<"index_bin">>,
                <<"Will">>,
                <<"Wilm">>,
                undefined,
                EE,
                FE
            }],
            Subs
        )
    ),
    BadIndex = <<"index_int">>,
    ?assertMatch(
        {
            error,
            query_evaluation,
            <<"Invalid index name">>
        },
        add_queries(
            QS,
            [{
                undefined,
                BadIndex,
                <<"Will">>,
                <<"Wilm">>,
                undefined,
                EE,
                FE
            }],
            Subs
        )
    ),
    % Revert Start and End term
    ?assertMatch(
        {
            error,
            query_evaluation,
            <<"Invalid query range">>
        },
        add_queries(
            QS,
            [{
                undefined,
                <<"index_bin">>,
                <<"Wilm">>,
                <<"Will">>,
                undefined,
                EE,
                FE
            }],
            Subs
        )
    ),
    % Use integer in range
    ?assertMatch(
        {
            error,
            query_evaluation,
            <<"Invalid query range">>
        },
        add_queries(
            QS,
            [{
                undefined,
                <<"index_bin">>,
                0,
                <<"Wilm">>,
                undefined,
                EE,
                FE
            }],
            Subs
        )
    ),
    % Bad regular expression
    ?assertMatch(
        {
            error,
            query_evaluation,
            <<"Invalid regex">>
        },
        add_queries(
            QS,
            [{
                undefined,
                <<"index_bin">>,
                <<"Will">>,
                <<"Wilm">>,
                <<"(?<extract_but_extra_parenthesis>.*))">>,
                undefined,
                undefined
            }],
            Subs
        )
    ),
    BEE1 = <<"delm($term, :delim, ($surname, $dob, $dod, $gns, $pcs))">>,
    ?assertMatch(
        {
            error,
            query_evaluation,
            <<"Invalid eval function">>
        },
        add_queries(
            QS,
            [{
                undefined,
                <<"index_bin">>,
                <<"Will">>,
                <<"Wilm">>,
                undefined,
                BEE1,
                FE
            }],
            Subs
        )
    ),
    BFE1 = 
        <<"($dob BETWEEN :low_dob NAND :high_dob) "
            "AND (contains($gns, \"#Willow\") AND contains($pcs, \"#LS\"))">>,
    ?assertMatch(
        {
            error,
            query_evaluation,
            <<"Invalid filter function">>
        },
        add_queries(
            QS,
            [{
                undefined,
                <<"index_bin">>,
                <<"Will">>,
                <<"Wilm">>,
                undefined,
                EE,
                BFE1
            }],
            Subs
        )
    ),
    % Invalid combination
    ?assertMatch(
        {
            error,
            query_evaluation,
            <<"Invalid combination of regex/eval/filter">>
        },
        add_queries(
            QS,
            [{
                undefined,
                <<"index_bin">>,
                <<"Will">>,
                <<"Wilm">>,
                <<"(?<extract>.*)">>,
                undefined,
                BFE1
            }],
            Subs
        )
    ),
    % Simple query with no expressions allowed
    {ok, R2} = 
        add_queries(
            QS,
            [{
                undefined,
                <<"index_bin">>,
                <<"Will">>,
                <<"Wilm">>,
                undefined,
                undefined,
                undefined
            }],
            maps:new()
        ),
    ?assertMatch(
        {<<"index_bin">>, <<"Will">>, <<"Wilm">>, _QE2},
        R2#riak_kv_query.query
    ).

validate_subs_intint_test() ->
    ?assertMatch(
        {error, query_evaluation, <<"Invalid type in substitution">>},
        validate_substitutions(#{12 => 12})
    ),
    ?assertMatch(
        ok,
        validate_substitutions(#{<<"binkey">> => 12})
    ).

-endif.