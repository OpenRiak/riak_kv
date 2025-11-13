# Riak KV - Query API

Riak supports a query language for secondary indexes, whereby range queries can be run on indexes, with the index entries containing projected attributes, which can be filtered using a filter expression language.

- [Adding Index Entries to Objects](#secondary-indexes---adding-index-entries-to-an-object)
- [Overview of querying those index entries](#secondary-indexes---querying-index-entries-overview)
- [An example people search](#example-1---a-simple-people-search-index)
- [An alternative example for people search](#example-2---an-alternative-people-search)
- [An example using the API for reporting](#example-3---reporting-index)
- [Setting performance expectations for queries](#performance-expectation)
- [A more formal description of the Query API](#query---definition)
- [Some notes on the underlying implementation](#notes-on-implementation)

## Secondary Indexes - Adding Index Entries to an Object

Querying in Riak is based around secondary indexes.  A Riak secondary index entry is a combination of a field, a term and an object key: where a field is a name for an index within a bucket, and a term is a sortable binary string that represents a value for a given key on that index, and the object key is the standard result of the query.

When an object is PUT into Riak, the PUT should include ALL the index entries for that object - the entirety of the current expected state.  Internally Riak will calculate the delta from the previously stored index entries, and only make the necessary key changes.  An individual object can have an unlimited number of index entries in total, and an unlimited number of terms on any given field (subject to limits in the HTTP infrastructure used for the request).

When an object is fetched from Riak, it will be returned with all its current Index values.

There is no out-of-the-box support for schema management within Riak, as Riak is intended to be independent of the format of the actual object body.  In general, applications that use secondary indexes within Riak will write an extension to the Riak client to examine the object body and calculate the required index entries, before completing a PUT.  It should be noted that as the schema is managed externally to Riak, schema changes are also required to be managed within the application.  Consideration of how to make such schema changes is the responsibility of the application designer e.g. versioning, rolling updates, querying-planning during transition etc.  

The design of secondary indexes in Riak makes them best suited to environments where the query demands are relatively predictable in advance, and also the approximate cardinality of the data elements.

Index entries can be made up of simple sorted keys:

e.g. `surname_bin: SMITH`

Index terms can be extended by projecting additional attributes onto the sort key, appended to the sort key, e.g. in this case by appending the date of birth to the sort key:

e.g. `surnamedob_bin: SMITH|19790613`

There is no pre-defined way of how to project attributes onto an index term in Riak 3.4; the definition, formatting and appending of projected attributes is the responsibility of the application.  There are mechanisms available within Riak to extract, filter-on and return projected attributes at query time - and it is a requirement of application design to ensure that projected attributes are appended in a way that is both flexible and efficient (within Riak).

## Secondary Indexes - Querying Index Entries Overview

A query consists of the following components:

- A bucket (required).
- A field (required).
- A range (required).
- An evaluation expression (optional); used to decode projected attributes to provide a map of those attributes to be processed via a filter expression.
- A filter expression (optional); used to filter results in/out of queries by applying checks to a map of projected attributes discovered on the index entry (using a filter expression).
- A regular expression (optional); a potentially less flexible, but commonly more performant alternative to evaluation and filter expressions - where a regular expression match against a term is used to filter the term in or out.
- A result aggregation method (optional); a mechanism for describing the type of results required, and how they should be sorted (e.g. just matching object keys, terms and keys, keys by specific attribute).

Queries can be sent individually, but it is also possible to send multiple queries along with an aggregation expression to define how the query results will be combined (e.g. using `INTERSECT`, `UNION`, `NOT`) - where Riak will provide a single set of results as a response based on the aggregation expression.

Queries are requested by posting a JSON object which defines the query to the HTTP URI on Riak of `types/BucketType/buckets/Bucket/query`.  The results are returned as a JSON object.

The Query can pass `substitutions`, a JSON array mapping keys with any string-based tag to values.  Substitutions are useful when a single query template is to be used within the application client, or to avoid difficulty with escaping special characters embedded within query elements.

In the development of Riak, it is assumed that in most production Riak systems, less than 1% of all transactions are secondary index queries, and this is reflected in the transaction mix of pre-release non-functional testing.  A secondary index query is generally 2 orders of magnitude more expensive in terms of CPU cost across the cluster than a standard GET, to fetch just a single index term.  To complete a query, it is necessary to complete an operation in at least `RingSize div n_val` vnodes, rather than `n_val` vnodes for a GET.  It is possible to drive up the volume of 2i queries, with real-world production examples of more than 10K queries per second being achieved - but such relatively high query volumes are not core to the Riak use case.

There is a relatively fixed cost per query, even where 0 results are returned; there is a marginal difference in the cost of scanning 10K index entries and scanning 10.  Queries for large sets of results are possible in a single round trip.  The query process will be greedy for CPU resource to complete the query, there is no constraint on how many CPU cores a query can use - up to a maximum of `RingSize div n_val` across the cluster.  The Erlang scheduler will generally negotiate fair use between queries and other user requests.

The aim of Riak development is to provide a database that performs efficient, scalable and predictable CRUD operations, and is just-queryable-enough to avoid the need of third party database integration in many cases.  However, Riak does support via an external replication API, the ability for the developer to  manage replication and reconciliation to third party query engines (e.g. OpenSearch), should more complex query support be required.

## Example (1) - A Simple People Search Index

In this example, the database contains people whose records are stored under a unique individual identifier (the primary Key used in Riak).  There is also a requirement to search for people to find potential matches where the unique identifier is not known, and in these searches the following criteria can be provided to the query:

- Date Of birth (required).
- Primary family name (optional).
- All known given names (optional).
- Primary [postal code](https://en.wikipedia.org/wiki/Postal_code) (optional).

For all queryable attributes approximate entries are allowed.  The Date of Birth can be a range rather than a specific date, the names and post codes require a minimal prefix (e.g. first two characters), but wildcards may be provided for unknown parts.

For this example, a single index per record is constructed to support these queries, whereby the Date Of Birth would be the sort key, the family name, given names and postcode could be added as projected attributes - with `|` used as a delimiter between the types of attributes and `.` used as a delimiter between the individual attributes of a given type (which in this case is only for given names).

So a sample person born on 1st May 1965, with current family name of SMITH; known by given names of ANNE, MARIE & ANNE-MARIE; and has registered a home Postal Code (LS9 0TW).  They would then be represented by the following index entry:

`peoplefinder_bin: 19650501|SMITH|ANNE.MARIE.ANNE-MARIE|LS9_0TW`

### Example (1) - Simple Range Query

To find all the people with a given date of birth, a simple range query could be used:

```json
    {
        "query_list" :
            [
                {
                    "index_name" : "peoplefinder_bin",
                    "start_term" : "19650501",
                    "end_term"   : "19650502"
                }
            ]
    }
```

This is the equivalent to finding all those born on "19650501" in "YYYMMDD" format.  As all index entries have additional information appended, the `end_term` "19650502" is lexicographically before any of the index entries for those born on "19650502" e.g. `"19650502" < "19650502|...."`.  As no `accumulation_option` has been set, this will return a list of keys for those people born on that day.

### Example (1) - Finding an Exact Match

To find an exact match on a subset of the provided data (e.g. Date Of Birth = 19650501, FamilyName = SMITH, GivenName = ANNE), the following query could be used:

```json
    {
        "substitutions" : {"dl1" : "|", "dl2" : ".", "qfn" : "SMITH", "qgn" : "ANNE"},
        "query_list" :
            [
                {
                    "index_name" : "peoplefinder_bin",
                    "start_term" : "19650501",
                    "end_term"   : "19650501~",
                    "evaluation_expression" : "delim($term, :dl1, ($dob, $fn, $gn, $pc)) | split($gn, :dl2, $gn)",
                    "filter_expression" : "($fn = :qfn ) AND (:qgn IN $gn)"
                }
            ]
    }
```

This query defines some `substitutions`, of the delimiters (to simplify escaping required when passing text into the expressions), and of the actual terms to be queried (which can make it easier within the application to transpose user input into standard query templates).  The substitutions can be referred to within expressions by prepending the substitution name with `:`.

e.g. the substitution of `{"qfn", : "SMITH", "qgn", "ANNE"}` will translate the filter expression `"($fn = :qfn ) AND (:qgn IN $gn)"` into `"($fn = SMITH ) AND (ANNE IN $gn)"`.

The query list in this case contains only one query, and that identifies the index field ("index_name"), and the start and end terms.  Note that as the projected attributes are appended the sort key, although the query is for an exact sort key it must be range query which covers all possible projected attributes (in this case by appending to the end_term a character "~" that has a value higher than the delimiter "|" in the ascii table).

The evaluation expression is a pipeline of evaluation functions to be applied to each index term.  The first evaluation function `delim($term, :dl1, ($dob, $fn, $gn, $pc))` instructs the query to split the query term using the delimiter identified by the substitution `dl1` (i.e. "|") and then up to four elements will be placed in the projected attributes maps as `$dob`, `$fn`, `$gn` and `$pc` respectively.  The second evaluation function `split($gn, :dl2, $gn)` is to take the value of the attribute `$gn` and create a new attribute `$gn` which is a list obtained by splitting the attribute value on the delimiter identified by the substitution `dl2` (i.e. ".").

So in this term there are two delimiters, one `|` which splits up a fixed number of attributes, and this is evaluated with the `delim` function which outputs elements directly into the map of attributes.  There is then a second delimiter `.` which splits one of those attributes, the given name attribute, into individual given names.  As there is a variable number of given names supported, the `split` function is used to output an attribute whose value is a list.  In this case the output name of the attribute `$gn` is the same as the input, so this alters the value in the attribute map rather than creating a new one.

After applying the evaluation expression, the filter_expression will receive a map of projected attributes like this (for this specific index entry):

```erlang
    #{
        <<"$dob">> => "19650501",
        <<"$fn">> => "SMITH",
        <<"$gn">> => ["ANNE", "MARIE", "ANNE-MARIE"],
        <<"$pc">> => "LS9_0TW"
    }
```

The filter expression does not need to qualify the date of birth, as this is already qualified by the range.  However, it needs to check that the Primary Family Name is as expected `($fn = :qfn)` and that the query given name is in the list of given names produced `(:gqn IN $gn)`.

Note that, in this particular case, there would be a significant performance improvement by rewriting the query as:

```json
    {
        "substitutions" : {"dl1" : "|", "dl2" : ".", "qgn" : "ANNE"},
        "query_list" :
            [
                {
                    "index_name" : "peoplefinder_bin",
                    "start_term" : "19650501|SMITH|",
                    "end_term"   : "19650501|SMITH|~",
                    "evaluation_expression" : "delim($term, :dl1, ($dob, $fn, $gn, $pc)) | split($gn, :dl2, $gn)",
                    "filter_expression" : "(:qgn IN $gn)"
                }
            ]
    }
```

The query could be further optimised as this (although in this case it will also hit a match on a given name that includes the letters ANNE rather than match only on a given name that is entirely ANNE):

```json
    {
        "query_list" :
            [
                {
                    "index_name" : "peoplefinder_bin",
                    "start_term" : "19650501|SMITH|",
                    "end_term"   : "19650501|SMITH|~",
                    "regular_expression" : "[^\\|]*\\|[^\\|]*\\|[^\\|]*ANNE"
                }
            ]
    }
```

Building such optimisations into queries can add significant complications to application code, and extend greatly the complexity of testing that application code. 

### Example (1) - Inexact Match

If for the same query it is required to have an inexact match (e.g. Born between between 1965 and 1970, birthday of 1st May, Family name of SM*, Given name of ANNE), the following query could be used:

```json
    {
        "substitutions" : {"dl1" : "|", "dl2" : ".", "qfn_begins" : "SM", "qgn" : "ANNE", "qbd" : "0501"},
        "query_list" :
            [
                {
                    "index_name" : "peoplefinder_bin",
                    "start_term" : "19650101",
                    "end_term"   : "19691231~",
                    "evaluation_expression" : "delim($term, :dl1, ($dob, $fn, $gn, $pc)) | split($gn, :dl2, $gn) | index($dob, 4, 4, $birthday)",
                    "filter_expression" : "begins_with($fn, :qfn_begins ) AND (:qgn IN $gn) AND ($birthday = :qbd)"
                }
            ]
    }
```

The evaluation expression is extended to output the birthday by taking the last 4 characters of the date of birth.  The filter expression checks an inexact match by looking only at the start of the family name.

Alternative approaches would be possible:

- `ends_with($dob, :qbd)` could be used for the birthday check avoiding the additional pipeline function in the evaluation expression.
- `index($fn, 0, 2, $fn)` could be used in the evaluation expression to slim the $fn to the first two characters for equality checking.
- Also, because of the birthday check, the range start and end terms could also be tighter, and reduce the number of index terms to be processed by about 20%.

### Example (1) - Inexact Match of Given Name

The evaluation expression language supports a number of different comparisons on exact terms, but when a term has been broken into a sub-list (as with the Given Names in the above example), it is only possible to look for an exact match within the sub-list.

There are three possible alternatives should a more complex match be required on such a sub-list:

- Use the alternative `accumulation_option` of `term_with_keys` to the default (which is `keys`), and this will return a list of term/key tuples to filter in the application (rather than just a list of primary keys).  By default the whole term will be returned, but a specific projected attribute and be returned as the term using the `accumulation_term` option as long as the value of that expected attribute is a string.  Filtering in the database is generally quicker than filtering in the application though - due to the increased parallelism of the database filter, and the reduced serialisation and sorting costs. 
- Use an alternative representation and the `contains` evaluation function - e.g. storing given names with a preceding and succeeding delimiter `.ANNE.MARIE.ANNE-MARIE.`, would allow for: `contains($gn, "ANNE")` to find any mention of ANNE in any part of any given name; `contains($gn, ".ANNE.")` to find only where the whole given name is ANNE; `contains($gn, ".ANNE") OR contains($gn, "ANNE.")` to find where the given name either begins or ends with ANNE.
- Use a regular expression filter rather than an evaluation and filter expression.  Regular expression filters are PCRE-style regular expressions which will return a result which matches on the regular expression.  These are generally more performant than applying filter and evaluation expressions.

### Example (1) - Wildcards within terms

Wildcard style queries against individual string attributes are only supported directly using the regular expression filter type.

When using evaluation and filter expressions, the filter expression functions `begins_with`, `ends_with` and `between` are to be used to support internal wildcards within terms.  For example to match on family names of `SM*KOWSKI` where `*` represents one or more characters - a filter expression of `begins_with($fn, "SM") AND ends_with($fn, "KOWSKI") NOT ($fn = "SMKOWSKI)` would be required.

There exists a regex based evaluation function that can be used as a pseudo filter function, where the power of regular expressions is required in a specific part of the query.  The regex evaluation function extracts matches, but only when the expected number of matches is found - so non-matching regular expressions will result in attributes not existing in the projected attribute map.

This query should filter family names based on a "fn_regex" provided in the substitutions. 

```json
    {
        "substitutions" : {"dl1" : "|", "dl2" : ".", "qgn" : "ANNE", "fn_regex" : "(?P<fn_match>SM[A-Z]+KOWSKI)"},
        "query_list" :
            [
                {
                    "index_name" : "peoplefinder_bin",
                    "start_term" : "19650501",
                    "end_term"   : "19650501~",
                    "evaluation_expression" : "delim($term, :dl1, ($dob, $fn, $gn, $pc)) | regex($fn, :fn_regex, ($fn_match)) | split($gn, :dl2, $gn)",
                    "filter_expression" : "attribute_exists($fn_match) AND (:qgn IN $gn)"
                }
            ]
    }
```

### Example (1) - More Extensible Index Schema

It is possible to reduce the pre-defined structure in an index entry by using KV pairs in the index entry.

For example, the above index entry could be stored in a Key=Value form, and note that we here differentiate for extra clarity between the primary given name (`pgn`), and the secondary given names (`sgn`):

`peoplefinder_bin: 19650501|fn=SMITH#pgn=ANNE#sgn=MARIE.ANNE-MARIE#pc=LS9_0TW`

This evaluation expression can then be used: `delim($term, :dl1, ($dob, $kvs)) | kvsplit($kvs, "#", "=") | split($sgn, :dl2, $sgn)`

To produce this set of projected attributes to be passed to the filter expression:

```erlang
    #{
        <<"$dob">> => "19650501",
        <<"$fn">> => "SMITH",
        <<"$pgn">> => "ANNE",
        <<"$sgn">> => ["MARIE", "ANNE-MARIE"],
        <<"$pc">> => "LS9_0TW"
    }
```

## Example (2) - An Alternative People Search

An alternative strategy to option (1), would be to use multiple indexes, with the Date Of Birth as a projected attribute.  In this case we can also introduce the concept of effective dates, where certain attributes (in particular Postal Code) are relevant only to certain timeframes - this then supports a search for people based on both the present information, and also the information at a given date.

In this example there will be three indexes:

- `familyname_bin : <FAMILYNAME>|<DOB>|<EFFECTIVE_STARTDATE><EFFECTIVE_ENDDATE>`
- `givenname_bin : <GIVENNAME>|<DOB>|<EFFECTIVE_STARTDATE><EFFECTIVE_ENDDATE>`
- `postalcode_bin : <POSTCODE>|<DOB>|<EFFECTIVE_STARTDATE><EFFECTIVE_ENDDATE>`

The start date and end dates will be of fixed YYYYMMDD format, with current information being given an artificial end date of `99999999`.

So for a sample person, the index entries could be:

- `familyname_bin: SMITH|19650501|19895060499999999, JONES|19650501|19650501198950604`
- `givenname_bin: ANNE|19650501|1965050199999999, MARIE|19650501|1965050199999999, ANNE-MARIE|19650501|1965050199999999`
- `postcode_bin: LS9_0TW|19650501|1990080199999999, LS9_1GH|19650501|1965050119900801`

This strategy requires more index entries, but potentially simpler and more powerful querying.

```json
    {
        "substitutions" : {"dl1" : "|", "low_dob" : "19650101", "high_dob" : "19650531"},
        "query_list" :
            [
                {
                    "index_name" : "familyname_bin",
                    "start_term" : "SMITH|",
                    "end_term"   : "SMITH~",
                    "evaluation_expression" : "delim($term, :dl1, ($fn, $dob, $edates))",
                    "filter_expression" : "$dob BETWEEN :low_dob AND :high_dob"
                }
            ]
    }
```

The query definition above will search for every SMITH born in the first 6 months of 1964.  Note that the delimiter chosen ("|") is after all the standard text characters in the ASCII table (char 124), so that this will match on only the complete name SMITH, whereas `"start_term" : "SMITH"` would also match on any surname starting SMITH.

```json
    {
        "substitutions" : {"dl1" : "|", "low_dob" : "19650101", "high_dob" : "19650531", "effective_date" : "19800101"},
        "query_list" :
            [
                {
                    "index_name" : "postcode_bin",
                    "start_term" : "LS9_",
                    "end_term"   : "LS9_~",
                    "evaluation_expression" : "delim($term, :dl1, ($pc, $dob, $edates)) | index($edates, 0, 8, $start_date) | index($edates, 8, 8, $end_date)",
                    "filter_expression" : "($dob BETWEEN :low_dob AND :high_dob) AND (:effective_date BETWEEN $start_date AND $end_date)"
                }
            ]
    }
```

The query definition above will search for anyone who was born in the first 6 months of 1964, and was living in the LS9 postal area on 1st January 1980.

If a compound query is required, while this can be managed on a single query with strategy (1) as index terms are pre-concatenated - an aggregation expression is required now to search for only the SMITHs that meet the address criteria.

```json
    {
        "aggregation_expression" : "$1 INTERSECT $2",
        "substitutions" : {"dl1" : "|", "low_dob" : "19650101", "high_dob" : "19650531", "effective_date" : "19800101"},
        "query_list" :
            [
                {
                    "aggregation_tag" : 1,
                    "index_name" : "postcode_bin",
                    "start_term" : "LS9_",
                    "end_term"   : "LS9_~",
                    "evaluation_expression" : "delim($term, :dl1, ($pc, $dob, $edates)) | index($edates, 0, 8, $start_date) | index($edates, 8, 8, $end_date)",
                    "filter_expression" : "($dob BETWEEN :low_dob AND :high_dob) AND (:effective_date BETWEEN $start_date AND $end_date)"
                },
                {
                    "aggregation_tag" : 2,
                    "index_name" : "familyname_bin",
                    "start_term" : "SMITH|",
                    "end_term"   : "SMITH~",
                    "evaluation_expression" : "delim($term, :dl1, ($fn, $dob, $edates))",
                    "filter_expression" : "$dob BETWEEN :low_dob AND :high_dob"
                }
            ]
    }
```

### Example (2) - Simple Variations and Limitations

It is possible to combine strategies (1) and (2) by using separate indices and overloading each term with all additional information.  The application would then need a query planning strategy to determine which index to use based on the information provided - i.e. the strategy would need to determine based on the query details which index would likely lead to the fewest number of index entries being scanned, and use that index and sort key combination.  Designing such a strategy would require up-front knowledge of how the data is distributed.

When using an `aggregation_expression` it is not possible to also use an `accumulation_option` - so terms cannot be returned to the application for additional filtering.

## Example (3) - Reporting index

As well as returning keys, and term/key tuples, when using individual queries it is also possible to return counts, and counts by term to assist in reporting.

For this example we assume all the people exist in a hierarchy.  Each person is assigned to a GP Provider, and every GP Provider belongs to a Strategic Health Authority (and these are represented by fixed-width codes).  People have a Date of Birth (from which we can calculate age), but also a series of characteristics which can be expressed in single character flags (e.g. administrative gender code, smoking status, death status, alcohol dependency etc).  This information is then required to do organisation, and population level reporting.

For this a single index is used:
 - `healthreport_bin : <SHA><GP><DOB><STATUS_FLAGS>`

So are test record may have an entry like:
 - `healthreport_bin: SHA0001GP00000119650501FYNNY`

So if today's date is 30 May 2025, and one wishes to count all the female smokers over the age of 60 registered in SHA001

```json
    {
        "accumulation_option" : "raw_count",
        "query_list" :
            [
                {
                    "index_name" : "healthreport_bin",
                    "start_term" : "SHA0001",
                    "end_term"   : "SHA0001~",
                    "evaluation_expression" : "index($term, 15, 8, $dob) | index($term, 23, 1, $agc) | index($term, 24, 1, $smoker)",
                    "filter_expression" : "($dob <= \"19650530\") AND ($agc = \"F\") AND ($smoker = \"Y\")"
                }
            ]
    }
```

If the same results are required, but this time a count by age at today's date (30th May 2025):

```json
    {
        "substitutions" : {"current_date" : "0530"},
        "accumulation_option" : "term_with_rawcount",
        "accumulation_term" : "$age",
        "query_list" :
            [
                {
                    "index_name" : "healthreport_bin",
                    "start_term" : "SHA0001",
                    "end_term"   : "SHA0001~",
                    "evaluation_expression" : "index($term, 15, 8, $dob) | index($term, 23, 1, $agc) | index($term, 24, 1, $smoker) | index($dob, 0, 4, $yob) | to_integer($yob, $yob) | index($dob, 4, 4, $birthday) | map($birthday, <=, ((:current_date, 2025)), 2024, $yoc) | subtract($yoc, $yob, $age) | to_string($age, $age)",
                    "filter_expression" : "($dob <= \"19650530\") AND ($agc = \"F\") AND ($smoker = \"Y\")"
                }
            ]
    }
```

## Performance Expectation

> TODO - stats on relative performance using the different approaches to querying

> TODO - Note on the lack of optimisation for aggregation expressions 

## Query - Definition

### Query JSON - Definition

The query should be posted as the HTTP body, to the query API for the relevant bucket, where there are the following JSON keys at the root of the document

index_name (required)

- the name of the index to be queried.

start_term (required)

- the starting term range for the query, which all matched terms must be greater than or equal to.

end_term (required)

- the end term of range for the query, which all matched terms must be less than or equal to.

aggregation_expression (optional)

- If multiple queries are to be run, the aggregation expression is used to inform the database how those results should be combined, using $1, $2 etc to refer to the numeric aggregation_tag for each query - with the key words UNION, INTERSECT and SUBTRACT to show how the sets of results are to be combined.  Parenthesis may be used for clarity. e.g. ($1 INTERSECT $2) UNION ($3 SUBTRACT $1)

accumulation_option (optional - default = keys)

- There are six options for accumulating the results from a single query:  keys (return a list of keys), term_with_keys (return a list of term/key tuples), raw_count (return a count of the matches made, without de-duplicating those matches), count (return a count of unique keys matched), term_with_rawcount/term_with_count (return a map of term to either count of matches, or count of unique keys).  If an aggregation_expression is used, only keys, count and raw_count are valid accumulation options.
- raw counts are generally more efficient than key counts, so if it is possible to reason that duplicate keys are not an issue (i.e. there is only one index entry per key), then raw_count should be used in preference to key_count.

accumulation_term (optional - default = $term)

- When using an accumulation option of term_with_keys, term_with_matchcount or term_with_keycount which term in the evaluated index term should be used. The default is $term - the whole term.  However a projected attribute extracted in the evaluation expression may be used instead.  

max_results (optional)

- The potential to limit the number of results returned by the query, to the first N results.  The query will terminate once sufficient results have been returned, and a continuation term will be returned along with the results, which can be passed into a subsequent query to return the next set of results after this point.

continuation (optional)

- A string returned from a previous query constrained by max_results, used to indicate the starting point for the next page of results.

substitutions (optional)

- An array of key/value pairs that match string that are referred to in queries to substitution values that should replace those keys in the query. e.g. {"low_dob" : "19550301", "high_dob" : "19560630"} can be passed as substitutions to populate an evaluation of "$dob" BETWEEN ":low_dob" AND ":high_dob".  The values of substitutions should all be strings.

timeout (optional)

- The timeout in seconds to wait for the query to complete, before a timeout error is returned.

query_list (required)

- A list of one or more queries (should be a list of just one query if an aggregation_expression is not used).
- Each query has the following parts:
  - aggregation_tag (optional unless an aggregation_expression is used)
  - index_name (should be a binary index)
  - start_term
  - end_term
  - regular expression (optional alternative to using evaluation or filter expressions, use to improve efficiency and performance of queries)
  - evaluation_expression (optional, an expression to extract projected attributes from the term)
  - filter_expression (optional, an expression to filter results based on those projected attributes)

### Evaluation Expression - Definition

The evaluation pipeline receives a map of projected attributes containing two Identifier/Value pairs - $term, $key.  The $term is the index term that has been matched (as it is within the sorted key range for the given index field and bucket), and the $key is the value of the Key.  Both $term and $key will be strings.  All pipeline functions will update the map, potentially adding new projected attributes, or adjusting the value for existing attributes - and the map will be forwarded onto the next stage for processing.

Values of the projected attributes in the map always start as strings in the pipeline, but may be explicitly converted to lists of strings, or to an integer - and in the case of an integer can also be converted back to a string.  Functions in the pipeline that receive inputs of the wrong type are skipped.

The functions that can be used in a pipeline are:

delim ( IN_ID identifier , DELIM string , OUT_ID_LIST identifier_list )

- take a value associated with IN_ID and split it using the delimiter DELIM.  The parts are matched to the identifiers in OUT_ID_LIST.  If there are only N values following the application of the delimiter where N is less than the length of the OUT_ID_LIST, then only then only the first N identifiers in OUT_ID_LIST are assigned a value.  Any overhanging elements (i.e. where N is greater than the length of the OUT_ID_LIST) are ignored.

join ( IN_ID_LIST identifier_list , DELIM string , OUT_ID identifier )

- a concatenation function that takes each value associated with an identifier in IN_ID_LIST in turn, and concatenates them together using the DELIM as a separator.  The output is given as the value of OUT_ID.  In effect `join` is the reverse of the `delim` function.

- this is generally used with term-based aggregators in queries (e.g. to create a combined term to count by).

split ( IN_ID identifier , DELIM string , OUT_ID identifier )

- works as with the `delim` function, but the output (a list of strings) is assigned to a single OUT_ID identifier.

slice ( IN_ID identifier , LENGTH pos_integer , OUT_ID identifier )

- slice a string into a list of multiple strings assuming each sub-string is of fixed length e.g. if the value of IN_ID is a string containing 2-character values, slicing with a length 2 will assign a list of 2-character strings to OUT_ID. 

index ( IN_ID identifier , POSITION non_neg_integer , LENGTH pos_integer , OUT_ID identifier )

- take a single slice from a string mapped to the IN_ID identifier, from character position POSITION of length LENGTH and assign the output to OUT_ID.  If there are insufficient characters in the string value of IN_ID to take a string of that position and length, then the function will be skipped.

kvsplit ( IN_ID identifier , PAIR_DELIM string , KV_DELIM string )

- take a string value associated with IN_ID, where the string contains a delimited (by PAIR_DELIM) list of Key/Value pairs - where the Key and Value and separated by KV_DELIM.  The output will add each Key/Value pair to the map of projected attributes.

regex ( IN_ID identifier , REGEX string , OUT_ID_LIST identifier_list )

- use a regular expression to extract new projected attributes as Key/Value pairs, where the REGEX must match the value of IN_ID and extract named capture groups that align with the attribute keys in the OUT_ID_LIST.  All expected captures must exist in the input value for the projected attributes to be updated, otherwise the function will pass on the map of projected attributes unchanged.

map ( IN_ID identifier , COMP comparator , MAP_LIST mappings_list , DEFAULT operand , OUT_ID identifier )

- to classify the value of a projected attribute the map function is used.  The MAP_LIST is a list of pairs, where the first element of the pair is a value to compare with, and the second element is a classification for a match against this  pair (the output value).  The comparison between the value and the first element is done using the comparator COMP.  If no element of the MAP_LIST returns a match against the input value, then the DEFAULT classification is used as the output value.  The output value is added to the projected attributes using the OUT_ID identifier as the key.
- this is generally used with term-based aggregators in queries (e.g. to create a combined term to count by).

to_integer ( IN_ID identifier , OUT_ID identifier )

- convert a string to an integer where IN_ID has a string value, and is mapped after integer-conversion to the projected attribute with an OUT_ID identifier.  The pipeline stage is skipped when the value does not convert to an integer.
- note that if IN_ID and OUT_ID are the same identifier, the type of the value of OUT_ID is dependent on the success of the conversion.

to_string ( IN_ID identifier , OUT_ID identifier )

- convert an integer back to a string where IN_ID has a integer value, and is mapped after string-conversion to the projected attribute with an OUT_ID identifier.  If the input value is already a string, the mapping will still occur without the conversion.

subtract ( X math_operand , Y math_operand , OUT_ID identifier )

- subtract Y from X and map the output to the OUT_ID identifier of the map of projected attributes.  X and Y can either be an integer provided as an input, or an identifier of an existing projected attribute which has been converted to an integer.  If either X or Y are not integers, then the function will be skipped.  

add ( X math_operand , Y math_operand , OUT_ID identifier )

- add X to Y and map the output to the OUT_ID identifier of the map of projected attributes.  X and Y can either be an integer provided as an input, or an identifier of an existing projected attribute which has been converted to an integer.  If either X or Y are not integers, then the function will be skipped.

The final map of projected attributes will be passed as the input to the Filter Expression.

### Filter Expression - Definition

The Filter expression takes the projected attributes as an input, and the output is either `true` (the term is a match) or `false`.

In the definition an `operand` can either be a `key` of a projected attribute (where the value of that attribute will be used when applying the expression), or a fixed value provided within the expression.

```
condition-expression ::=
      operand comparator operand
    | operand BETWEEN operand AND operand
    | operand IN operand 
    | operand IN (',' operand )
    | function
    | condition AND condition
    | condition OR condition
    | NOT condition
    | ( condition )

comparator ::=
    =
    | <>
    | <
    | <=
    | >
    | >=

function ::=
    attribute_exists (key)
    | attribute_not_exists (key)
    | attribute_empty (key)
    | begins_with (key, substr)
    | ends_with (key, substr)
    | contains (key, substr)
```

## Notes on Implementation

### Siblings

Riak supports the `allow_mult = true` state, whereby the history of changes to an object is retained when concurrent updates are made to the same object.  In the sibling state, all index entries on all versions of the object are active from a query perspective.

### Unicode support

Testing is currently only undertaken on ascii-based index terms, although filter and evaluation expressions have been designed to support unicode.  There are a number of potential issues with unicode support, not least with support for unicode in HTTP headers, so end-to-end tested Unicode support is currently deferred to a future release. 

### Performance and Efficiency

Index entries are stored in the leveled ledger (or key store).  The index entries are packed into blocks of 64 entries, and to query a given vnode backend each level of the key store must be checked and compared (to ensure entries at a lower level have not been replaced by those awaiting compaction at a higher level).  The query is distributed across `RingSize div n_val` vnodes in parallel.  So with a ring size of 512, and a `n_val` of 3 there will be 171 parallel queries running across the cluster to complete the query.

Where the number of index entries to be scanned per vnode is bigger than the block size (e.g. > 10K results in total) this can be fast and efficient.  For a smaller number of results per vnode, the query will still be fast, but it is relatively less efficient.

There is an overhead per-vnode to setup the snapshot for the query, including running the query against the in-memory part, and then a cost which is correlated to the number of compressed blocks of index entries that need to be serialised (normally one per level if there are less than 64 entries in the range per vnode).  Reducing the ring size will generally improve the efficiency of secondary index queries, but will not necessarily improve the speed.  However, reducing the ring size does not help the long-term scalability of a cluster.  With 1% of work being complex 2i queries, there can be a 10-20% capacity constraint for every doubling of the ring size.

If applying either an evaluation/filter expression or a regular expression it is normally the expression that dominates the CPU utilisation.  Writing the expression using regex is normally about 20-50% more efficient than using an evaluation and a filter expression (the regular expressions are compiled before being distributed to each vnode).  The cost of this expression is proportional to the number of keys in the sort key range (not the number of keys that are deserialised).

Aggregation of queries is performed at a vnode-level, before results are returned to be aggregated for the client - so even when cross-cluster result sets are large the aggregation operations are sub-divided into relatively small set operations.

### Consistency

Index changes are not deferred to an async process, at a vnode level all index changes are made as a transaction with the object change.  Outside of failure scenarios, secondary index queries will almost always immediately reflect the results of any changes in the object (with caveats related to unreliable latency across intra-cluster networking communication).

In failure and recovery scenarios, false negatives are possible (i.e. results may be missing until anti-entropy mechanisms correct) but results will be eventually consistent.  The query uses a coverage plan which will check only one (of N) potential copies of the data, and so should a vnode be temporarily incorrect, the entropy is not detected as part of the query.  The `participate_in_coverage` configuration option (which can be applied at run-time) is used to mitigate this - this can be used to prevent a node with a known entropy issue from being involved in queries. 

### Notes on Implementation - Further Improvements

The evaluation and filter expression language is a work in progress.  so it is also possible to submit an Issue (or a PR) to request an extension to the functions provided.  Extensions under consideration are:

- An evaluation function that calculate the Jaro-similarity between an attribute value and a given string (Erlang has included jaro_similarity/2 since OTP 27);
- An evaluation function that converts a given string into a soundex representation of that string (currently Riak users have implemented Soundex support simply by adding additional indexes with soundex variations of the required terms).