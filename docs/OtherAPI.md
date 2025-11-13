# Riak KV - The Other APIs

The majority of work within Riak KV can be done using the [Object API](/ObjectAPI.md), and the [Query API](/QueryAPI.md).  There are though additional APIs, with specific purposes:

- [The AAE Fold API](#aae-fold-api)
- [The Fetch API used to access replication queues](#the-fetch-api)
- [The Data Type API](#the-data-type-api)
- [The Map/Reduce API](#the-mapreduce-api)
- [The List API](#the-list-api)

## AAE Fold API

The AAE Fold API requires the configuration of `tictacaae_active = active`, otherwise folds will fail.  When using a single leveled backend, this should use the native keystore within leveled.

When using any other backend or multi-backend this will require an additional parallel key-store, which may have an impact on the achievable PUT throughput, and the memory used by Riak.  The use of a parallel backend also requires periodic key-store rebuilds, to ensure that the key-store correctly represents the content in the backend store.  The parallel store must be configured with `tictacaae_storeheads = enabled` to use the full functionality of AAE Folds.

The AAE Fold API has four potential interfaces:

- [AAE Folds via the Command Line](#aae-folds-via-the-command-line);
- [AAE Folds via remote_console](#aae-folds-via-the-remote-console); 
- [AAE Folds via HTTP](#aae-folds-via-http);
- [AAE Folds via protocol buffers](#aae-folds-via-pb).

### AAE Fold efficiency

Some considerations on the efficiency of AAE Folds:

- Using a restricted key_range is the most reliable method of improving the speed and efficiency of aae folds;
- A modified date range will reduce the volume of data to be processed and returned, but significant gains are only made when setting a "high" low modified date.  Old content below the low modified data can be skipped over without reading, but new content since the high modified date must still be read and deserialised.
- The segment_filter can skip the reading and deserialising of slots (each slot contains 128 keys) by checking in the slot header, with approximately 99.6% of blocks skipped when checking for a single segment.
- The segment_filter can be used for sampling, or approximations.  With the standard tree size, there are a `1024 * 1024` segments, so choosing a random slice of 64 segments in that integer space will give results from `1 / (8 * 1024)`th of the key-space.
  - Use of a contiguous slice is more efficient than selecting random slices, as when checking Segments only the first 15 of the 20 bits (assuming standard tree size) in a segment ID are used.
  - When folds are used with Riak anti-entropy mechanisms, the `max_results` settings are used to control the size of the list of segment IDs passed into a fold.

### Node worker pools

AAE folds use node worker pools.  These pools are defined to constrain concurrency for operational queries, to provide an upper limit on how many CPU cores may be used by different classes of operational work.  There is no guarantee that worker pools will be able to use their limit - use of each core is still managed fairly by the erlang scheduler. The node worker pool can be configured via the `worker_pool_strategy` in riak.conf, and can be set to three different modes:

- none; do not use node worker pools.
  - aae_folds and other operational work will be fairly scheduled by the erlang scheduler equally with other activity.
- single; use a single pool of work for all operational work.
  - This means the whole bandwidth for operational work cna be consumed by any operational work.
  - No segmentation, so one set of jobs may prevent other work from finding available capacity.
- dscp; divides pools up into categories based on the network pooling strategy of differentiated services.
  - There is no Expedited Forwarding queue, this is assumed to be the `vnode_worker_pool`.
  - There are four Assured Forwarding queues: AF1 (cached tree rebuilds, hot backups); AF2 (legacy key-listing); AF3 (AAE folds); AF4 (AAE folds).
  - there is a single Best Endeavours queue, this is used for the rebuild of parallel aae store rebuilds.

When queueing items via a fold (e.g. `repl_keys_range`, `repair_keys_range`, `find_tombs` and `reap_tombs`), if a smaller node worker pool is used, then items will be added to the queue in batches by vnode.  When items are dequeued, this may result in phases of concentrated activity on particular preflists.

#### Dynamic changes to node worker pools

> TODO: Depends on PR

### Supported fold types

All APIs support the following types of fold:

#### merge_root_nval

For a given n_val merge the root of a the merkle tree across all partitions, given a cluster-wide view of the tree root:

- Relatively fast, as uses cached trees;
- Intended for internal use within inter-cluster reconciliation.

#### merge_branch_nval

For a given `n_val` and list of `branch_id`'s (normally deltas discovered after comparing tree roots), merge the branches across all partitions, to give a cluster wide view of those branches within the merkle tree:

- Relatively fast, as uses cached trees;
- intended for internal use within inter_cluster reconciliation.

#### fetch_clocks_nval

For a given set of segment IDs return all the keys and clocks within those segments, potentially constrained by a modified date range.

- If a full-sync manager process detects a false delta, it will temporarily set enable the `aae_fetchclocks_repair` option, and this will cause this query to repair the cached tree for the given segment IDs, as well as collect the results to return.
  - It is possible to force this repair option via configuration or environment variable change.
- Uses the AF3 queue when running node worker pools in DSCP mode.
  - These queries will bypass the pool, running immediately, when repair is required.

#### merge_tree_range

Outputs a full merkle tree representing the overall cluster state for a given bucket.

- Relatively slow compared to `_nval` equivalent queries, as no cached trees can be used, requires a fold over the actual keys to calculate the tree.
- Setting filters is recommended to speed up the query (unless buckets are small).

#### fetch_clocks_range

Equivalent to fetch_clocks_nval but with Bucket and KeyRange constraints.

- Unlike fetch_clocks_nval, this will never result in a repair of cached trees.
- Uses the AF3 queue when running node worker pools in DSCP mode.

#### repl_keys_range

Used to replicate a range of keys to another cluster (or indeed any consumer of a given replication queue).  To be used when seeding new clusters, or if there is a known delta that can be expressed and resolved more quickly by this mechanism rather than by waiting for inter-cluster reconciliation to auto-heal.

- When adding to the replication queue, will be added with a lower priority when compared to real-time replication.
- Each replication queue has a small in-memory part but a large on-disk part.  The size of the on-disk component is controlled in `riak.conf` via `replrtq_overflow_limit`.
- Uses the AF4 queue when running node worker pools in DSCP mode.

#### repair_keys_range

Used to prompt read repair in a bucket, to fix an entropy problem within the cluster, potentially limited by key range or modified date range.

- Uses the `riak_kv_reader` queue, and consumption from that queue is constrained by having a single process per node handling queued repairs.
- The reader queue has a small in-memory part but a large on-disk part.  The `reader_overflow_limit` is not configurable via `riak.conf`.
- Uses the AF4 queue when running node worker pools in DSCP mode.

#### find_keys

Outputs a list of keys in the bucket where the object has either a sibling_count or a size (in bytes) that exceeds a certain threshold, potentially limited by key range or modified date range.

- Commonly used as an operational query (e.g. "find all objects modified in past 24 hours with more than one sibling").
- May also be used to list keys, where using a `$key` query is not supported.  It is much slower (but potentially safer) than `$key` query due to the constraints of the [node_worker_pools](#node-worker-pools).
- Uses the AF4 queue when running node worker pools in DSCP mode.

#### find_tombs

Outputs a list of tombstone keys (deleted keys where the tombstone has not been reaped) in the bucket, potentially limited by key range or modified date range.

- Uses the AF4 queue when running node worker pools in DSCP mode.

#### erase_keys

Prompts for a list of matching keys in the bucket to be erased via the `riak_kv_eraser`, potentially limited by key range or modified date range.

- Uses the `riak_kv_eraser` queue, and consumption from that queue is constrained by having a single process per node handling queued repairs, and by the configuration of the `tombstone_pause` within riak.conf.
- The queue has a small in-memory part but a large on-disk part.  The size of the on-disk component is controlled in `riak.conf` via `eraser_overflow_limit`.
- Uses the AF4 queue when running node worker pools in DSCP mode.

#### reap_tombs

Prompts for a list of matching tombstones in the bucket to be erased via the `riak_kv_reaper`, potentially limited by key range or modified date range.

- Uses the `riak_kv_reaper` queue, and consumption from that queue is constrained by having a single process per node handling queued repairs, and by the configuration of the `tombstone_pause` within riak.conf.
- The queue has a small in-memory part but a large on-disk part.  The size of the on-disk component is controlled in `riak.conf` via `reaper_overflow_limit`.
- Uses the AF4 queue when running node worker pools in DSCP mode.

#### object_stats

Returns a summary of stats for objects within the bucket, potentially limited by key range or modified date range.

- Returns an output like `[{total_count, 1000}, {total_size, 1000000},  {sizes, [{1, 800}, {2, 180}, {3, 20}]},  {siblings, [{1, 1000}]}]`.
  - The sizes are the count of objects by order of magnitude in bytes (e.g. 1 is 10 -> 100 bytes, 2 is 100 -> 1000 bytes etc).
  - The siblings are the count of objects with that count of siblings.
- Uses the AF4 queue when running node worker pools in DSCP mode.

#### list_buckets

Returns a list of buckets, assuming the given n_val.

- The list may be incomplete if the passed n_val is greater than the configured n_val of some buckets.
- will only return buckets that contain objects.
- Uses a skipping cursor in both `native` and the `leveled_ko` type of parallel store, so that the fold is much more efficient than folding over all keys.
- Uses the AF4 queue when running node worker pools in DSCP mode.

### AAE Folds via the Command Line

> TODO - Awaiting PR

### AAE Folds via the Remote Console

The AAE Fold API is accessible via `remote_console`.  Using the remote_console is an operator action, but it can be helpful when writing [Erlang functions](https://www.erlang.org/doc/readme.html) that take action based on AAE Folds.

To run an aae_fold via `remote_console`, a query definition is required and then that definition can be called using:

```erlang
FoldResult = riak_client:aae_fold(QueryDefinition).
```

The different inputs to an aae_fold are described in the specification:

```erlang
-type segment_filter() :: list(integer()).
-type tree_size() :: leveled_tictac:tree_size().
-type branch_filter() :: list(integer()).
-type key_range() :: {riak_object:key(), riak_object:key()}|all.
-type bucket() :: riak_object:bucket().
-type n_val() :: pos_integer().
-type riak_client_modified_range() ::
    {date, calendar:datetime(), calendar:datetime()}.
    %% If using riak_client:aae_fold/1 -
    %% will be auto-converted to modified_range().
-type modified_range() ::
    {date, non_neg_integer(), non_neg_integer()}.
-type hash_method() :: pre_hash|{rehash, non_neg_integer()}.
    %% Use pre_hash unless there is specific concern about hash collision
-type change_method() :: {job, pos_integer()}|local|count.
    %% Should generally just use count or local only
-type query_types() :: 
    merge_root_nval|merge_branch_nval|fetch_clocks_nval|
    merge_tree_range|fetch_clocks_range|repl_keys_range|repair_keys_range|
    find_keys|object_stats|
    find_tombs|reap_tombs|erase_keys|
    list_buckets.

-type query_definition() ::
    {merge_root_nval, n_val()} |
    {merge_branch_nval, n_val(), branch_filter()} |
    {fetch_clocks_nval, n_val(), segment_filter()} |
    {fetch_clocks_nval, n_val(), segment_filter(), modified_range()} |
    {merge_tree_range, bucket(), key_range(), tree_size(), {segments, segment_filter(), tree_size()} | all, modified_range() | all, hash_method()} |
    {fetch_clocks_range, bucket(), key_range(), {segments, segment_filter(), tree_size()} | all, modified_range() | all} |
    {repl_keys_range, bucket(), key_range(), modified_range() | all, riak_kv_replrtq_src:queue_name()} |
    {repair_keys_range, bucket(), key_range(), modified_range() | all, all} |
    {find_keys, bucket(), key_range(), modified_range() | all, {sibling_count, pos_integer()}|{object_size, pos_integer()}} |
    {find_tombs, bucket(), key_range(), {segments, segment_filter(), tree_size()} | all, modified_range() | all} |
    {erase_keys, bucket(), key_range(), {segments, segment_filter(), tree_size()} | all, modified_range() | all, change_method()} |
    {reap_tombs, bucket(), key_range(), {segments, segment_filter(), tree_size()} | all, modified_range() | all, change_method()} |
    {object_stats, bucket(), key_range(), modified_range() | all} |
    {list_buckets, n_val()}.
```

### AAE Folds via HTTP

AAE folds require a URL and potentially a filter.  The filter may be a base64 encoded list of JSON key/value pairs to pass key_range, date_range, segment_filter, hash_iv, or change_method.

| Query Type | URL | Filter |
|:-----------|:-----------|:-----------|
| merge_root_nval | `/cachedtrees/nvals/<NVal>/root`| no filter required |
| merge_branch_nval | `/cachedtrees/nvals/<NVal>/branch` | b64 encode json: list of integers |
| fetch_clocks_nval | `/cachedtrees/nvals/<NVal>/keysclocks` | b64 encode json: segment_filter, date_range |
| merge_tree_range | `/rangetrees/types/<BucketType>/buckets/<Bucket>/trees/<TreeSize>` | b64 encode json: key_range, segment_filter, date_range |
| fetch_clocks_range  | `/rangetrees/types/<BucketType>/buckets/<Bucket>/keysclocks`| b64 encode json: key_range, segment_filter, date_range |
| repl_keys_range | `/rangerepl/types/<BucketType>/buckets/<Bucket>` | b64 encode json: key_range, segment_filter, date_range |
| repair_keys_range | `/rangerepair/types/<BucketType>/buckets/<Bucket>` | b64 encode json: key_range, segment_filter, date_range  |
| find_keys (siblings) | `/siblings/types/<BucketType>/buckets/<Bucket>/counts/<Cnt>` | b64 encode json: key_range, date_range |
| find_keys (by size) | `/objectsizes/types/<BucketType>/buckets/<Bucket>/size/<Size>` | b64 encode json: key_range, date_range  |
| find_tombs  | `/tombs/types/<BucketType>/buckets/<Bucket>` | b64 encode json: key_range, segment_filter, date_range |
| erase_keys | `/erase/types/<BucketType>/buckets/<Bucket>` | b64 encode json: key_range, segment_filter, date_range |
| reap_tombs  | `/reap/types/<BucketType>/buckets/<Bucket>` | b64 encode json: key_range, segment_filter, date_range |
| list_buckets | `/aaebucketlist` | `filter=<NVal>` |

To run a query against an untyped bucket, remove the `types/<BucketType>` slice of the URL.

### AAE Folds via PB

The [PB Object API is described in the riak_pb repository](https://github.com/OpenRiak/riak_pb/blob/e908ddaadc06cb56e248f197dc2dca7d759e53b2/src/riak_kv.proto#L409-L660).

## The Fetch API

The fetch API is currently source-only, and has no documented support for external use.

## The Data Type API

> TODO: Needs to point to legacy docs, and refer to roadmap item for long-term replacement

## The Map/Reduce API

> TODO: Needs to point to legacy docs, and refer to roadmap item for long-term replacement

## The List API

> TODO: formally deprecated, used Query API or AAE Fold
>
> Recommendation to remove permission by default

## Legacy Query API

> TODO: Point to legacy docs, and refer to replacement Query API