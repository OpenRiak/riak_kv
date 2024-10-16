# Riak (NextGen) Replication - Getting started

The Riak NextGen replication is an alternative to the riak_repl replication solution, with these benefits:

- allows for replication between clusters with different ring-sizes and n-vals;
- provides very efficient reconciliation to confirm clusters are synchronised;
- efficient and fast resolution of small deltas between clusters;
- extensive configuration control over the behaviour of replication;
- a comprehensive set of operator tools to troubleshoot and resolve issues via `remote_console`;
- uses an API which is reusable for replication to and reconciliation with non-Riak databases.

The relative negatives of the Riak NextGen replication solution are:

- greater responsibility on the operator to ensure that the configuration supplied across the nodes provides sufficient capacity and resilience;
- slow to resolve very large deltas between clusters, operator intervention to use alternative tools may be required;
- no support for hierarchical replication (i.e. such as the spanning-tree protected replication available in riak_repl);
- relatively little production testing when using parallel mode AAE (i.e. when not exclusively using the leveled backend).

## Concepts - Queues and Workers

The first building block for NextGen replication are replication queues, which exist on the cluster that is a source of replication events.

- Each node must be configured with a separate queue for each cluster or external service which is required to receive replication events.
- Each replication event will be placed on all queues relevant to that event, but only on one node within the cluster.
- Each queue is prioritised so that real-time events are consumed prior to any events related to batch or reconciliation activity.
- Queues that grow beyond a configurable size are persisted to disk to manage the memory overhead of replication.
- The queues are all temporary (even where persisting to disk); replication references will be lost on node failure or on node restart.
- It is expected that real-time replication is always supported by inter-cluster reconciliation (i.e. such as that provide by NextGenRepl full-sync) to cover scenarios where references are lost.

A Sink cluster, one receiving replication events, must have sink workers configured to read from a remote queue on Source clusters.

- Sink workers must be configured to point to at least one node in the Source cluster, but they may be configured to automatically discover other nodes within the cluster from that node.
- A sink worker can only be configured to read from one Source queue (by name) - but that name can exist on multiple nodes and/or clusters.  For a sink node to receive updates from multiple clusters, consistent queue names should be used across the source clusters.
- Each node will have a configurable number of sink workers, which will be distributed across the source peer nodes (either configured or discovered).  The number of workers can be adjusted via the `remote_console` at run-time.
- There is an overhead of a sink making requests on the source, so each sink worker will backoff if a request results in no replication events being discovered.
- The sink worker pool does not auto-expand.  It is an operator responsibility to ensure there are sufficient sink workers to keep-up with real-time replication. There is some protection from over-provisioning but not from under provisioning.
- A sink worker can fetch only one object at a time, and must push that object into the Sink cluster before returning to fetch the next available replication event. 
- A sink-worker will never prompt re-replication.  No hierarchies of replication are supported, each cluster which is to receive replication events must be directly configured as a sink for that source.

Configuring and enabling source queues and sink workers is sufficient to enable real-time replication.  Other replication features (such as full-sync reconciliation) depend on the queues and workers to operate, but require additional configuration.

## Concepts - Replication References

A replication reference is the entity queued to represent an event which has been prompted for replication.  The reference may be the actual object itself, or a reference to it - the queue will automatically switch to using references not objects as the queue expands.  Where a reference is a proxy for a replicated object, when the object is fetched by the sink worker, the fetch API will automatically return the object from the database i.e. this conversion is managed behind the API on the source.

There are three basic ways of creating replication references: real-time, full-sync and folds.

If a node is enabled as a replication source, each PUT that the node coordinates will be sent to the source queue manager to be checked against the source queue configuration, and it will be added to each queue for which there is a configuration match.  The coordinating nodes are not tightly-correlated with the nodes receiving the PUT, and will generally lead to an even distribution of references across the nodes.  Note that PUTs on the write-once PUT path are not coordinated, and so are incompatible with NextGen replication.

If NextGenRepl full-sync is enabled, a full-sync sweep may result in deltas being discovered.  The cluster running the full-sync sweep will be automatically configured to queue up any changes where it discovered itself to be more advanced on its local source queue (to be pulled by the remote sink worker).  It is also possible for full-sync to be bi-directional, so that a full-sync process can prompt a remote peer to queue a replication reference where the remote cluster is in advance of itself.  Full-sync is throttled to repair slowly, so generally only a small number of references will be generated for each full-sync sweep, even when deltas between clusters are large.

Replication references may also be generated by aae folds.  As aae folds can prompt large volumes of references, using coverage queries and node_worker pools - the references may be unevenly distributed around the cluster, and also may be concentrated in batches on certain vnodes within the queue.  When performing large folds, test in live-like environments and consider scheduling outside of peak hours.  There are three aae_fold operations that may generate replication references:
- repl_keys_range; to be used either in transition events (when seeding a cluster) or in recovery events (to re-replicate all changes received during a period where there was a known replication issue).  The repl_keys_range query can replicate a whole bucket, or a range of keys within a bucket optionally constrained by a last-modified date range.
- erase_keys; to be used to erase a range of keys, and if real-time replication is enabled each erase event will also prompt a replication reference to that deletion.
- reap_tombs; to be used to erase a range of tombstones, and if real-time replication of tombstones is enabled (replication must be specifically configured for tombstone reaps) each reap event will also prompt a replication reference to that reap.

It is theoretically possible to prompt NextGen replication events through other mechanisms (pre or post commit hooks, or map/reduce jobs).  However, these methods are not subject to any testing as part of the Riak development and release process. 

## Concepts - Reconciliation with Active Anti-Entropy

A full-sync process can be used to reconcile between two clusters which have configured real-time replication.  The purpose is to quickly determine if the two clusters are in sync, and if they are not in sync identify some keys to be repaired.

Full-sync with NextGen replication is dependent on active anti-entropy, there is no key-listing form of reconciliation or synchronisation.  Riak has two anti-entropy mechanisms, and older version of AAE known as hashtree, and an updated one known as tictacaae.  Both methods use merkle trees to represent the state of a partition (a subset of a vnode), where the merkle tree has a million leaves (or segments) that each hold a single hash that represents the accumulated hashes of all the keys and hashes in that partition where the key hashes to that segment ID (i.e. that one millionth of the key-space).

The tictacaae service differs in two key ways:
- the merkle trees it holds are mergeable, multiple trees representing multiple partitions can be quickly merged to represent the combined tree of those partitions.
- the hashtree aae service always requires a segment ordered keystore to be kept in an eleveldb database; whereas the tictacaae service can reuse as a keystore the ledger of a native leveled based backend (or if the backend is not leveled, use a parallel leveled-based key-store that can be either segment-ordered or key-ordered).

The NextGen repl full-sync solution depends on tictacaae being enabled.  The tictacaae does not to be used exclusively, it can exist in parallel to hashtree-based AAE during transition scenarios.

A more efficient and flexible full-sync reconciliation service is possible with tictacaae, as a tree to represent the whole store can be made quickly by merging a covering set of partition trees.  The cached trees are split into three levels - root, branches and leaves.  To confirm synchronisation it is only necessary to compare the roots of the merged trees match (which is just 16KB of data to represent the entire cluster).  

The basic mechanism for performing a full-sync reconciliation is as follows:
- Compare the roots by merging the cached tree roots on both clusters.
- If there are no deltas, the sync status is true; otherwise compare the roots again.
- If any of the 4K root hashes differ in both comparisons - this represents a delta.
- Compare the branches for the differing root elements, and then the leaves until a list of differing segment IDs are found.
- Limit the process so that it only ever discovers `max_results` different segment IDs.
- Run a coverage query across both clusters, using the key-stores (which in native mode will be the leveled backend), asking for the keys & clocks which match the mismatched segment IDs.
- Compare the keys and clocks, and prompt the cluster with the more advanced clock for a mismatch to re-replicate its object.

The disadvantage of tictacaae over hashtree, is that when using a key-ordered backend ot the AAE system (such as a native leveled vnode backend), the query to fetch the keys & clocks by segment has to run a full scan over all the object keys.  This disadvantage is mitigated within leveled as it embeds within its own hash-based filters hints about the presence of segment hashes within blocks of keys.  Further the use of a native backend reduces the write and rebuild activity necessary to maintain a parallel keystore.

## Concepts - Delete Mode

There are three possible delete modes for Riak

- Timeout (default 3s);
- Keep;
- Immediate.

When running replication, it is strongly recommended to change from the default setting, and use to the delete mode of `keep`.  This needs to be added via a `riak_kv` section of the advanced.config file (there is no way of setting the delete mode via riak.conf).  Running `keep` will retain semi-permanent tombstones after deletion, that are important to avoid issues of object resurrection when running bi-directional replication between clusters.

When running Tictac AAE, the tombstones can now be reaped using the `reap_tomb` aae_fold query.  This allows for tombstones to be reaped after a long delay (e.g. 1 week).  

Running an alternative delete mode, is tested, and will work, but there will be a significantly increased probability of false-negative reconciliation events, that may consume resource on the cluster.

## Getting started

For this getting started, it is assumed that the setup involves:

- 2 x 8-node clusters (A & B) where all data is nval=3, and where application writes may be received by either cluster;

- 1 x 2-node cluster (a backup cluster - C) where all data is nval=1, which does not receive real-time write activity;

- A requirement to both real-time replicate and full-sync reconcile between clusters;

- Each cluster has o(1bn) keys;

- bi-directional replication required between active (non-backup) clusters.

### Configure real-time replication

The first stage is to enable real-time replication.  Each of the active clusters will need to be configured with a source queue for both the peer active cluster and the backup cluster

For a node on cluster_a, the following configuration is required.

```
replrtq_enablesrc = enabled
replrtq_srcqueue = cluster_b:any|cluster_c:any
```

This configuration enables the node to be a source for changes to replicate, and replicates `any` change to queues named `cluster_b` and `cluster_c` - and this queue will need to be configured as the source for updates on the sink cluster.  The configuration is required on each and every node in the source cluster.  Once a node has been enabled as a source, all PUTs that node coordinated will be passed to the queue process to be assessed against the replrtq_srcqueue configuration, and potentially be queued.

For more complicated configurations further queue names can be used, with different filters - filters can be on bucket, bucket-type or bucket prefix.

For the sink cluster (cluster_b), the following configuration is required on node 1:

```
replrtq_enablesink = enabled
replrtq_sinkqueue = cluster_b
replrtq_sinkpeers = <ip_addr_node1_clustera>:8087:pb
replrtq_sinkworkers = 16
replrtq_peer_discovery = enabled
```

This informs this sink node to connect to Node 1 in cluster A, and discover from that node all the nodes in the cluster from this Node, then spread its sink workers across those discovered nodes.  Periodically the node will reconnect to node 1 and re-discover peers - if a node has joined or left the cluster, the peer list will update and the sink workers will be re-distributed across the new list of nodes.

If Node 1 is down the old list will continue to be used.  If node 1 is down at startup, no peers will be discovered.  So ideally, for resilience, different nodes in the sink cluster should be peered (for discovery) with different nodes in the source cluster.  Multiple peers can be configured within the `replrtq_sinkpeers` to have resilience for discovery.

For the backup cluster (cluster_c), this will need to be a sink for both cluster A and cluster B, and so the following configuration is required on node 1:

```
replrtq_enablesink = enabled
replrtq_sinkqueue = cluster_c
replrtq_sinkpeers = <ip_addr_node1_clustera>:8087:pb|<ip_addr_node1_clusterb>:8087:pb
replrtq_sinkworkers = 16
replrtq_sinkpeerlimit = 4
replrtq_peer_discovery = enabled
```

If peer_discovery is not used (i.e. `replrtq_peer_discovery = disabled`), then it is an operator responsibility to ensure that every node in the source cluster has an active peer in the sink cluster, but also to adjust those configuration when nodes join or leave.  A node in a source cluster without an active worker consuming from it will not offload its queue - the replication events will simply backup until a peer relationship from the sink is formed.  Sink workers can tolerate communication with dead peers in the source cluster, so sink configuration should be added in before expanding source clusters.

The number of `replrtq_sinkworkers` needs to be tuned for each installation.  Higher throughput nodes may require more sink workers to be added.  If insufficient sink workers are added queues will build up.  The `replrtq_sinkpeerlimit` will determine the maximum number of workers that will be pointed at an individual source node.

The size of replication queue is logged as follows:

`@riak_kv_replrtq_src:handle_info:414 QueueName=replq has queue sizes p1=0 p2=0 p3=0`

More workers and sink peers can be added at run-time via the `remote_console`.  The command `riak_client:replrtq_reset_all_workercounts(WorkerCount, PerPeerLimit)` can be used to achieve this reset.  To force peer discovery to immediately update the list of peers `riak_client:replrtq_reset_all_peers(QueueName)` can be used on a sink node.  These changes are node-specific, and so will not automatically change other nodes within the same sink cluster. 

There is a log on each sink node of the replication timings:

`Queue=~w success_count=~w error_count=~w mean_fetchtime_ms=~s mean_pushtime_ms=~s mean_repltime_ms=~s lmdin_s=~w lmdin_m=~w lmdin_h=~w lmdin_d=~w lmd_over=~w`

The mean_repltime is a measure of the delta between the last-modfied-date on the replicated object and the time the replication was completed - so this may vary if prompting replication via aae_fold or reconciliation.  The `lmdin_<x>` counts are the counts of replicated objects which were replicated within a second, minute, hour, day or over a day.

### Configure Full-Sync Reconciliation and Replication (All)

To use the full-sync mechanisms, and the operational tools then TictacAAE must be enabled:

```
tictacaae_active = active
```

This can be enabled, and the cluster run in 'parallel' mode - where backend other than leveled is used.  However, for optimal replication performance Tictac AAE is best run in `native` mode with a leveled backend.  When enabling tictacaae for the first time, it will not be usable by full-sync until all trees have been built.  Trees will periodically rebuild, and full-sync should continue to operate as expected during rebuilds.

Full-sync replication requires the existence of source queue definitions and sink worker configurations.  The same configurations can be used as for real-time replication.  If there is a need to have only full-sync replication without allowing for real-time replication - then the `block_rtq` keyword can be used instead of `any` on the source queue definition.

To enable full-sync replication on a cluster, for all the data in the cluster, the following configuration is required on a cluster_a node to full-sync with cluster_b:

```
ttaaefs_scope = all
ttaaefs_queuename = cluster_b
ttaaefs_queuename_peer = cluster_a
ttaaefs_localnval = 3
ttaaefs_remotenval = 3

ttaaefs_cluster_slice = 1
```

A node can only be configured to full-sync with one other cluster, so if there is a need to full-sync with multiple clusters different nodes must be use different configurations to point at those different clusters.  So for the backup cluster, cluster C node 1 would use:

```
ttaaefs_scope = all
ttaaefs_queuename = cluster_a
ttaaefs_queuename_peer = cluster_c
ttaaefs_localnval = 1
ttaaefs_remotenval = 3

ttaaefs_cluster_slice = 3
```

Node 2 would use the alternative configuration to peer with Cluster B:

```
ttaaefs_scope = all
ttaaefs_queuename = cluster_b
ttaaefs_queuename_peer = cluster_c
ttaaefs_localnval = 1
ttaaefs_remotenval = 3

ttaaefs_cluster_slice = 3
```

Where clusters have different n_vals, these must be configured correctly in the full-sync relationships.  If two clusters (A and B) both have data with different n_vals (i.e. some buckets are n_val 3, and some buckets are n_val 4) - then different nodes will need different configurations, one set of nodes to perform the n_val 3 full-sync operations, and another set of nodes to perform the n_val 4 configurations.

It is possible to have scopes for full-sync that are limited by bucket or bucket-type.  However, when using such limited scopes, the full-sync process cannot use cached aae tress - and so full-sync comparisons, particularly when confirming no deltas exist, may have significantly higher costs than when using the `all` scope.  Where possible, it is simpler and more efficient to use the `all` scope.  

Then to configure a peer relationship:

```
ttaaefs_peerip = <ip_addr_node1>
ttaaefs_peerport = 8087
ttaaefs_peerprotocol = pb
```

Unlike when configuring a real-time replication sink, each node can only have a single peer relationship with another node in the remote cluster.  Note though, that all full-sync commands run across the whole cluster.  If a single peer relationship dies, some full-sync capacity is lost, but other peer relationships between different nodes will still cover the whole data set.  It is only necessary to have one working peer relationship to confirm clusters are in-sync.  If there are multiple active peer relationships between two clusters, some simple offset-based scheduling is done to space out the full-sync requests - but this is no single coordinating scheduler for full-sync within the cluster.

Once there are peer relationships, a schedule is required, and a capacity must be defined.

```
ttaaefs_allcheck = 0
ttaaefs_hourcheck = 0
ttaaefs_daycheck = 0
ttaaefs_autocheck = 24
ttaaefs_rangecheck = 0

ttaaefs_maxresults = 32
ttaaefs_rangeboost = 8

ttaaefs_allcheck.policy = always
```

There are two stages to the key comparison - the tree comparison, and the key comparison.  When using `ttaaefs_scope = all` the tree comparison is always for the whole keyspace.  The `ttaaefs_*check` will determine the scope of the subsequent key comparison in terms of the modified date range.

The schedule of `ttaaefs_<sync_type>check`s is how many times each 24 hour period to run a check of the defined type - on this node for its peer relationship.  The schedule is re-shuffled at random each day, and simple offsets are used to space out requests between nodes.  It is recommended that only `ttaaefs_autocheck` be used in schedules by default, `ttaaefs_autocheck` is an adaptive check designed to be efficient in a variety of scenarios.  The other checks should only be used if there is specific test evidence to demonstrate that they are more efficient.

When using `ttaaefs_autocheck` with a scope of `all` every comparison is between the whole key-space using the cached aae trees at the first stage.  This should be fast (< 10s).  When running this between healthy clusters this should result in `{root_compare, 0}` as a result (and occasionally `{branch_compare, 0}` when there are short-lived deltas). If a delta between the trees is confirmed, and the last check was successful, the check will attempt to only compare keys and values (as represented by vector clocks) that were last modified since the previous check - this is much quicker than comparing all keys.  Normally it would be expected that the issue discovered in this case is between recently modified keys.  The tree comparison will discover the tree segments where there is a delta (up to max_results segments) but then only the recently modified keys in those segments are compared.  This will repair the delta slowly, but efficiently.

There may be circumstances when non-recent deltas have been uncovered.  It may be that historic data appears to have been lost (perhaps due to resurrection of old data on a remote cluster), or a disk corruption has just been detected following a tree rebuild.  In these cases the exchange will result in `{clock_compare, 0}` - a tree delta was discovered, but not key deltas given the range limit.  This node will then be set to check all keys on its next run.  When it next checks all keys, it will use the high/low modified date range discovered in future checks.  Running key comparisons across all buckets and over all time is expensive, even when restricting the segments using max_results.  So the `autocheck` full-sync process will always look to learn clues about where (in terms of bucket, or modified range) the delta exists to make this more efficient.

It is possible to restrict the escalation to chekcing all keys, so that it will only occur if the time is in an off-peak window - outside of the window, the escalation will simply be to a `ttaaefs_daycheck`.  Use of `ttaaefs_allcheck.policy = window` is discouraged, as the results are potentially confusing to any operator wihtout detailed knowledge of how full-sync works.  The window option is likely to be removed in a future release.

The `all`, `day` and `hour` check's restrict the modified date range used in the full-sync comparison to all time, the past day or the past hour.  the `ttaaefs_rangecheck` uses information gained from previous queries to dynamically determine in which modified time range a problem may have occurred (and when the previous check was successful it assumes any delta must have occurred since that previous check).

It is normally preferable to under-configure the schedule.  When over-configuring the schedule, i.e. setting too much repair work than capacity of the cluster allows, there are protections to queue those schedule items there is no capacity to serve, and proactively cancel items once the manager falls behind in the schedule.  However, those cancellations will reset range_checks and so may delay the overall time to recover.

Each check is constrained by `ttaaefs_maxresults`, so that it only tries to resolve issues in a subset of broken leaves in the tree of that scale (there are o(1M) leaves to the tree overall).  However, the range checks will try and resolve more (as they are constrained by the range) - this will be the multiple of `ttaaefs_maxresults` and `ttaaefs_rangeboost`.

It is possible to enhance the speed of recovery when there is capacity by manually requesting additional checks, or by temporarily overriding `ttaaefs_maxresults` and/or `ttaaefs_rangeboost`.  This can be done from remote_console using `application:set_env(riak_kv, ttaaefs_maxresults, 64)` for example, or `application:set_env(riak_kv, ttaaefs_rangeboost, 4)`.

In a cluster with 1bn keys, under a steady load including 2K PUTs per second, relative timings to complete different sync checks (assuming there exists a delta):

- all_check 150s - 200s;

- day_check 20s - 30s;

- hour_check 2s - 5s;

- range_check (depends on how recent the low point in the modified range is).

Timings will vary depending on the total number of keys in the cluster, the rate of changes, the size of the delta and the precise hardware used.  Full-sync repairs tend to be relatively demanding of CPU (rather than disk I/O), so available CPU capacity is important.

The `ttaaefs_queuename` is the name of the queue on this node, to which deltas should be written (assuming the remote cluster being compared has sink workers fetching from this queue).  If the `ttaaefs_queuename_peer` is set to disabled, when repairs are discovered, but it is the peer node that has the superior value, then these repairs are ignored.  It is expected these repairs will be picked up instead by discovery initiated from the peer.  Setting the `ttaaefs_queuename_peer` to the name of a queue on the peer which this node has a sink worker enabled to fetch from will actually trigger repairs when the peer cluster is superior.  It is strongly recommended to make full-sync repair bi-directionally in this way.

If there are 24 sync events scheduled a day, and default `ttaaefs_maxresults` and `ttaaefs_rangeboost` settings are used, and an 8-node cluster is in use - repairs via ttaaefs full-sync will happen at a rate of about 100K per day.  It is therefore expected that where a large delta emerges it may be necessary to schedule a `range_repl` fold, or intervene to raise the `ttaaefs_rangeboost` to speed up the closing of the delta.

To help space out queries between clusters - i.e. stop two clusters with identical schedules from mutual full-syncs at the same time - each cluster may be configured with `ttaaefs_cluster_slice` number between 1 and 4.  Give each cluster a unique number, and use that same slice number on every node.

### Configure Full-Sync Reconciliation and Replication (Per-Bucket)

The `ttaaefs_scope` can be set to a specific bucket.  The non-functional characteristics of the solution change when using per-bucket full-sync.  There is no per-bucket caching of AAE trees, so the AAE trees will need to be re-calculated by scanning the whole bucket for every full-sync check (subject to other restrictions on check type).  So the cost of checking full-sync for an individual bucket in happy-day scenarios is considerbaly higher than using a scope of `all`.  When deltas are discovered in trees, the scanning required to compare keys and clocks will be limited to the bucket, and so this may be faster.

The rules of the `ttaaefs_<sync_type>check` configuration are followed with per-bucket synchronisation.  So using `ttaaefs_autocheck` when a previous check succeeded will scan only recently modified items to build the tree for comparison.  This does mean that non-recently modified variations within the bucket (such as resurrected objects or tombstones) will not be detected by `ttaaefs_autocheck` as when `ttaaefs_scope = all`.  When using per-bucket full-sync, it may be wise to occassionally schedule a `ttaaefs_allcheck` to cover this scenario.

Note that a scheduled run of an `ttaaefs_allcheck` will occur regardless of whether the current time is within or outside of the allcheck.window.  The window is related only to the running of `ttaaefs_autocheck` and it limits the `ttaaefs_autocheck` so that it can only being uplifted to a `ttaaefs_allcheck` within the window, outside of the window it will only be ` ttaaefs_daycheck`.

When using per-bucket full-sync, and performing a rolling upgrade ro Riak 3.2.3 or 3.4.0, there may be errors merging buckets.  To prevent these errors during the rolling upgrade, then either disable full-sync for the period of the upgrade, or use the configuration option to force the new nodes to use legacy format trees:

```
legacyformat_tictacaae_tree = enabled
```

There are significant memory improvements related ot the new tree format, so the configuration should be reversed after the rolling upgrade has completed.  There are no inter-cluster issues with tree versions, it is only an issue when merging trees within a cluster.

### Replication API

All real-time and full-sync operations are available via the Riak API, and supported by the Riak erlang clients (both PB and HTTP).  They have been used in different systems for replicating and synchronising with third party databases, such as OpenSearch or DynamoDB.

It is recommended, where possible to use the PB API for performance reasons, and also as TLS security can be enabled via this API.  The HTTP API is slower, but maybe useful where it is easier to set up peer relationships with a HTTP-based load-balancer rather than an individual node.

The API is not documented outside of the code base.

### Monitoring and Run-time Changes

#### Monitoring full-sync exchanges via logs

Full-sync exchanges will go through the following states:

`root_compare` - a comparison of the roots of the two cluster AAE trees.  This comparison will be repeated until a consistent number of repeated differences is seen.  If there are no repeated differences, we consider the clusters to be in_sync - otherwise run `branch_compare`.

`branch_compare` - repeats the root comparison, but now looking at the leaves of those branches for which there were deltas in the `root_compare`.  If there are no repeated differences, we consider the clusters to be in_sync - otherwise run `clock_compare`.

`clock_compare` - will run a fetch_clocks_nval or fetch_clocks_range query depending on the scheduled work item.  This will be constrained by the maximum number of broken segments to be fixed per run (`ttaaefs_maxresults`).  This list of keys and clocks from each cluster will be compared, to look for keys and clock where the source of the request has a more advanced clock - and these keys will be sent for read repair.

At clock_compare stage, a log will be generated for each bucket where repairs were required, with the low and high modification dates associated with the repairs:

```
riak_kv_ttaaefs_manager:report_repairs:1071 AAE exchange=122471781 work_item=all_check type=full repaired key_count=18 for bucket=<<"domainDocument_T9P3">> with low date {{2020,11,30},{21,17,40}} high date {{2020,11,30},{21,19,42}}
riak_kv_ttaaefs_manager:report_repairs:1071 AAE exchange=122471781 work_item=all_check type=full repaired key_count=2 for bucket=<<"domainDocument_T9P9">> with low date {{2020,11,30},{22,11,39}} high date {{2020,11,30},{22,15,11}}
```

If there is a need to investigate further what keys are the cause of the mismatch, all repairing keys can be logged by setting via `remote_console`:

```
application:set_env(riak_kv, ttaaefs_logrepairs, true).
```

This will produce logs for each individual key:

```
@riak_kv_ttaaefs_manager:generate_repairfun:973 Repair B=<<"domainDocument_T9P3">> K=<<"000154901001742561">> SrcVC=[{<<170,167,80,233,12,35,181,35,0,49,73,147>>,{1,63773035994}},{<<170,167,80,233,12,35,181,35,0,97,246,69>>,{1,63773990260}}] SnkVC=[{<<170,167,80,233,12,35,181,35,0,49,73,147>>,{1,63773035994}}]

@riak_kv_ttaaefs_manager:generate_repairfun:973 Repair B=<<"domainDocument_T9P3">> K=<<"000154850002055021">> SrcVC=[{<<170,167,80,233,12,35,181,35,0,49,67,85>>,{1,63773035957}},{<<170,167,80,233,12,35,181,35,0,97,246,68>>,{1,63773990260}}] SnkVC=[{<<170,167,80,233,12,35,181,35,0,49,67,85>>,{1,63773035957}}]

@riak_kv_ttaaefs_manager:generate_repairfun:973 Repair B=<<"domainDocument_T9P3">> K=<<"000154817001656137">> SrcVC=[{<<170,167,80,233,12,35,181,35,0,49,71,90>>,{1,63773035982}},{<<170,167,80,233,12,35,181,35,0,97,246,112>>,{1,63773990382}}] SnkVC=[{<<170,167,80,233,12,35,181,35,0,49,71,90>>,{1,63773035982}}]

@riak_kv_ttaaefs_manager:generate_repairfun:973 Repair B=<<"domainDocument_T9P3">> K=<<"000154801000955371">> SrcVC=[{<<170,167,80,233,12,35,181,35,0,49,70,176>>,{1,63773035978}},{<<170,167,80,233,12,35,181,35,0,97,246,70>>,{1,63773990260}}] SnkVC=[{<<170,167,80,233,12,35,181,35,0,49,70,176>>,{1,63773035978}}]
```

At the end of each stage a log EX003 is produced which explains the outcome of the exchange:

```
log_level=info log_ref=EX003 pid=<0.30710.6> Normal exit for full exchange purpose=day_check in_sync=true  pending_state=root_compare for exchange id=8c11ffa2-13a6-4aca-9c94-0a81c38b4b7a scope of mismatched_segments=0 root_compare_loops=2  branch_compare_loops=0  keys_passed_for_repair=0

log_level=info log_ref=EX003 pid=<0.13013.1264> Normal exit for full exchange purpose=range_check in_sync=false  pending_state=clock_compare for exchange id=921764ea-01ba-4bef-bf5d-5712f4d81ae4 scope of mismatched_segments=1 root_compare_loops=3  branch_compare_loops=2  keys_passed_for_repair=15
```

The mismatched_segments is an estimate of the scope of damage to the tree.  Even if clock_compare shows no deltas, clusters are not considered in_sync until deltas are not shown with tree comparisons (e.g. root_compare or branch_compare return 0).

#### Prompting a check

Individual full-syncs between clusters can be triggered outside the standard schedule:

```
riak_client:ttaaefs_fullsync(all_check).
```

The `all_check` can be replaced with `hour_check`, `day_check` or `range_check` as required.  The request will uses the standard max_results and range_boost for the node.

#### Configure and Monitor work queues

There are two per-node worker pools which have particular relevance to full-sync:

```
af1_worker_pool_size = 2
af3_worker_pool_size = 4
```

The AF1 pool is used by rebuilds of the AAE tree cache.

If the full-sync processes are taking too long (perhaps as max_results or range_boost are set too aggressively) then the worker pools may backup.  At some stage there may develop a situation where all full-sync queries will time out as the queries will take too long to reach the front of the queue, and hence all the effort associated with the queries will be wasted.

By default there is a log prompted for every aae_fold on completion (all full-sync activity depends on aae_folds prompted on both the source and sink).  Each worker pool will regularly log its current queue length and last checkout time (when it last picked up a new piece of work).  There are also riak stats for each pool, giving the average queue time (how long work is waiting in the queue), and work time (how long eahc piece of work takes).

Significant improvements have been made since 2023 on the performance of full-sync folds, and there are more improvements in the pipeline (as at 2024), so running the most up-to-date version of Riak is helpful.

#### Update the request limits

If there is sufficient capacity to resolve a delta between clusters, but the current schedule is taking too long to resolve - the max_results and range_boost settings on a given node can be overridden.

```
application:set_env(riak_kv, ttaaefs_maxresults, 64).
application:set_env(riak_kv, ttaaefs_rangeboost, 16).
```

Individual repair queries will do more work as these numbers are increased, but will repair more keys per cycle.  This can be used along with prompted checks (especially range checks) to rapidly resolve a delta.

The fetching of keys and clocks will require a scan across the key-store, which is divided into blocks of roughly 24 keys, where every block has a potentially-cached array of 15-bit hashes, one hash for each key in the block.  The hashes used in this array is 15 sub-bits of the segment ID for the key, so if none of the hashes match any of the segment IDs the block does not need to be read from disk - and reading a block has a relatively significant CPU cost (to decompress and deserialise the block).  Doubling the max results, will double the number of blocks that need to be read and deserialised, and double the number of keys and clocks to be compared, but other factors in the scan (such as the number of hash comaprisons) will remain roughly constant.  The actual impact of tuning this value is context-specific.

It should be noted, that if the number of segment IDs being checked goes significantly over 1000, then the number of blocks that can be skipped will start to tend towards zero.  So the combined value of maxresults * rangeboost should generlaly be kept to a value less than or equal to 1024.

#### Overriding the range

When a query successfully repairs a significant number of keys, it will set the range property to guide any future range queries on that node.  This range can be temporarily overridden, if, for example there exists more specific knowledge of what the range should be.  It may also be necessary to override the range when an even erroneously wipes the range (e.g. falling behind in the schedule will remove the range to force range_checks to throttle back their activity).

To override the range (for the duration of one request):

```
riak_kv_ttaaefs_manager:set_range({Bucket, KeyRange, ModifiedRange}).
```

Bucket can be a specific bucket (e.g. `{<<"Type">>, <<"Bucket">>}` or `<<"Bucket">>`) or the keyword `all` to check all buckets (if nval full-sync is configured for this node). The KeyRange may also be `all` or a tuple of StartKey and EndKey.

To remove the range:

```
riak_kv_ttaaefs_manager:clear_range().
```

Remember the `range_check` queries will only run if either: the last check on the node found the clusters in sync, or; a range has been defined.  Clearing the range may prevent future range_check queries from running until another check re-assigns a range.

#### Re-replicating keys for a given time period

The aae_fold `repl_keys_range` will replicate any key within the defined range to the clusters consuming from a defined queue.  For exampled, to replicate all keys in the bucket `<<"domainRecord">>` that were last modified between 3am and 4am on a 2020/09/01, to the queue `cluster_b`:

```
riak_client:aae_fold({repl_keys_range, <<"domainRecord">>, all, {date, {{2020, 9, 1}, {3, 0, 0}}, {{2020, 9, 1}, {4, 0, 0}}}, cluster_b}).
```

The fold will discover the keys in the defined range, and add them to the replication queue - but with a lower priority than freshly modified items, so the sink-side consumers will only consume these re-replicated items when they have excess capacity over and above that required to keep-up with current replication.

The `all` in the above query may be replaced with a range of keys if that can be specified.  Also if there is a specific modified range, but multiple buckets to be re-replicated the bucket reference can be replaced with `all`.

#### Reaping tombstones


With the recommended delete_mode of `keep`, tombstones will be left permanently in the cluster following deletion.  It may be deemed, that a number of days after a set of objects have been deleted, that it is safe to reap tombstones.

Reaping can be achieved through an aae_fold.  However, reaping is local to a cluster.  If full-sync is enabled when reaping from one cluster then a full-sync operation will discover a delta (between the existence of a tombstone, and the non-existence of an object) and then work to repair that delta (by re-replicating and hence resurrecting the tombstone).

When reaping tombstones, there are two approaches within clusters kept using full-sync - with operator coordination or with replication.  

If using operator coordination, The reap folds can be issued in parallel on the two clusters, but will full-sync temporarily disabled.  The same fold will reap in different order on different clusters, so if full-sync is enabled, full-sync will (slowly) resurrect tombstones that have already been reaped - so it may be necessary then to re-reap for the same range in a subsequent operation.

There is a configuration option to automatically replicate reap requests generated via aae_folds - in riak.conf set `repl_reap = enabled`.  This will allow you to reap from one cluster, the reap aae_fold will populate a reap queue, and as each reap item is taken from the queue and acted on it will be replicated via source queue and sink workers to any clusters configured for real-time replication.  This will keep clusters roughly in-sync during the reap.  `repl_reap` is disabled by default, but is now the recommended way of managing reaps in multi-cluster environments.

It is possible for reap (and erase) folds to overwhelm the cluster, as they can generate reap requests much faster than they can be completed.  Reaps and erases are both throttled via the riak.conf configuration of `tombstone_pause = 10` - where the configured value is the number of milliseconds to pause for each reap/erase event.  This can be adjusted at run-time from the remote_console using `application:set_env(riak_kv, tombstone_pause, 10)`.  In some cases sink clusters may need a lower value than source clusters to keep up-to-pace with the reap or erase events.

To issue a reap fold, for all the tombstones in August, a query like this may be made at the `remote_console`:

```
riak_client:aae_fold({reap_tombs, all, all, all, {date, {{2020, 8, 1}, {0, 0, 0}}, {{2020, 9, 1}, {0, 0, 0}}}, local}).
```

Issue the same query with the method `count` in place of `local` if you first wish to count the number of tombstones before reaping them in a separate fold.

#### Erasing keys and buckets

It is possible to issue an aae_fold from `remote_console` to erase all the keys in a bucket, or a given key range or last-modified-date range within that bucket.  This is a replicated operation, so as each key is deleted, the delete action will be replicated to any real-time sync'd clusters.

Erasing keys will not happen immediately, the fold will queue the keys at the `riak_kv_eraser` for deletion and they will be deleted one-by-one as a background process.

This is not a reversible operation, once the deletion has been de-queued and acted upon.  The backlog of queue deletes can be cancelled at the `remote_console` on each node in turn using:

```
riak_kv_eraser:clear_queue(riak_kv_eraser).
```

See the `riak_kv_cluseraae_fsm` module for further details on how to form an aae_fold that erases keys.

#### Gathering per-bucket object stats

It is useful when considering the tuning of full-sync and replication, to understand the size and scope of data within the cluster.  On a per-bucket basis, object_stats can be discovered via aae_fold from the remote_console.

```
riak_client:aae_fold({object_stats, <<"domainRecord">>, all, all}).
```

The above fold will return:

- a count of objects in the buckets;

- the total size of all the objects combined;

- a histogram of count by number of siblings sibling;

- a histogram of count by the order of magnitude of object size.

The fold is run as a "best efforts" query on a constrained queue, so may take some time to complete.  A last-modified-date range may be used to get results for objects modified since a recent check.


```
riak_client:aae_fold({object_stats, <<"domainRecord">>, all, {date, {{2020, 8, 1}, {0, 0, 0}}, {{2020, 9, 1}, {0, 0, 0}}}}).
```

To find all the buckets with objects in the cluster for a given n_val (e.g. n_val = 3):

```
riak_client:aae_fold({list_buckets, 3}).
```

To find objects with more than a set number of siblings, and objects over a given size a `find_keys` fold can be used (see `riak_kv_clusteraae_fsm` for further details).  It is possible to run `find_keys` with a last_modified_date range to find only objects which have recently been modified which are of interest due to either their sibling count or object size.

#### Participate in Coverage

The full-sync comparisons between clusters are based on coverage plans - a plan which returns a set of vnode to give r=1 coverage of the whole cluster.  When a node is known not to be in a good state (perhaps following a crash), it can be rejoined to the cluster, but made ineligible for coverage plans by using the `participate_in_coverage` configuration option.

This can be useful when tree caches have not been rebuilt after a crash. The `participate_in_coverage` option can also be controlled without a re-start via the `riak remote_console`:

```
riak_client:remove_node_from_coverage()
```

```
riak_client:reset_node_for_coverage()
```

The `remove_node_from_coverage` function will push the local node out of any coverage plans being generated within the cluster (the equivalent of setting participate_in_coverage to false).  The `reset_node_for_coverage` will return the node to its configured setting (in the riak.conf file loaded at start up).


#### Suspend full-sync

If there are issues with full-sync and its resource consumption, it maybe suspended:

```
riak_kv_ttaaefs_manager:pause()
```

when full-sync is ready to be resumed on a node:

```
riak_kv_ttaaefs_manager:resume()
```

These run-time changes are relevant to the local node only and its peer relationships.  The node may still participate in full-sync operations prompted by a remote cluster even when full-sync is paused locally.

#### Trigger Tree Repairs

When an `all_check` is prompted due to a `{clock_compare, 0}` result, there are two scenarios:
- the cached trees differ but the differences lie outside the previous range;
- the cached trees differ due to a bad tree cache, and there are no actual differences.

For the second case, it is necessary to repair the trees, so when an `all_check` is triggered it will also prompt for trees to be repaired on this node (for the identified mismatched segments only) then next time there is a keys and clocks fetch.  The triggering should have a log of:

`Setting node to repair trees as unsync'd all_check had no repairs - count of triggered repairs for this node is ~w`

The triggering of tree repairs increases the cost of the fetching of keys and clocks.  Each trigger is coordinated so that it is only fired once and once only (per trigger event) on each vnode.  Usually there is a single vnode with a bad tree cache, but it may take a full cycle of checks for the trigger to be enbaled and enacted on the correct node.

Normally bad caches are a result of a tree rebuilds.  It such triggered repairs are required frequently, consider reducing the frequency of aae tree cache rebuilds:

`tictacaae_rebuildwait = 1344` - increases the wait between rebuilds to 1344 hours (8 weeks).
