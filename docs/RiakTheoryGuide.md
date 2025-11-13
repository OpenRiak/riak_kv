# Riak KV - Theory Guide

This guide is a work in progress, and provides insight into the underlying theories and processes which underpin the function of a riak cluster.  Understanding this theory will be helpful to understand the design, setup and operation of a Riak cluster.

- [The ring and how data is distributed in Riak](#the-ring---the-distribution-of-vnodes)
- [Handling of requests](#handling-requests)
- [Background processes](#background-processes)

## The Ring - The distribution of vnodes

Riak is a set of smaller databases which are distributed across physical nodes.  The smaller databases are termed vnodes, and the vnode is a set of functions that are controlling a database backend - where the backend (either leveled or bitcask) does the work to modify and fetch serialised data from disk.

The number of vnodes is the ring size, which must be a factor of 2.  It is desirable for the ring size  to be much greater than the number of nodes (i.e. actual devices).  The ring size must be a factor of 2, because each key will be hashed to a given position in the ring, by taking a sha hash of the Bucket and Key, and using an equivalent function to: `Hash band (RingSize - 1)`.  This will give each key a position between `0` and `RingSize - 1`, i.e. zero-indexed position in the vnodes.

As the object should be stored in multiple places, normally 3 (which is our `n_val`).  An object is then mapped to the Position, and the `(Position + 1) mod RingSize` and `(Position + 2) mod RingSize`.  This position triple is called the preflist, or the set of primary vnodes for the key.

When a cluster is formed, a claim algorithm will distribute vnodes `0` to `RingSize - 1` around the physical nodes, so that all of these preflists fall onto 3 separate nodes, but also ensures that for every such position `(Position + 3) mod RingSize` is also on a diverse physical node to the preflist for that position.

To restore full data protection after failure, Riak must request the next node along in each preflist to start a fallback vnode.  For example, if a node holding vnode 10 fails, then this has an impact on the keys that have mapped to vnodes 8, 9 and 10 - they all now have a missing vnode.  three fallback vnodes will now be started:

- A key which hashes to vnode 8 will be stored in vnodes 8, 9 and a fallback for 10 that runs on the node owning vnode 11.
- A key which hashes to vnode 9 will be stored in vnodes 9, 11, and a fallback to vnode 10 started on the node which owns vnode 12.
- A key which hashes to vnode 10 will be stored in vnodes 11, 12 and a fallback for vnode 10 started on the node which owns vnode 13.

If the distribution in claim is correct, the full divergence of `n_val` resilience is maintained even when a single node fails.  Having full resilience for greater numbers of failures is configurable (assuming there exists sufficient nodes).

Each primary vnode will store the data from three preflists, and only the data for those preflists - a vnode is never both primary and fallback.  The keys that map to itself (M), and the keys that map to `(M - 1) mod RingSize` and `(M - 2) mod RingSize` are those preflists.  Fallback vnodes will contain keys for just one preflist - so every primary failure requires the starting of three fallbacks.

In reality, the ring appears to be more confusing than it is, as it does not use simple integers `0`, `1`, `2`, `3` etc to represent the positions in the ring.  It actually uses the position from taking the hash bits from the high end of the hash not the low end i.e. for a ring size of 256 `Hash band (255 bsl 152)` is used rather than `Hash band 255`.  This causes all the vnodes to be instead named `0`, `1 bsl 152` (i.e. `5708990770823839524233143877797980545530986496`), `2 bsl 152` (i.e. `11417981541647679048466287755595961091061972992`)... etc, but the principle is still unchanged as if they were more simply `0`, `1`, `2`, `3` etc.

## Eventual Consistency

Riak is designed to be eventually consistent, in that it is:

- Permissive about accepting updates, ensuring data is stored securely on behalf of the application, even when the current state of the data relative to the update cannot be guaranteed;
  - either because some state may be in geographically diverse location where waiting for verification of present state would unacceptably increase latency,
  - or because availability of individual components has limited visibility of the current state.
- Definitive that all changes will eventually be visible;
  - not just because data is replicated between nodes and between clusters,
  - but also because it is continuously reconciled, with background process that efficiently analyse the overall system for discrepancies and proactively heal those deltas without operator intervention,
  - where that continuous reconciliation occurs both within and between clusters.

It should be noted that Riak offers the same guarantees of zero-intervention eventual-consistency for both multi-cluster environments as well as single cluster environments.  However, within a cluster it is possible to enforce conditions on writes to make Riak less permissive, but less likely to result in conflict (e.g. conditional PUTs with token-based consensus).

In databases in general, not being eventually consistent increases the operational processes required to ensure data integrity is maintained: e.g. static failovers between primary and standby clusters, intervention to recover from replication failures between regions.  However, being eventually consistent means that eventually there will be failure events that mean maintaining availability comes at a cost that object values will inevitably end-up in conflict - an object may have two values where the database cannot determine which is the most current, often as updates were made concurrently by two different application instances.

Handling an object where the value is in doubt, adds cognitive load to the application developer - it is the key trade-off between the operator and the developer to accept when adopting Riak.  It is possible to craft objects whereby the situation can always be resolved - known as conflict-free replicated data-types.  However, designing a system based only on those data types is another type of cognitive load for the application developer.

In general, most applications that depend on Riak evolve strategies to restrict and manage conflict scenarios:

- Separating immutable and mutable data into different objects.
- Using application conditions to direct updates to clusters to avoid conflicting cross-cluster updates on the same object.
- Use sharding within the application or Riak's conditional PUT controls to avoid intra-cluster conflicts.
  - Potentially adopt an event sourcing CQRS-type pattern, to first secure capture of the data and then retry updates to queryable stores, so that refusing a write downstream will not induce a risk of data-loss.
- Adding metadata to objects to allow deterministic resolution in either all cases, or just common cases.

### Quorum on Read, Write and Query

The default GET and PUT options are based on validating quorum within the cluster before returning a response to client.  Quorum meaning that a majority of vnodes within a preflist must have provided acknowledged input to the transaction.  So although Riak offers a guarantee that data will be eventually consistent, within a single, stable cluster results will generally be immediately consistent.  A read that follows a write will see the most up-to-date value, as a read must consult a majority of vnodes, and a write must update a majority of vnodes for that key.

Quorum is the default for the GET of an object, but not the default for a query run across multiple objects.

Queries are distributed to a single "covering" set of primary vnodes, and all index updates within a vnode are transactional to the object change; so Riak is different to some other distributed databases in that queries in a single, stable cluster will generally immediately reflect the latest update.  There is no post-update delay for indices to be updated. However, these index queries are checking only one replica - so if a primary vnode is active but not up-to-date (i.e. due to a recent recovery from failure or corruption), query results are not validated by checking results between replicas.  A query is not equivalent to a GET, it has a higher probability of failing to return up-to-date data.  This can be partially mitigated by relying on operator intervention during recovery (using the  `participate_in_coverage` setting to block a recovering node from participating in queries).

It is possible to use inverted indexes for queries within Riak, so that queries can also use quorum reads.  However, using inverted indexes in Riak 3.4 requires management from within the application, not the database.

## Handling requests

There are three core APIs, which have their own process for handling requests:

- [the object API](#object-api);
- [the query API](#query-api);
- [the AAE fold API](#aae-fold-api).

Common across the API is the concept of [dotted version vectors](#version-vectors), which is used through Riak to track the change history of an individual object.

### Object API

When a request is made to PUT an object in Riak, the PUT is sent to an available primary to coordinate the change.  A Primary vnode is considered available when the node on which it resides is considered by cluster health-checks to be active, and it is currently reachable.  The coordination of a change is the updating of the version history of the object (the version vector), storing the object and prompting replication to other clusters where required. The PUT is then sent to the remaining available primaries (or fallbacks should there be a failure), to be stored at those vnodes if the version history indicates this change is more recent that the currently stored object.

Handling a forwarded PUT is marginally less expensive than coordinating a PUT.

When a request is made to GET an object in Riak, the metadata (containing the vector of the version history) for that object is fetched from each vnode in the preflist.  The first vnode to respond is tasked with fetching the value, and the remaining responses are used to determine whether the fetched value represents the most recent version (and if it is it may be returned to the client as the response).  If a replacement (later) version is available, then that is fetched as the value instead.  If analysis of the version vector and the version of the values, cannot determine which value is up-to-date the full history of unreconciled values is returned as "siblings".

Handling the value fetch on vnode is an order of magnitude more expensive than simply handling the request for metadata.

Each vnode has a single queue through which all requests are received.  There is no priority on this queue, a request cannot be processed until all previous requests have been handled.  Latency on a very busy Riak cluster is generally governed by the vnode queue sizes.  The GET and PUT process are designed to ensure that request performance is never governed by the pace of the longest queue.  Activity can proceed with a quorum of answers, and work is dynamically reduced so that vnodes with longer queues do less work until those queues realign with other vnodes.

Within the object API load distribution is first based on consistent hashing (to find the preflist of vnodes), but the race to support the value fetch in `GET` operations, and also the selection of the coordinator of a `PUT` operation is designed to try and rebalance load discrepancies within a preflist of vnodes.

### Query API

When a query is made to Riak, the index entries for the objects are spread across all the vnodes, but due to replication between vnodes a complete answer can be obtained by asking approximately a third of the vnodes (i.e. either `RingSize div n_val` or `(RingSize div n_val) + 1`).  The query server distributes the query across this set of vnodes, and compiles the pre-filtered results returned to be passed back to the client.  The coverage planner, which determines the vnodes which are required to supply a complete answer to the query, attempts to balance the load by randomising the plans it produces.

Unlike the Object API, the query API will be impacted by the longest wait for any vnode in the coverage plan.  Under extreme stress, query latency will be more volatile in the cluster than individual object latency.

When a query request is processed by a vnode, it is not run directly.  A rapid snapshot is taken of the vnode, which is passed to an async worker to run the actual fold - so that other requests in the vnode are not delayed by the fold.  Each vnode has its own dedicated pool of workers for running these folds on the snapshots.

The Query implementation is highly parallel.  It is quick to return 1,000 index entries from a vnode, but as keys will be fetched from `RingSize div n_val` vnodes concurrently - it can be nearly as quick to return 100,000 index entries from a cluster.  The filtering of index entries (using regular expressions or filter expressions) is also distributed.  Queries will make heavy use of available CPU cores across the cluster - and the fairness of that use is controlled by the Erlang scheduler, not directly by Riak.

Result collation, when a list of keys or terms and keys, happens on the coordinating node - the one to which the application sent the query request.  Sorting and serialising large sets of results is not parallelised and will impact just that node.

### AAE Fold API

AAE folds are distributed to run across the cluster using the same coverage planning process as the Query API.  AAE Folds will run against the leveled keystore, or a parallel AAE keystore when using bitcask or mulit-backend bitcask - the parallel AAE node is a modified version of the leveled backend.

Regardless of whether a parallel keystore or a native keystore is to be used for the fold, the request must still wait in the vnode queue.  As with the Query API there is a rapid snapshot so that the actual query operation can be passed to an async worker, and does not delay the vnode.

Unlike the Query API, the AAE Folds are considered to be non-urgent, and so all folds use shared pools on the node (`node_worker_pool`) rather than a per-vnode pool.  This constrains the CPU cores which can be concurrently busy running AAE folds.  AAE folds tend to be long-running (they often scan whole buckets), and there performance is governed both by their functional complexity and the access to available capacity in the `node_worker_pool`.

The snapshots taken for folds (or queries) are released once a fold is completed.  While a snapshot is active, which includes the time the snapshot is awaiting capacity in the `node_worker_pool`, there are constraints on garbage collection:

- compaction of the leveled journal (the value store) is not constrained;
- compaction (merge) of the bitcask backend is not constrained;
- compaction of the leveled keystore (either native or parallel) will continue, but space freed by the compaction will not be released until the snapshot is released.

All query types have a hard timeout, when the snapshot will be released regardless of whether the query has completed.

## Version vectors

> TODO

## Background processes

Riak has a number of background processes:

- The primary background process in Riak, when configured, is [the tictacaae active anti-entropy](#anti-entropy), the continuous reconciliation process used to ensure all vnodes are eventually consistent.
- There are a [number of queue based](#disk-backed-queues) background processes, through which non-urgent activity can be deferred, to manage the impact of this activity on the performance of externally prompted requests.
- Maintenance of the distributed knowledge of the cluster state is managed by [background processes within the riak_core application](#riak-core-cluster-management).

### Anti-Entropy

Riak tracks the current state of the Version Vectors across all the key space to perform anti-entropy, to recover an object to its most up-to-date value if a vnode has a stale or missing entry.  Anti-entropy can be used both within and between clusters, using special cached and mergeable merkle trees; these trees allow entropy to be tracked across large key spaces highly efficiently.  There are also a number of other mechanisms that repair in reaction to the detection of failure (read repair), or in update vnodes following cluster changes (handoff for both repair, cluster change and recovery of fallbacks).

The active anti-entropy process is designed to be highly efficient, and very quick, when confirming no deltas exist.  The work to discover and repair deltas is relatively expensive - but is throttled in default configuration to avoid overloading the database.  As there are other anti-entropy mechanisms (e.g. quorum reads with read repair); slow repair is preferred to high repair-related resource utilisation.

The anti-entropy trees have 1,024 branches, and each branch has 1,024 leaves.  Each key in the store is mapped by a hash algorithm into a given leaf.  The hash value of that leaf is calculated by taking the a hash of both the Key and version vector for the object - and then performing an `xor` operation on all the hashes within that leaf.  The hash value for each branch is the hash of each leaf in the branch combined using `xor`.

Each vnode has a cached tree for each preflist the vnode supports (with a single `n_val` in the cluster there will be `n_val` preflists in each vnode, and hence `n_val` cached trees). The cached tree represents the state for the whole preflist on the vnode.  When an object is modified, then the object key and the both the previous and current version vector is sent to the `aae_controller` for the vnode; which will update the correct preflist's tree cache, using a double xor operation (in effect one to remove the previous hash, and one to add the new hash).

The intra-cluster anti-entropy can then compare the preflist tree for one vnode, with the preflist tree of another vnode within the same preflist, to confirm if the vnode's are in-sync for that preflist.  To make that comparison, only the 1,024 hashes (4KB) of the branches are compared.  If there is a delta, then the same branch comparison will be run in a slow loop - checking for deltas which are constant across the loops.  If the loop stabilises on a non-zero number of deltas, then the 1,024 leaves in those branches are compared in a loop to find a constant delta.  If there is no constant delta, the trees are considered in sync (i.e. any discovered delta was a matter of timing).

If a set of leaves is discovered to be out-of-sync, then there must be a comparison between the objects to discover which objects need repair.  To compare the objects between vnodes, only the Version Vectors need to be compared.  To find the Keys and Version Vectors for a set of leaves, a fold over the whole key_store (either native or parallel) is required - however that fold is passed the segment IDs (an integer identifier for the leaves), and the store has in-built hints to filter out blocks of keys that do not contain segment IDs of interest.  This means the cost of finding Keys and Version Vectors is significant, but mitigated by the segment ID acceleration.

To limit the volume of data to be compared, and improve the performance of searches for Keys and Version Vectors, the number of segment results to be compared as a result of any exchange is limited.  All anti-entropy processes will also try and gather information from previous delta discoveries to intelligently reduce the scope of future discoveries - i.e. by looking at the modified date range in which differences fall, or if they are limited to specific buckets.  With information from previous deltas, the cost of finding more deltas can be reduced.

There exists the possibility that some event might cause the tree cache to become out of sync with the vnode backend store.  There are two processes to control this should it occur:

- when requested to find all Keys and Version Vectors for a set of segment IDs, the tree cache is also rebuilt for those leaves as part of the query.
- periodically there will be a cache rebuild event, where there will be a fold over the key store, and a full rebuild of the tree cache.

When running Anti-entropy in parallel mode, there is also a need for periodic rebuilds of the key store.  These may be expensive events, depending on the size and type of the store.  The rebuild jobs use random factors to try and prevent coordination of rebuilds between stores, and rebuilds are also queued using the node worker pool to prevent excessive concurrency of rebuilds.

Inter-cluster reconciliation uses the same principles as intra-cluster reconciliation.  For inter-cluster reconciliation the state of the clusters must be compared, not the state of the vnodes - two clusters may have different ring sizes, so a vnode-to-vnode reconciliation would not necessarily work.  To find the state of the cluster, the trees for all preflists can be merged into one tree using the `xor` operation.  Special coverage queries known as AAE folds, are used to either merge tree components, or to find Keys and Version Vectors across the cluster.

The cost of resolving entropy inter-cluster is higher than with intra-cluster entropy - and so the throttling of that resolution is generally stricter.

### Disk-backed Queues

> TODO - i.e. replication queue, reaper, eraser, reader

### Riak Core cluster management

> TODO - perhaps point to riak_core wiki?

