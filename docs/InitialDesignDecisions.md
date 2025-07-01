When starting with Riak a number of initial design decisions need to be made at the outset of the project.  This is a summary of those decisions, and the factors relevant to making each choice.

The initial design decisions are split into the following categories:
- [Database backend](#database-backend)
- [Ring size](#ring-size)
- [Intra-cluster data resilience](#intra-cluster-data-resilience)
- [Interconnecting multiple clusters](#interconnecting-multiple-clusters)
- [Deleting data](#deleting-data)
- [Mapping data to objects](#mapping-data-to-objects)

This document will cover each decision point, and also discuss how to transition where a sub-optimal choice has been made.

# Database backend

## Database backend - making a choice

A Riak database cluster is a collection of smaller databases (known as vnodes).  Each vnode has a backend which is responsible for storing, and proving access to the data.  The choice of backend is important to the performance of the solution, but also critical to the features which are available to use.

The following choices exist:
- leveled (default from Riak 3.4)
- bitcask (default prior to Riak 3.4)
- eleveldb (deprecated as of Riak 3.4)
- in-memory (deprecated as of Riak 3.4)
- multi-backend (supported only in limited use cases)

In geneal, the best choice is to use the leveled backend.  The bitcask backend may be used, especially if
- there is no potential future need for querying of data, and;
- objects are largely immutable, and;
- read-repair is sufficient to meet the application anti-entropy requirements.

Even in those situations, the leveled backend may be more efficient.  The leveled backed is not efficient for storing small objects (of size 2KB or less).  In this case it may be worth evaluating other options.

Use of multi-backend should generally be avoided.  It may specifically be used to manage multiple expiry schedules across multiple bitcask backends, but not if anti-entropy requirements exist beyond read-repair or if inter-cluster reconciliation is required.  In these cases managing expiry [through the use of the eraser process is preferred](#deleting-data). 

### Leveled

The leveled backend has the following characteristics and features:
- Pure erlang log-structured-merge (LSM) tree backend, designed and developed specifically for use within Riak.
  - Implementation in Erlang simplifies resource management as CPU scheduling of all Riak activity is under the control of the Erlang virtual machine.  
- Differs from most other LSM implementations in that values are set-aside in a sequence-ordered journal, and only keys and metadata are placed in key-ordered the LSM-based ledger.  This provides for lower cost and more efficient reads when only keys and metadata is required (which in Riak is internally usually the case, even when the external user requires the value).  It also reduces the overhead of write amplification, supporting more efficiently larger object sizes.
- Specific internal optimisations to increase efficiency within Riak for Tictac-based method of anti-entropy and inter-cluster reconciliation.
- Supports index entries as well as objects in the key-ordered ledger, to allow full use of the Riak query API.
- Is the priority backend used within the OpenRiak community for both functional and non-functional testing of new releases.
- Generally requires significantly less memory than the total size of all the keys.
  - A fixed overhead (per vnode) of about 20K keys and metadata is kept in memory, plus 1% of the keys, plus 2-bytes per key.
  - Guarding against out-of-memory errors is an operator responsibility.  The cluster should be expanded as the memory limit is reached, the per-vnode memory overhead will not be proactively reduced.

### Bitcask

The bitcask backend has the following characteristics and features:
- A simple low-code, journal based key-value store, originally design and developed by the Riak team; that has been used as a general implementation and model for such stores.
- Written primarily in Erlang, but including around 3K lines of C code to provide the in-memory database of keys.  Access to C-code is generally for short-lived functions, which limits the impact on the scheduling requirements of the BEAM.  The C-code is also extremely stable, and is low-risk in terms of overheads associated with platform evolution, and changes to C standards.
- Supports for pure objects only, no support for index entries and any part of the Riak Query API.
- Requires an out-of-hours merge window to be available and configured, for compaction of mutated objects.  Merging under database load may lead to highly unpredictable performance.
- Requires a separate key-store backend if anti-entropy of inter-cluster reconciliation features are required.
- All keys are kept in-memory, and so sufficient memory is required as the number of keys expand.
  - Guarding against out-of-memory errors is an operator responsibility.  The cluster should be expanded as the memory limit is reached, the per-vnode memory overhead will not be proactively reduced.
- No current support for optimised HEAD requests, which can have significant impact on overall efficiency within Riak.
  - Implementations of bitcask have been produced with this optimisation, and may be open-sourced in the future. 

### Eleveldb

The eleveldb backend has the following characteristics and features:
- A heavily-adapted version of the google leveldb store - a LSM tree backend written in C++.  The adapted version is now deprecated, and the original version is subject to only limited maintenance activity.
  - has specific optimisations when compared with google leveldb to: reduce stalling; share and schedule resources where multiple instances operate on the same server; recover disk space following deletion; support automated object expiry.
- Supports secondary index entries, but will not support the full Riak Query API.
- Potentially faster and more efficient than leveled, especially when values are small.
- Moves the majority of CPU and memory management away from the BEAM, to be managed directly within the C++ code.
  - This provides some additional capabilities, in particular the ability to fix the percentage of memory used used across all vnodes on a node.
  - This has some long-term maintenance overheads, which the OpenRiak community will not continue to support after the release of Riak 3.4.

### In-memory

The in-memory backend has the following characteristics and features:
- Not persisted, all data will be lost on restart (though not that Riak is resilient to the loss of data on an individual node).
- Based on the erlang ETS tables.
- Has crude and imperfect handling of out-of-memory issues to help limit the size of each individual vnode store.
- Supports secondary index entries, but will not support the full Riak Query API.

### Multi-backend

The multi-backend has the following characteristics and features:
- Allows different data buckets to be mapped to different backends, so that different buckets can utilise the different capabilities of those backends.
- Generally not recommended for production use, unless there are specific issues that cannot otherwise be handled.
  - Behaviour of individual backends is better understood, and subject to greater testing - especially when requiring anti-entropy and inter-cluster reconciliation. 

## Database backend - changing the choice

The database backend configuration is local to a node.  Some cluster-wide behaviour is dependent on the backend configuration being consistent across nodes (i.e. if some nodes use bitcask, but others use leveled - 2i queries will not work within the cluster as the bitcask-based nodes will not support the queries).  However, accounting for this, it is possible to change backend through a "rolling replace" within a cluster - by replacing one or more nodes at a time to a node with a previous backend configuration, to one with a new (where the new configuration has more capabilities then the old).

For example, a multi-backend configuration with bitcask and in-memory backends and parallel-mode tictac aae, can be upgraded to a single leveled backend with native tictac aae by migrating one node at a time using `riak admin cluster replace` (assuming the TTL capability requirement is not being utilised).

# Ring size

## Ring size - making a choice

A riak cluster distributes data across a number of individual databases (known as vnodes), and those databases are then distributed across the physical nodes and locations of the cluster.  The number of vnodes in the databases is required to be a factor of 2, and bigger than the total number of nodes in the database cluster.  This number is known as the "Ring Size".

Starting with a large ring size is helpful as:
- It provides greater potential for future scalability of the service, as for efficient use of hardware there should always be at least one vnode per CPU core within the cluster.
- It provides for more even distribution of load, as if `RingSize div NodeCount = K` each node will have either `K` or `K + 1` vnodes - and the relative delta between the size of `K` and `K + 1` decreases as the RingSize increases (e.g. with a node count of 6, there is a 10% difference between the resource required for a busy vs quiet node with a Ring Size of 64, but just a 2.3% difference with a Ring Siz eof 256).
- Background database activity is split by vnode, so increasing the number of vnodes has smoothing impact on that load making it more predictable.

Starting with a smaller ring size is helpful as:
- Index queries, where a small number of results (i.e. < 10K) are returned are more efficient with a smaller ring size.  If 1% of the overall database throughput is such queries, then a shift up in ring size can reduce efficiency by up to 10% - but with less load of such queries, the impact is less.
- There is a lower per-node memory footprint (with the leveled backend) if the vnode count per node is lower - although that footprint can be addressed through other means (e.g. reducing the `penciller_cache_size`).
- Background database activity is split by vnode, so decreasing the number of vnodes reduces the overall volume of such activity.

Unless it is expected that index queries will exceed more than 1% of total throughput, it is generally expected that `512` is a reasonable starting ring-size for production clusters.

## Ring size - changing the choice

Once a Ring Size is set, there is no way of updating a cluster in-place.  Accordingly, getting the ring size "correct" at initialisation of the cluster is important.  

If a change of ring-size is necessary, it can be managed by initiating a new cluster in parallel with the alternative ring-size.  The data can then be replicated (new updates via real-time replication, historic changes via range_repl aae_fold), and then the two clusters can be reconciled using the nextgenrepl tictacaae full-sync process. Once reconciliation is confirmed, transition to the new cluster with the alternative ring-size may be completed.

Completing such a transition on a large cluster may take between several days.

# Intra-cluster data resilience

## Intra-cluster data resilience - making a choice

A Riak cluster is a set of nodes (e.g. physical servers or virtual cloud instances), and the nodes within a cluster can be divided into separate locations (e.g. physical racks, cloud placement groups, cloud availability zones or operator-defined maintenance groups).  Guarantees about the safety of data within a cluster can be set for both the nodes and the locations.

There are three configurable elements that control the resilience of the data distribution with a cluster:
- `n_val`; the replication value for the data items within the cluster, how many copies of the data should exist within the cluster.  the n_val should normally be consistent across all buckets.  This is normally set to 3, but in some circumstances this may be changed to 1 (e.g. backup clusters) or 5 (e.g. for larger clusters looking to guarantee quorum reads in the presence of triple failures) - although in theory any positive integer value may be supported.
- `target_n_val`; the target for the distribution guarantee.  The `target_n_val` should always be consistently defined across nodes.  This is normally set to 4.  If the target is equal to the n_val, then all copies of the data stored within Riak in a healthy cluster will be guaranteed to be on separate nodes. If the target is greater n_val, then the integer difference represents how many failures can be tolerated before the guarantee fails. 
- `target_location_n_val`; the target for the distribution guarantee from the perspective of locations not nodes.  The `target_location_n_val` should always be consistently defined across nodes. This is normally set to 3.  When a `target_location_n_val` and `target_n_val` are both configured then the cluster change process will attempt to uphold both guarantees.  The use of locations is optional within Riak.

The target settings are used by the cluster claim algorithm, which is used whenever a cluster is formed or changed (e.g. joining or leaving a node) to redistribute the vnodes around the cluster  where required.  To use both `target_location_n_val` and `target_n_val` the cluster claim algorithm should be changed from version 2 (the default) to version 4.  When a `target_location_n_val` is not used a more optimal distribution may be achieved by using version 3 of the claim algorithm (in comparison to version 2).

Using a `target_location_n_val` in conjunction with version 4 of the claim algorithm, may lead to substantial calculation times on cluster changes. To discover what combinations may be supported given a cluster (given a count of nodes and distribution of nodes around locations), then the [offline ring calculator](https://github.com/OpenRiak/ring_calculator) may be used.  The bigger the `target_n_val` and `target_location_n_val` chosen, the more efficient and resilient the eventual cluster setup will be.  Failure to meet targets during cluster claim will lead to visual warnings when cluster change operations are requested - but not to failures.  If visual warnings are returned the ring calculator can be used to determine a supportable combination of settings.

As well as the cluster n_val settings, there is a configurable option to allow for pro-active reconciliation and repair of data within a cluster.  By default, there is no reconciliation within a cluster to ensure that data has been consistently replicated - it is done on a per-item basis when each item is read by the application, and so unread data may have un-repaired issues with resilience.  To enable pro-active reconciliation the `tictacaae_active` configuration should be enabled; it is strongly recommended when using leveled to enable this rather than the legacy `anti_entropy` mechanism.  This will enforce continuous reconciliation between vnodes, and gradual repair of any deltas.

Enabling `tictacaae_active` has additional benefits: it is a pre-requisite for inter-cluster reconciliation; and it also allows for the use of aae folds which provide important information ot operators about the data within the cluster (e.g. list buckets, find average object size, find objects in sibling state, find very large objects etc).  The overheads of running `tictacaae_active` differ based on backend choice, with the lowest relative impact being with the leveled backend.

## Intra-cluster data resilience - changing the choice

The `n_val` is in theory configurable by bucket, which allows for multiple n_vals to be used within the cluster.  However, each unique n_val will increase significantly the overhead of running anti-entropy (anti-entropy comparisons are per n_val, and separate caches are required for each n_val), and the complexity of configuring inter-cluster reconciliation.

Once a `n_val` has been set on a bucket, there is no tested way of reducing it and converging on a clean state - other than replicating to a new cluster and transitioning between clusters.  Increasing the `n_val` should eventually converge into an expected state.

The `target_n_val` and `target_location_n_val` configuration is used each time a cluster change is planned (i.e. adding or removing a node).  So using a new value will take affect once the next change is made within a cluster.

Both anti-entropy mechanisms can be deployed in parallel to help with transition.  Enabling anti-entropy takes time to take effect (as caches are built).  Disabling it is immediate, although garbage collecting any legacy on-disk overhead is a manual operator task.

# Interconnecting multiple clusters

## Interconnecting multiple clusters - making a choice

Riak clusters can be configured to replicate to other clusters, and further it is possible to continuously reconcile that the replication is correct (and prompt activity to resolve any deltas caused by replication failures).  Multiple clusters are generally used to:
- replicate into different physical locations to provide for disaster protection (e.g. between cloud regions, cloud providers, cloud availability zones, physical data centres, on and off-premises).
- provide clusters that support off-line usage of the data (e.g. reporting or backup), potentially with lower replication values (e.g. `n_val` of 1).
- replicate to additional clusters to provide for scale, or more efficient read activity in alternative locations.

The process of replication and reconciliation between the clusters is identical regardless of how multiple clusters are to be used.  An application cluster may consider two Riak clusters to be Active/Active, Active/Ready-Standby or Active/Passive; but these configurations must be managed in the architecture as Riak clusters are by default Active/Active. Riak is an eventually consistent database, and as such does not by default prevent concurrent writes.  Concurrent updates will lead to multiple-values being retained, with default settings, and merging those values should be managed within the application.

It is possible to control the possibility of consistency issues using conditional PUT logic (with token-based consensus) that will block all but one of a concurrent update when the cluster is in a non-exceptional state.  However this control is only possible within a single cluster, there are no controls possible to block the creation of multi-value siblings when concurrent updates are allowed by the application into different clusters.

It is necessary to make design choices between using locations for resilience and using clusters.  For instance, in a cloud environment one could choose:
- to deploy two clusters (`n_val` 3) in two separate availability zones, and a backup (`n_val` 1) cluster in a third availability zone - with locations aligned to placement groups for the nodes in the data-resilient clusters.  This provides that ability to switch the application between availability zones on failure without loss of resilience, and provides a total of 7 copies of the data all guaranteed to be in separate physical zones.
- to deploy a single cluster (`n_val` 5) spread across three availability zones (with locations aligned with availability zones).  This means that one cluster can be used to manage consistency (e.g. using conditional PUTs with token-based consensus), whilst guaranteeing there will always be at least two copies of the data available even if an entire Availability Zone is lost.  It would also be likely in this scenario that object GETs would generally not require the object value to traverse across availability zones (when using the leveled backend).
- to add to the above an additional cluster in alternative cloud provider or cloud region.

Topologies of clusters are possible.  With nextgenrepl it is not possible to forward updates through clusters - each cluster must subscribe to updates from every other cluster, so a full mesh is required to be configured.

When splitting a cluster across locactions, it should be noted that the network latency between nodes will delay client response to both read and write requests.  If there are network bandwith costs between locations, the se costs will be minimised by running separate clusters in each location.  Using locations within a cluster will not be as optimal, but will be reduce bandwidth costs for reads relative to configuring a cluster without locations.

## Interconnecting multiple clusters - changing the choice

Adding new clusters is a simple process assuming that tictacaae is enabled, but may be time-consuming based on the volume of data.  A new cluster can have real-time replication enabled to being to receive all new changes, then be seeded by using the `range_repl` aae_fold.  The seeding process needs to be throttled by controlling the number of sink workers on the new cluster to prevent the source cluster from being overloaded.  Once the `range_repl` process has completed, covering all the buckets, the reconciliation (aka full-sync) process can be enabled to detect any remaining deltas.  Not that the reconciliation process should NOT be used for bulk migration of data, as it is not designed ot efficiently handle large deltas.

# Deleting data

## Deleting data - making a choice

Deletion of data within eventually consistent databases is surprisingly complicated.  There are two aspects of deletion to be considered, dynamic deletion of individual objects, and the scheduled deletion of expired objects.

The complexity of deletion with eventual consistency is that it is an underlying requirement in a distributed system to compare potentially differing results between different locations for an object (i.e. between vnodes or between clusters).  If location A has an object, and location B doesn't; there is a need to differ between the situation where location B is correct (due to a deletion not being replicated), or location A is correct (due to an insertion not being replicated) - as these circumstances require opposing actions.  There are secondary consequences if keys are reused following deletion; whereby data could be potentially lost if an old deleted object is resurrected and appears to be more recent.

For deletion there are three modes with which a cluster can be run: `keep`, `immediate` and `time-interval`.  It is strongly recommended to where possible use the `keep` based method, especially where the intention is run multiple inter-connected clusters.  The `keep` method is a configuration whereby no object is directly deleted, it is replaced instead by a special `tombstone` object that has no value (and will appear as not found when fetched via the API), but retains a reference to its change history (the vector clock) so that it can be correctly assessed for recency when comparing with an undeleted version of the object, or a replacement of the tombstone.

Tombstones have no significant cost in terms of disk space, as they have no value, but they exist as a key and this represents an overhead.  It is good practice therefor to periodically reap old tombstones, where the tombstones have existed for a long-enough period to be sure no lingering problems of stale data exist for that key (e.g. tombstones > 1 month old).  If running multiple clusters and scheduling reap jobs, it is necessary to:
- use the nextgenrepl replication system;
- enable the configuration of `repl_reap` so that reaps are replicated;
- cancel any scheduled reap activity if there are node failures or cluster change operations in progress;
- monitor to ensure that the pace of reaping on the cluster supporting the reap job can be safely maintained on any cluster receiving the reaps (it is normally good practice to increase the `tombstone_pause` on the source cluster for the reap job when those reaps are to be replicated).

The `time-interval` method of deletion essentially attempts to automate the job of having a delay between the deletion and the reap.  However, for every object between deletion states all nodes hosting a related vnode for the object have to maintain an in-memory timer process for each object awaiting reap.  This limits the practical scale of the time-interval - generally this is used for setting the time-interval not seconds, which is deficient to fully handle the risks associated with deletion and false recovery of data.  The `immediate` configuration ignores the risk of deletion, and does not use tombstones. 

It is also possible to manage deletion through the use of a Time To Live.  However, using TTL is impractical when also using anti-entropy, and so this is not recommended unless there is permanent intention never to use anti-entropy or the related services (e.g. inter-cluster replication).  TTL will be supported as of Riak 3.4 on a per-bucket basis in-conjunction with the `aae_exclude` bucket property  - so that the disadvantages of not having anti-entropy are limited to those buckets with TTLs.  It is expected that this will be used for buckets which are specifically expected to be short-lived, and not requiring full reliability against data loss i.e. it is expected to be used for exceptional cases within a database, not as a general answer to deletion. 

As well as support via aae_fold for reap jobs, there is likewise support for erase jobs.  Erase and reap jobs, can be limited by bucket (mandatory), key range (optional) and last-modified-date range (optional).  It is recommended that scheduled erase jobs be used to handle garbage collection of temporary objects (using the last-modified-date range) in preference to TTL configuration.

## Deleting data - changing the choice

The delete mode is a per-node configuration which needs to be applied consistently across all nodes in a cluster, and across all connected clusters.  Changing the delete mode is possible, but needs to be a coordinated change across the whole environment - a small time delta is not an issue and can be handled by anti-entropy and reconciliation processes. 

# Mapping data to objects

## Mapping data to objects - making a choice

The most important design decision is how to map the data requirements in a project to objects in a Key-Value store.  Getting this correct tends to be specific to the application, and is inter-dependent on other initial design decisions - but there is some general guidance that tends to be helpful in most cases:

- Optimise the model for reading not writing, by storing information that is likely required to be fetched together in the same object.  It is normally easier to fetch a single object and strip unnecessary information, than it is is to fetch multiple objects to fulfill a single application data demand - where possible make the most common read requests fulfilled via a single object read request.
- By default always use `allow_mult = true`, and ensure all updates pass the context of a recent read.  The optimisation gains from using `allow_mult = false` or `lww = true` are small, and the actual behaviour in this mode is often misunderstood.  The setting `allow_mult = false` should be preferred to `lww = true`.
- Eventually parallel writes will occur, and siblings will exist.  Siblings can be minimised using conditional PUTs, if sibling resolution is complex or requires manual intervention.  Use aae_folds feeding operator dashboards ot track the generation of siblings.  To auto-resolve siblings CRDTs (conflict-free replicated data types) can be used, and third-party client-side libraries are generally a better long-term option than using Riak's internal CRDTs.
- Values can be large, especially when using the leveled backend.  Objects significantly in excess of 1MB are not in themselves likely to cause a direct performance issue.  Values are compressed before being persisted to disk (unless compression is disabled), when using the leveled-backend, so pre-compression is not necessary unless network bandwidth is a significant factor.
- Specialist data-types (e.g. conflict-free replicated data types) do not scale well as the objects grow.  The values of CRDTs (e.g. set sizes) should be kept small.
- There are three aspects to the object key - bucket type, bucket and key.  Bucket types simply allow for bucket metadata to be reduced, as the properties of the bucket are stored once in the type, not multiple times for each bucket: however there is no flexibility in the relationship between types and buckets (i.e. a bucket cannot change its type in the future).  In general scaling the number of buckets and bucket-types adds cognitive load for the developer and operator, especially if bucket properties are used to vary behaviour between different objects.
- Avoid single hot keys that are more frequently accessed (either for read or write) than any other key.
- Plan index entries for future query needs.  Conjunction queries are possible across indexes as of Riak 3.4 Query API, but best performance is generally met by pre-concatenating index terms and running range queries with filters on concatenated terms.
- For very frequently demanded queries, and where false negatives are not tolerable but false positives are - the use of direct index objects managed by the application is preferred over the use of secondary indexes.

## Mapping data to objects - changing the choice

Riak is designed to be agnostic to the format of the data, the schema belongs to the application and not the database.  It is therefore necessary to plan for schema migration within the application - detecting the schema version for an object, finding objects within a given schema version, updating a schema version in parallel to other application activity.  It is strongly recommended a lazy migration strategy is used whereby the application can roll forward each object to the latest version on GET, without necessarily updating the persisted version.  
