# Bucket Properties

A summary of the bucket properties that can be configured in Riak.

There are two types of Buckets in Riak - non-typed buckets, and typed buckets.  Typed buckets were introduced to make it easier to expand the number of buckets that can be supported with non-default properties.  

A number of "defaults" for bucket properties are configurable via `riak.conf` e.g.

```
buckets.default.n_val = 3
buckets.default.merge_strategy = 2
buckets.default.pw = 1
buckets.default.allow_mult = true
...
```

Configuring these defaults will impact only non-typed buckets.  So any bucket name used where no type is specified will inherit these defaults, but any typed bucket created will NOT inherit these configured defaults - typed buckets instead have fixed, pre-defined defaults.

Two pre-defined defaults changed with the introduction of typed buckets (the merge strategy aka `dvv_enabled`, and the `allow_mult` configuration), it is strongly recommended to configure your clusters to have the new default properties for non-typed buckets to avoid confusion with non-typed buckets having different defaults i.e. by adding to your `riak.conf`:

```
buckets.default.merge_strategy = 2
buckets.default.allow_mult = true
```

As any change made to `buckets.default.*` configuration in `riak.conf` is not inherited for typed buckets, there is no way of changing the defaults for typed buckets, so the operator is required to ensure that all default properties are manually set on every type.  For example if you wish to change the default n_val to 5 - this needs to be changed in riak.conf `buckets.default.n_val = 5` but ALSO the property `{n_val, 5}` has to be added on every single bucket type created.

In general, setting bespoke bucket properties should be done using typed buckets due to the relative efficiency of the implementation with types, but changes to defaults should be considered very carefully.  Bespoke properties allow for bespoke behaviours, but bespoke behaviours add to the cognitive load of future operators.

Default changes made via riak.conf need to be set consistently across a cluster.  No bucket properties are gossipped between clusters, so properties are cluster-specific.  In general any cluster setting related to vector clocks MUST be configured consistently across replicating clusters e.g. `dvv_enabled`, `old_vclock`, `young_vclock`, `big_vclock` and `small_vclock`.  Other properties can be different between clusters. 

Some changes can be applied using GET/PUT specific parameters, which will override the default bucket property i.e. a bucket could be configured to use `{sync_on_write, one}` but a specific PUT can override this by setting `{sync_on_write, all}`.  Although the use of GET/PUT specific parameters is supported, it is is not recommended.  Operation-specific parameters that override defaults are not logged, and can considerably increase the operator challenges when troubleshooting intermittent problems.

## dvv_enabled

In Riak 2.0 the handling of siblings was improved by the enabling of dotted version vectors.  All buckets should use `{dvv_enabled, true}`.  The introduction of DVV did not force non-typed buckets to use DVV, and by default non-typed buckets will continue to use legacy vector clocks.

To correct this the following configuration should be added to the `riak.conf`:  `buckets.default.merge_strategy = 2`.

If vector clock sizes are approaching the `small_vclock` limit, then it is important that `{dvv_enabled, true}` before pruning is applied, or pruning may lead to unexpected siblings.

## allow_mult

The `allow_mult` bucket property has a default value of `true`, for any typed bucket, but a default value of `false` for any untyped bucket.  It is recommended to use the value of `true`.

The internal workings of Riak are identical for the two allow_mult settings, with the exception of the case when an unresolvable conflict is discovered in the object change history.  In this case: if `{allow_mult, true}`, all conflicting versions are returned to the client to resolve (on the next GET); if `{allow_mult, false}` only the object with the most recent last_modified_date is returned.

The last_modified _date is a microsecond-level timestamp, that depends on the accuracy of the local node's clock.  If the timestamps match of conflicting changes, then an arbitrary choice is made, although there is a preference for changes with values over deletions.  When using `{allow_mult, false}`, the use of reliable time sources to co-ordinate time within and across clusters is strongly recommended.   

When using conflict-free replicated data types, `{allow_mult, true}` must always be used.

Unless the non-existence of an object can be guaranteed by the application using Riak, it is recommended that applications always read before write, and include the vector clock from the read in the write.  This ensures that even when using `{allow_mult, false}`, fallback to time comparison is kept to a minimum.

## last_write_wins

The `last_write_wins` bucket property has a default value of `false`.  It should only ever be changed when the `allow_mult` bucket property is set to `false`.

In general, the default should be used, even when `{allow_mult, false}`.  Setting `{last_write_wins, true}` changes the behaviour on PUT, so that an incoming write is assumed to be superior to an existing write without checking the change history of the existing object.  Internally within Riak, the actual order which PUTs are applied is non-deterministic, and there are many situations (replication, anti-entropy, handoffs) where old PUTs may be received after new PUTs.  In these cases setting `{last_write_wins, true}` may have unexpected consequences

If, and only if, the bitcask backend is used, and objects are being updated and not simply inserted, and there is no use of tictac anti-entropy, then there is a small performance advantage from setting `{last_write_wins, true}`.  For other backends and scenarios there is no performance benefit.

It is recommended that `{last_write_wins, true}` only be used when for once-only PUTs with the bitcask backend (where a small performance benefit will exist); where the consequences of out-of-order writes have been fully considered.

## n_val

The `n_val` bucket property has a default value of `3`, and can be set to any positive integer: though general only values of `1`, `3` and `5` are in common use.

Setting distinct `n_val`s on a per-bucket basis is not recommended, it is preferable to have a consistent `n_val` across a cluster.  This is because:
- related configuration settings `target_n_val` and `target_location_n_val` are cluster-wide and not bucket-specific;
- the scope of the anti-entropy system grows with every unique n_val;
- nextgenrepl full-sync configuration is specific to each n_val, having multiple n_vals requires different nodes in the cluster to reconcile for different n_vals.

The value of `1` is sometimes used in read-only clusters, to reduce storage costs in clusters purposed for backups or offline-reporting.  The value of `5` is used only in very large clusters, either as the probability of concurrent failures requires higher redundancy, or because there is a need to improve the efficiency of secondary index queries.

Changing an n_val on a bucket which already contains data will have unexpected and untested consequences, especially when contracting the n_val.

## node_confirms

The `node_confirms` bucket property has a default value of `0`, and may be set to any non-negative integer less than or equal to the n_val (for that bucket).  The purpose of `node_confirms` is to offer a guarantee that the data is available on multiple machines, for example setting node_confirms to 2 will guarantee that at least two machines have the data - and the risk of the data being lost can be considered accordingly.

the `node_confirms` property were added as an alternative to using the `pr` and `pw` parameters to provide guarantees that distinct nodes were used for storage before confirming reads/writes.  The use of `pw` could both fail to provide the desired guarantee of physical redundancy; but also could prompt false failures in circumstances where the data was still resiliently stored, reducing availability.

Note that `node_confirms` is applied on both reads and writes.  The parameter is also applied on reads so that an application can understand on read that a previous put has not yet reached the required level of diversity.

## sync_on_write

The `sync_on_write` bucket property has a default value of `backend` allows for more flexible guarantees about data being flushed to disk.  By default, Riak backends will confirm a PUT once a file write has been completed, but that write may only be resident in memory in the file-system page cache; so at this stage the data is not safe (for example if a power failure simultaneously killed multiple nodes).  Riak backends can be configured to flush all writes to disk, but this has a significant impact on throughput, both in normal operation (each PUT prompts n_val flushes per cluster) and also when managing transfers between nodes. 

The `sync_on_write` bucket property can be configured to `backend` (default - revert back to original behaviour, and use only the backend setting), `one` or `all`.  It is assumed when using `sync_on_write` the backend will be configured not to flush to disk on every write.  In this case a write to a bucket with `backend` may be resident in memory on all nodes after the PUT is confirmed to the application client.  If `all` is set, all writes that have confirmed will also have been flushed (by default 2 of 3 writes must be confirmed before the client receives a positive response).  If `one` is used, the first location to process the PUT will flush to disk, where other locations are allowed to hold it in memory in the file system page cache.

The `sync_on_write` property is used only for PUTs via the API.  Internal PUTs (e.g. for transfers) will ignore the property and use the backend configuration, and so will not carry the overhead of flushing.

It is recommended not to use backend sync configuration, and instead control flushing only through use of this bucket property.

If replicating between clusters and `one` is used as the `sync_on_write` bucket property, then the cluster that receives the PUT form the application will flush to disk on one node - but all clusters receiving the PUT via replication will not be required to flush to disk on any node.  The properties of `backend` or `all` are treated equally in source and sink clusters.

## aae_tree_exclude

The `aae_tree_exclude` bucket property has a default value of `false` and allows for some flexibility when reconciling between clusters using nextgenrepl full-sync.  In general with Riak nextgenrepl it is assumed that clusters aim to contain the same data.  It is possible to replicate specific buckets between specific sources, and also possible to reconcile only individual buckets between clusters - but per-bucket reconciliation is not as efficient as full-cluster reconciliation.  The efficiency of full cluster reconciliation is based on the use of cached and mergeable aae (active anti-entropy merkle) trees that represent all the data in the store.

The purpose of `aae_tree_exclude` is to not include the bucket in the cached tree, so that the bucket isn't considered in the reconciliation job.  For example, this may help when:
- a subset of buckets are not replicated between clusters;
- a bucket is using a backend TTL within one of the clusters (a cached tree cannot coordinate changes with backend stores which implement auto-expiry - so cached trees are prompt false AAE workloads when a backend TTL is used).

If a bucket is configured to `{aae_tree_exclude, true}` then the keys in that bucket are not added to the cached tree, and are not considered when running either inter-cluster or intra-cluster anti-entropy reconciliation jobs.  The keys are still visible to aae_folds, and if using parallel-mode tictacaae modification will still impact the parallel mode key store.

The preferred long-term strategy for temporary objects is to use the eraser and reaper processes to garbage collect objects, rather than relying on backend TTL.  However when migrating from a multi-backend store with TTL-based backends, the migration should be easier if: those temporary buckets are excluded from aae trees, are replicated separately using range_repl, and reconciled using bucket-specific aae full-sync jobs.

The `aae_tree_exclude` bucket property may be cached by processes within a cluster, so changing the property will not have immediate.  If changing the property it should be coordinated with a rolling restart.

## small_vclock

The `small_vclock` bucket property has a default value of `50`, and that sets the size of version vectors before pruning will take place. Version vectors will initially tend to be the size of the total of all n_vals in all clusters accepting writes for that value (two clusters with n_val of 3 will lead to version vectors of size 6 if objects are subject to sufficient updates).  However, when nodes are replaced, and when clusters are expanded or contracted, new potential vnodes are generated which may lead to the version vector expanding.

It is not recommended to change the `small_vclock`, unless specific problems are seen with objects reaching the pruning limit - and in this case increasing the size may be used as a workaround to those issues.  Any change must be reflected in all connected clusters.

Other vclock settings - `old_vclock`, `young_vclock`, `big_vclock` - should not be changed from defaults without careful analysis of the vclock pruning code. 

## notfound_ok

The `notfound_ok` bucket property has a default value of `true`, and this means when calculating the 'r' value of a read a response from an individual vnode will count as a valid read, and so will count towards quorum being reached.

There are two circumstances where setting `{notfound_ok, false}` may be used:
- when the application never expects to read keys that are not present, and so not_found is a failure;
- when specific performance to reduce `r` or `w` values to fast return to clients before operations have reach quorum, that may lead otherwise to near-parallel reads and writes falsely responding `not_found`.  Note that such performance hacks are generally not recommended.

## pr and pw

The `pr` and `pw` bucket properties default to `0`, and are used to require primary vnodes to be involved in reads and writes.  This may prevent writing to minority partitions, however when `{n_val, 3}` this will probably lead to intermittent failures when only two nodes fail in a cluster.  As clusters grow the probability of two concurrent failures will increase significantly.

It is strongly recommended to consider using `node_confirms`, `sync_on_write` or token-based conditional PUTs to achieve controls in preference to configuring `pr`/`pw`.

## General read/write parameters

There are a number of configurable read/write parameters - `r`, `w`, `dw`, `rw`, `basic_quorum`, `sloppy_quorum`.  It is strongly recommended to stick to default quorum settings.  Any attempt to re-configure to improve speed of response to clients, will increase the risk of overloading vnode mailboxes and causing unnecessary failures.