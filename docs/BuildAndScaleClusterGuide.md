# Riak KV - Building and Scaling a Cluster

This guide is split into two parts:

- [The considerations to make when choosing infrastructure for Riak](#choosing-infrastructure)
- [The practical steps to actually make and expand a cluster](#forming-and-expanding-a-riak-cluster)

## Choosing infrastructure

### Nodes

A Riak cluster is built up of multiple individual compute nodes.  Those nodes are expected to be distinct servers, or cloud instances.

> Riak is designed to scale-out system across _inexpensive_ computers, where Riak smoothly handles the failure of individual nodes.  Riak will run for extended periods with nodes > > down, so operator action can be deferred - the aim is to be highly available with minimal operator intervention required at inconvenient hours.

It is common for mission-critical production systems using Riak to NOT use component resilience that might be considered essential in a traditional scale-up database (e.g. RAID arrays); choose simplicity and speed of components, and expect the reliability to come from the Riak cluster not the individual nodes.

When making server or instance choices, the following guidance should be considered with regards to component choices:

- There are production use cases of Riak with rigid zero-data-loss requirements that use ephemeral storage components due to the reliability and repair capability within a Riak cluster, and between replicating clusters in diverse locations.
  - It is best to cost-optimise for speed and capacity with node storage, rather than for resilience.
- All the memory of the system will be used to improve performance, any memory not be taken by Riak should be consumed by the file-system page cache and increase throughput potential by reducing IO throughput and latency.  Over-provisioning memory is generally of value.
- The Erlang/OTP platform used by Riak, and the design of Riak itself, is optimised to make use of multi-core architectures; more CPU cores should generally be preferred to faster CPU cores.
  - There are production deployments of Riak on both ARM and Intel-based CPUs.  Riak depends on the Erlang VM which has JIT optimisations for both architectures.
- The Riak system is tested to perform predictably at certain throughput constraints - e.g. max CPU utilisation, bandwidth or disk contention.  Running Riak close to these limits for extended periods should not lead to volatile outcomes.
- The Riak system will fail suddenly if space constraints are breached - i.e. available disk space, memory and at open file limits.  There is no management of activity to prevent breaches when close to these limits.  It is is critical to monitor against these limits and have additional nodes available to scale out the cluster should breaching space limits become a threat.
  - Riak will open a large volume of file descriptors, to it is important to ensure that the Riak process has a sufficiently large ulimit set.  Generally this will need to be at least 256K, but a ulimit of over 1M may be required on large-scale nodes.

There are also broader considerations to be made with regards to node choices, and the overall organisation of nodes across a cluster, and when planning to operate a cluster:

- The Riak system will spread load evenly through the cluster, data is sharded across individual vnodes by consistent hashing, and vnodes are allocated to nodes so that each node will have either X or X + 1 vnodes.  All nodes should therefore have, wherever possible, equal capacity.
  - Riak has internal mitigation to the problem of individual nodes that are temporarily running slower than other nodes in the cluster; the job of fetching data blocks are balanced so that the work is generally performed on the fastest nodes (i.e those with available resource).  Also client responses are returned at the speed of a quorum of nodes, without waiting for the slowest response.  However, PUT and Query workloads will eventually be slowed to the pace of the slowest node.
  - Where individual nodes are undergoing long-running system tasks that may cause local slowness (e.g. RAID rebuild activity), it may be better for the nodes to be stopped (and therefore out of the active cluster), rather than acting as a slow node within the cluster.
- The design of Riak handles failure of individual nodes, however if the design of the underlying infrastructure can cause multiple nodes to fail concurrently (e.g. in a cloud environment where multiple nodes may be provisioned on the same underlying hardware), then resilience should be provided by running multiple clusters or by identifying in the Riak cluster groups of nodes with shared failure modes as "locations".  A location may be aligned in cloud environments with placement-groups or availability zones, with racks that have common network components in physical environments, or with maintenance groups where efficient operations require multiple-nodes to be updated concurrently.
  - The Riak cluster claim algorithm will allocate data so replicas are split across locations, so that sufficient copies of the data are always available.
- Riak performance testing of high-intensity database operations has demonstrated situations where virtualised cloud instances have performed up to 2.7 times worse than equivalent physical hardware.  Where possible abstraction between Riak the Operating System and the underlying hardware should be avoided.
  - Riak is designed to deployed as a single package where, other than monitoring and security software, Riak is the only package present on the node.  It is recommended to automate operational activity on Riak nodes through scripting tools (e.g. Ansible) rather than abstraction layers (e.g. Docker).
- Scheduling of operational actions within Riak cluster should avoid concurrent running of resource-intensive activity e.g. array integrity checks in software RAID systems, solid-state disk trim jobs, or operational security software sweeps.
- Some cloud providers offer special instance types design for scale-out databases (e.g. AWS im4gn family), and generally such instances should be preferred over general purpose instances.

### Network

The following considerations should be made when selecting the network infrastructure for running Riak:

- When using Riak to store and retrieve large (e.g. o(100KB) or bigger) objects, network bandwidth may be the bottleneck and in many systems bandwidth of more than 1 Gbps will be required.
- TCP incast is a generic problem in distributed systems, where multiple nodes return the same object to a coordinating node concurrently.
  - Since Riak 3.0, depending on storage backend, the potential for incast issues is significantly mitigated by coordinating with object metadata transmission rather than value transmission.
  - It is still prudent to consider the potential for incast problems in network design - in particular ensuring that network switches are data-centre class with appropriate buffer sizes.
- Riak is partition tolerant, in that during partition events data can still be stored securely across multiple nodes, and values can be merged (potentially forming siblings where conflicts cannot be resolved) when partitions heal.  Read events (both Object and Query API calls) may still not succeed correctly during partitions, particularly on minority partitions.  So although there exists aspects of partition tolerance, it is still important to design networks running Riak clusters so that partition events are rare.
- It is assumed in the design and development of Riak that network round-trip times within a cluster are o(1) ms or better.  At higher latencies network delays will tend to become the most significant proportion of the overall user response delay.
  - There is no assumption of minimal network latency between clusters, so resilience across geographically diverse locations with long round-trip times should be managed by running multiple clusters.
- For every GET from Riak, the value will normally be fetched once within the cluster (the majority of the time from within the same location) generating a intra-cluster network bandwidth requirement.  For every PUT the value will normally be sent three times within the cluster.  Between replicating clusters GETs do not create network bandwidth needs, but each PUT requires a single transfer of the value.
  - These facts may change depending on choice of both n_val and storage backend.
- The Riak API uses either protocol buffers (PB) or HTTP, and both interfaces can be converted to require TLS encryption - by configuring an additional listener for HTTPS and by negotiation on the in-clear listener with PB.
  - To ensure full protection from network eavesdropping, TLS enablement must also be enforced in the [Erlang Distribution Protocol](https://www.erlang.org/doc/apps/ssl/ssl_distribution.html).  TLS enablement must also be separately configured in the `riak.conf` file to ensure protection of handoff communication, and of replicated traffic.
  - Access controls with TLS enablement can be made via certificate or username/password identification with the PB API, but is only tested with username/password authentication via the HTTPS API.
- Riak has the option to enforce compression in the storage backend, but this will not lead to generic enablement of object compression for intra-cluster communication - the network must support the bandwidth necessary to transmit uncompressed objects within the cluster, and back to clients.
  - Compression of objects replicating between clusters may be enabled via configuration.

### Load-balancing

Non-functional tests of Riak are performed with requests distributed across the Riak cluster using the NGINX proxy.  Other proxy servers with equivalent functionality should also work.  The following considerations should be made when installing and operating a load-balancer for Riak connections:

- For full use of proxy functionality, use of the HTTP API is preferable.  Though some load-balancers will support load-balancing of general TCP connections, which can in-turn allow for load-balancing of PB connections, such solutions will likely constrain proxy functionality:
  - the proxy may not be able to log the details of individual requests or provide WAF-like features (no access bucket/query to information in URL or request methods);
  - the proxy many not be able to use proactive health-checks (e.g. in scripting non-HTTP server validation checks);
  - the proxy may not be able to use use reactive health-checks (as errors cannot be detected via HTTP response codes).
- When making extensive use of secondary indexes on objects, those indexes are stored as HTTP request/response headers.  Some load-balancing proxies (and HTTP client software) may apply limits to the size of both individual HTTP headers and the overall size of all HTTP headers - and without re-configuration those sizes could be breached with either large or numerous secondary index entries.
  - A proxy may also restrict the use of underscores, so it may be necessary to specifically enable `underscores_in_headers` to prevent requests including 2i headers with underscores from being blocked.
- Some legacy Riak clients will attempt to implement their own health-checking and load-balancing across configured destinations.  This should be disabled when using a proxy, as it may lead to unexpected failure propagation e.g. a client determining a proxy has failed because it load-balanced a request to a node that has failed.
- If enabling proactive health-checking of nodes, sending a `ping` request represents a weak check of availability, and a `status` request may have excessive costs.  It is better to use checks for the availability of sentinel objects instead (store specific objects in the cluster for the purpose of health-checks).
  - There is no mechanism for making objects permanent and immutable, so care must be taken to ensure sentinel objects are not accidentally deleted.
- When sending requests via a proxy, it is recommended to avoid connection pooling (e.g. use a `connection_close` of equivalent directive).  Pooling and reusing long-lived connections will reduce response times by a small margin; however there will be failure conditions that may take a long time to be detected, especially without frequent proactive health-checks.
- A proxy for a Riak cluster will generally require a significant amount of bandwidth, especially where the cluster is supporting relatively large objects.  Scaling proxy bandwidth may require a step-change in underlying network technology compared to that of the individual nodes.
- The 503 service unavailable message is used by Riak when sending a timeout.  However, such timeouts may occur because of poorly formed requests (such as overly complex queries).  It is therefore generally recommended that 503 errors should not be considered as server failures within the proxy configuration, so that nodes that coordinate complex queries are not marked as down.
- If a node is marked as `down` by a proxy, either through failure detection or operator intervention, it should be noted that the node will still play an active role in the cluster unless it has been stopped.  Marking a node as `down` is not sufficient to remove a role from service.
- When deploying a new node, note that it may be considered as active by a load-balancer when the node is started, and before the join has been initiated - and in this state the node would not have access to the data in the cluster.  Careful orchestration of change between the load-balancer configuration and cluster change actions is required.
- Load-balancing proxies are the recommended approach for logging of individual requests, should tracking per request be required.
  - The response time metrics provided by Riak commence at the start of the internal process, and do not include the time to deserialise the request and serialise the response.  Logging metrics from a proxy is a better way of assessing actual response times than relying on the Riak metrics.
- It is generally easier to automate the management of security controls by manipulating the configuration of a load-balancing proxy, than it is through manipulation of the Riak security CLI controls.  Many environments therefore delegate access controls that restrict users and networks to specific Riak functions to the load-balancing proxy.

## Forming and Expanding a Riak cluster

Riak may be deployed in the style of a traditional database, with a single node primary "cluster", and a single node standby "cluster" - with the replication and reconciliation controls in riak used to make sure that primary and secondary remain in-sync.  Such setups are commonly only found in non-production environments, the power of Riak is only truly realised when it is used as a scale out database where:

- `n_val` is at least 3 (there are three copies of the data stored for resilience in the cluster);
- the node count is at least 6;
- the ring_size is set so there are at least 2 vnodes for every CPU core in the cluster.

With modern hardware, a simple configuration such as this can achieve a very high throughput, whilst holding a large volume of data - a higher throughput than most small enterprises would ever require in their infrastructure.  As a consequence Riak is primarily targeted at technology companies, cloud providers, large enterprises or large public-sector organisations with nation-scale requirements.

The largest Riak users have o(1000) nodes, but these are generally split into different clusters serving different purposes or geographies.  It is rare to have individual clusters that scale beyond 50 nodes.

A cluster is formed by joining nodes to a cluster.  Note that a Riak node, when started is a cluster of 1.  If the ring-size is 256, a Riak node that is not part of a cluster will start 256 vnodes as it considers itself to be the whole cluster.  When nodes join a cluster, the handoff process is two-ways - the joining node is handing off vnodes it will no longer run to the cluster, and the cluster will hand off vnodes it requires the joining node to run to that node.  Note that each vnode consists of two vnode modules - `riak_kv_vnode` and `riak_pipe_vnode` - and both modules must handoff for a vnode handoff to complete (although generally the `riak_pipe_vnode` is empty so this handoff is immediate).

For details of the cluster management commands:

```
riak admin cluster --help
```

The process of joining, is a five stage process:
- staging changes;
- plan the change;
- verify the plan;
- commit the change;
- await handoffs.

### Join process - staging a change

There must first be a `join`, an act that simply informs the cluster of the intention to make a change.  To perform the join the joining node must be started and be configured with the same `ring_size` as the existing cluster, and must have its location set (if a location-aware cluster is required).  The join command is issued on the joining node.

### Join process - plan a change

The second stage is a `plan`.  In the plan stage, a claimant node, which will have been elected in the cluster, takes all the pending changes (in this case the joins) - and produces a plan of how vnodes should be arranged in the cluster following the transition.

As well as the pending changes, there are four inputs to that planning process:

- The `target_n_val` - which should be >= to the `n_val`. If this is set to the `n_val` this will simply guarantee that all primary locations for an object will be on separate nodes.  If this is set to `n_val + N`, then even after `N` failures each the object will still be stored on separate nodes e.g. the `target_n_val` is the number of primaries and fallbacks which must be on distinct nodes.
- the `target_location_n_val` - which defaults to `target_n_val` minus one, but the supportable value will depend greatly on the number of locations and how evenly the nodes are spread across those locations.  The higher the `target_location_n_val`, and the `target_n_val` the more certain the availability of data in the cluster is.  For experimenting with checking the validity of larger settings, there is a [ring calculator](https://github.com/OpenRiak/ring_calculator) where you can check your proposed configuration is possible before making the change.
- The `ring_size` - how many vnodes need to be distributed, this must be set across the cluster at the start of the cluster, changing the ring-size can only be managed by replicating to a new cluster.
- The cluster claim algorithm - which algorithm should be used to generate the plan.

There are three supported cluster claim algorithm in riak, and the algorithm is an environment variable which can be set in `riak.conf`.

#### Join process - `choose_claim_v2`

The v2 claim is a simple algorithm which achieves a minimal standard of correctness, but only when locations are not defined.  It is the default algorithm.

#### Join process - `choose_claim_v3`

The v3 claim algorithm which tries to find an optimal solution through a series of random attempts, and returns a plan which is the most optimal it found.  The algorithm is not idempotent, repeated planning may result in different outcomes.  Restoring a previous outcome after re-planning is not possible without expert support.  The v3 algorithm does not support locations, and is considered an experimental feature, but is still in use in large scale production systems.

#### Join process - `choose_claim_v4`

The v4 algorithm a brute-force algorithm which will attempt to solve a sufficient answer, potentially by exhausting all possibilities.  The v4 algorithm is the only effective algorithm for handling locations.  Because it seeks a sufficient answer, rather than an optimal one, the offline `ring_calculator` can be used to determine how far the target inputs can be pushed and still have a viable solution, before running the plan.

Due to the length of time the brute-force algorithm may take, the `plan` command may timeout - however work in progress is cached, so pausing and re-running the plan should return a timely answer

### Join process - verify the plan

The response to the plan request will be an outline of the plan.  If the plan does not contain any staged `leave` requests it will be a single transition plan.

The plan may contain a warning e.g. if the `target_n_val` has not been achieved:

> WARNING: Not all replicas will be on distinct nodes

To avoid an unsafe cluster, the plan must be cleared and another attempt made with different inputs (e.g. extra nodes, more locations, alternative targets, different claim algorithm).

### Join process - commit the plan

On issuing the `commit` of the plan, the transfers will be triggered, once certain management timeouts have occurred.  Note that the commit stage will re-plan the change, assuming the same inputs as the `plan` stage - in the case of `choose_claim_v3` by passing the same random seed to the random generator.  The `plan` is not a saved entity.

### Join process - await handoffs

The pace of handoffs within the cluster, where there is a significant volume of data to handoff, is determined by the handoff concurrency limits.  There are two concurrency limits, the `cluster_transfer_limit` and the per-node `transfer_limit` - both limits must be lifted to achieve higher concurrent transfers.

When increasing the number of concurrent transfers, it is important to monitor the system for signs of stress related to transfers, such as the `backend_pause` log in the leveled backend.  In some cases, where the recipient node for a handoff cannot process the inbound data fast enough, the handoff will error and exit.  following exit, when the handoff is re-scheduled it will re-commence from the start and redo all previous handoff work.  Avoiding handoff errors is critical to transfer performance.

The `handoff_batch_threshold_count` may be reduced if handoff errors are occurring.  This controls the size of each handoff batch, and reducing the size of a batch should reduce the risk the batch will not be processed within the timeout.

Current handoff status can be tracked with `riak admin transfers` or `riak admin handoff details`, but real-time indexing of the `riak_core_handoff_sender` logs will provide a clearest picture of handoff activity.

There may be extreme scenarios where cluster changes require significant reshuffling to satisfy or optimise the cluster plan, especially with location awareness enabled.  In these cases many vnodes may be transferred as part of one change - and this may create a scenario where an individual node is receiving new vnode handoffs, but has yet been able (due to concurrency controls) release existing data to alternative nodes.

In these circumstances, if a node is under disk space pressure, inbound handoffs can be temporarily disabled, and then re-enabled once outbound handoffs have occurred: see `riak admin handoff --help`.

## Shrinking a cluster

Cluster changes to sharing a cluster require the staging of `leave` requests.  These plans may result in two-phase transition plan - where in the first phase the leaving node simply offloads its vnodes to safe nodes (given the targets), and in the second phase the remaining nodes shuffle vnodes to ensure a better balance of load.

If the first phase creates an unsafe situation, where a remaining node has higher proportion of the disk space than it can support, an alternative plan can be made by using the `full_rebalance_on_leave` configuration option.  With this option, a single-phase transition is planned based on an ideal plan for the new layout, and a broader shuffle will occur bypassing the first phase (although this will lead to more handoff events).  This should always be enabled when using `choose_claim_v4`.