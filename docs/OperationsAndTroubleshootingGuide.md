# Riak KV - Operations and Troubleshooting

The following sections provide guidance when operating or troubleshooting a Riak cluster:

- [Handling failure - replace, repair and recover](#replace-repair-and-recover)
- [Using the remote console](#remote-console)
- [Accessing extended configuration options](#extending-configuration)
- [Making use of logging and statistics](#logging-and-statistics)
- [Monitoring background operational services](#monitoring-operational-services)
- [Enabling Riak security controls](#enabling-riak-security)
- [Garbage collection - monitoring and tuning](#garbage-collection---reap-erase-and-scheduled-compaction)
- [Understanding the contents of a Riak cluster](#data-inspection)
- [Volume and performance testing](#volume-and-performance-testing)
- [Backing up a cluster](#backup-options)
- [Advanced troubleshooting of Riak internals](#advanced---troubleshoot-via-the-erlang-vm)

## Replace, Repair and Recover

There are several potential repair and recovery processes for handling different scenarios:

- [proactive replace](#proactive-replace);
- [reactive replace](#reactive-replace);
- [leveled backend repair](#repair-an-individual-leveled-store);
- [repairing a single vnode](#repair-an-individual-vnode);
- [repairing a key range](#repair-key-ranges).

The most common repair requirements are for proactive replace, and reactive replace: testing these processes under load prior to production deployment of Riak is recommended.

### Proactive Replace

It is possible to proactively replace a node in a Riak cluster, for example if:

- a cloud provider intends to withdraw an instance;
- a hardware upgrade is required;
- to change the storage_backend of a cluster node-by-node;
- or to fully vacuum a node's storage backends of garbage.

A proactive replace is a cluster administration change, and [follows the standard five stage process described in the general guidance on amending the cluster make-up](/docs/BuildAndScaleClusterGuide.md#forming-and-expanding-a-riak-cluster).  In the case of a proactive replace, the first stage, staging, requires the staging of two changes:

- the `join` of a new node, and
- a `replace` to indicate the old node which should be replaced.

The plan should be planned, reviewed, committed and then monitored as with other changes.

The node may have its `location` set prior to the `join`, but the location will be ignored by the `replace` i.e. if the replacement node is in a different location to the existing node, this will not be factored in - the replace will transfer all vnodes to the new node, regardless of the `target_location_n_val` constraint.  Staging a location change after the `replace` has completed (i.e. following the `commit` and the transfers), may be used to `plan` a reshuffle of the cluster as a separate change activity.

See `riak admin cluster --help` for further details on the required inputs to cluster change commands.

During the replace operation the replacement node should have `participate_in_coverage` disabled, and have coverage support enabled only once all transfers have completed and (if configured) tictac anti-entropy has confirmed that all vnodes are in sync.

After completing a proactive replace operation, it may be necessary to realign node naming with design documents or monitoring systems; to rename a replacement node with the name of the node it replaced.  Once the replace operation is complete, it is possible to rename a node while it is down using `reip_manual` - see `riak admin reip_manual --help`.  The ring_directory is normally named `ring` in the platform data directory.  It will contain files such as `riak_core_ring.default.20221122164111`, where the middle term between the periods (in this case `default`) represents the required cluster name.

### Reactive Replace

If a node temporarily fails, then recovers without a loss of historic delta; the node will automatically rejoin the cluster and have any delta in data patched via anti-entropy mechanisms, without the need for operator intervention.

If a node has failed following an incident, and all data on the node is lost, the cluster can still be recovered back to its previous state without requiring a backup of the failed node.

Recovery of such a lost node requires a reactive replace.  There are three stages to replace and recover the node:

- [ensuring the node is downed](#administratively-downing-a-node);
- [forcing the replace](#forcing-a-replace);
- [repairing the new node](#completing-a-repair).

#### Administratively Downing a Node

A node that is down, should not have a negative impact on the cluster.  There may be situations though, where a node is impaired, but that issue has not been automatically recognised, and so the node is not considered as down within the cluster.

The status of all nodes in the cluster, from the perspective of another node can be gained by running:

```bash
riak admin cluster status
```

There are four states that a node can be considered to be in `up`, `down`, `up!` and `down!`.  The `!` indicates that the health-check status is unexpected given the administrative status - i.e. a node that is `down!` is not functioning as expected, but has not been marked as `down`.

If a node is known to be not operational, it should be marked as down using `riak admin down` from another node; and this should set the status of an unhealthy node to `down` not `down!`.

#### Forcing a Replace

When replacing a failed node, the situation differs depending on whether the new node is to be given the same IP address as the replaced node.  If the new (replacement) node has been built with the same address and naming it can be re-joined by re-staging a join, planning the change and committing it (which should lead to no actual transfers).  If the new node has differing configuration, then the plan will require a `join` and a `force_replace` operation to be staged.

If `force_replace` has been used, then the replacement node can be renamed at a later date using `riak admin reip_manual`.

The new node should be started with `participate_in_coverage` disabled, as it will at this stage be a full member of the cluster but have no data.  It is also more efficient to suspend anti-entropy until the repair is complete.

```bash
riak eval "riak_client:tictacaae_suspend_node()."
riak eval "riak_client:remove_node_from_coverage()."
```

#### Completing a Repair

The data can then be recovered from the other nodes in the cluster issuing the `riak_client:repair_node()` command from the `remote_console` of the replacement node.  This will prompt all vnodes which partially overlap the data held in the vnodes on the replacement node to race to play a role in repairing the node.  Each vnode will only repair the data which overlaps, filtering out any data that another vnode has already repaired (or is in the process of repairing).

To improve the performance of repair, the `repair_span` configuration in the [riak_core schema section of riak.conf](https://github.com/OpenRiak/riak_core/blob/openriak-3.4/priv/riak_core.schema) can be changed to `double_pair`, and this has been proven to be more effective when used with the leveled backend together with the enablement of the `repair_deferred` option in the [riak_kv schema section of riak.conf](https://github.com/OpenRiak/riak_kv/blob/openriak-3.4/priv/riak_kv.schema).

The combination of `repair_span = double_pair, repair_deferred = enabled` is significantly more effective when repairing under load.  With these configuration options, it should be noted that repairs will happen in key order, not in reverse order of receipt (the default).  With these changes, using the leveled backend, non-functional testing demonstrates that repairs can complete efficiently even when nodes are persistently at 100% CPU utilisation due to the handling of application requests.

Repair uses handoffs, and so can be tracked as with other cluster change operations.  Once handoffs are complete, Tictac AAE should be re-enabled, e.g. by using `riak_client:tictacaae_resume_node().`.  Once Tictac AAE confirms all vnodes are in-sync - then `participate_in_coverage` can be re-enabled.

### Repair an individual leveled store

The leveled backend is split into two parts - a journal, and a ledger.  The journal is the log of all received changes, and is the source of truth in leveled.  The ledger is a log-structured merge tree that provides a sorted view of the index keys, and object keys and metadata.  As the journal is the source of truth, the ledger can be rebuilt from the journal, and leveled will do this automatically on startup if the ledger is missing.

There exists the (very rare) potential for a ledger to be corrupted.  There are also circumstances where following an update the ledger is not fully efficient until it is rebuilt.  As a missing ledger is rebuilt automatically on startup, rebuilding of the ledger can simply be prompted by deleting it:

- Stopping the node;
- Deleting the ledgers in the impacted partitions (under each vnode's leveled store there should be a ledger folder);
- Restarting the node.

On restarting the node all missing ledgers will be rebuilt before the node becomes an active participant in the cluster - the `riak_kv` application which determines availability of a node, will not complete startup until all the rebuilds are complete.  Rebuild progress can be tracked in the leveled logs with `log_ref=b0006`.

Previous versions of Riak had an option to repair secondary index entries through a specific anti-entropy recovery process.  This is no longer supported.  If there are detected issues with inconsistency between objects and their index entries, then this should be addressed by the simple approach of deleting the ledger (which contains the index entries) to force a rebuild on restart of the vnode.

### Repair an individual vnode

Storage backends make use of CRC checks to detect and respond to corruption (by impacting individual objects not the whole store).  If an object, or a block of keys, becomes corrupted due to an issue with file storage then this should be detected by CRC checks.  The result of a failed CRC check, will be to respond as if the object in question is missing, rather than trigger a failure of the whole vnode.

Where such corruption is limited to a leveled ledger, then [a repair via leveled rebuild](#repair-an-individual-leveled-store) can be used to recover.  However, in other backends, or with a corruption in the leveled journal - it may be preferable to repair a whole vnode rather than wait for other anti-entropy processes to eventually resolve the impact of the corruption (by repairing each impacted object).

The process to [complete a full node repair](#completing-a-repair) can be targeted at an individual vnode to repair just that vnode.  To prompt the repair of an individual vnode, the partition number - the [integer identifier of a vnode](/docs/RiakTheoryGuide.md#the-ring---the-distribution-of-vnodes) - must be passed to the vnode repair function.  The vnode repair function (`riak_kv_vnode_repair/1`) can be called by using the [`remote_console`](#remote-console) or directly from the command line through the `riak eval` CLI call:

```bash
riak eval "riak_kv_vnode:repair(<partition_number>)."
```

The repair node will replace any object which the store does not presently hold.  However, following corruption, that validation may not be accurate and the store may incorrectly report presence.  So it is normally better to delete all the data on the vnode following corruption before triggering the repair.  Data will always be repaired eventually, deleting the store first ensures the time to repair is bounded and not dependent on long-running background recovery jobs.

### Repair key ranges

Outside of the circumstances covered in the previous sections, it is not expected that there should be a need for operator intervention in the recovery from failure.  There is though an additional process for handling any unexpected scenarios, to allow for cluster wide repair of key ranges.  The `repair_key_range` operation is targeted at a specific bucket, potentially combined with a key range or last modified date range: and triggers via an AAE fold the read repair process within the cluster for that range.

Refer to the [API guide for AAE Fold](/docs/OtherAPI.md#aae-fold-api) for information on triggering a `repair_key_range` AAE fold.

The aae_fold will send repair events to the `riak_kv_reader` queue, and progress can be tracked by tracking the queue's log outputs.  There is an automated background process on each node that will consume repair events from the queue, and trigger read repair (if required) by a clientless GET of the object.  Each node's reader queue is limited to 1M requests, and requests over this limit will be discarded.  This limit is not configurable in Riak 3.4.  The `riak_kv_reader` process will dequeue items from the `riak_kv_reader` queue and prompt an internal GET request; which, should there be a discrepancy, prompt a repair via `read_repair`.

Repair key range operations are a potentially efficient method for repairing keys across a cluster following a known incident, the impact of which was restricted to a given time range; and may prove to be quicker in some circumstances than waiting for the delta to heal via active anti-entropy.

## Remote Console

Advanced information and debugging tools are available via `riak remote_console`.  This will attach a remote shell to the running node.  With this shell Erlang functions can be called as if on the local node, and this can be used for a number of purposes.

### Accessing objects

To interrogate the internal representation of an object, it can be fetched from inside of the API using the riak_client module.  To use riak_client, a local_client must first be derived, to pass in as the final argument to riak_client function calls where required e.g.:

```erlang
(dev1@127.0.0.1)1> {ok, C} = riak:local_client().
{ok,{riak_client,['dev1@127.0.0.1',undefined]}}
(dev1@127.0.0.1)2> riak_client:get({<<"BucketType">>, <<"BucketName">>}, <<"KeyName">>, C).
```

The `rp/0` function call can be used to present the full output of any function within the shell, by default outputs will be truncated.  Note that displaying very large outputs fully in the shell may have significant costs and an impact on the usability of the session.

For the full functionality of [riak_client, see the module code](https://github.com/OpenRiak/riak_kv/blob/openriak-3.4/src/riak_client.erl).

### Running AAE Folds

Refer to the [API guide for AAE Fold](/docs/OtherAPI.md#aae-fold-api) for information on triggering an AAE fold from `riak remote_console`.

### riak_client remote_console commands

There are a number of administration commands that are available [via the riak_client module](https://github.com/OpenRiak/riak_kv/blob/openriak-3.4/src/riak_client.erl).  These include:

- `participate_in_coverage/1`, `remove_node_from_coverage/0`, `reset_node_for_coverage/0` - used to change the `participate_in_coverage` status of the node.  When a node is known to have a potential data issue (i.e. it is being recovered from a failure), it can be removed from coverage, and reset back into coverage once the data has been proven to be fully populated.
- `replrtq_reset_all_peers/1` - used to force all active and available nodes in the cluster to reset their peer discovery (used in real-time repl), to be used after adding a new node to a remote cluster.
- `replrtq_reset_all_workercounts/2` - used to force all active and available nodes to change their worker counts and per-peer limits, which may be required when a sink cluster cannot keep up fetching the remote replication traffic and so requires more sink workers (or sink workers per peer).

### Hanging console sessions

Remote console sessions are distinguished with `ps -ef` by the `-progname` switch.  Riak applications will have ``--progname <PATH>/bin/riak``; whereas remote_console sessions will have ``progname <path>/bin/erl``.

If an active remote_console session is detached in an unexpected way e.g. due to the network timeout of a SSH session over which the remote_console was run; "hanging" console process may be left running.  After a long period, a passive hanging console process may enter a loop and consume an entire CPU core, so it is wise to monitor for the presence of such long-lived hanging sessions.

### Using `riak eval`

All single commands run from riak remote_console should be scriptable from the command line using `riak eval`:

```bash
riak eval "riak_client:repair_node()."
```

## Extending configuration

### Using advanced.config

The advanced.config file is found within the `platform_etc_dir` (a location configured within `riak.conf`), which is normally the system `/etc/riak` folder, after a package-based build of Riak.  The file bypasses the Riak-specific configuration mechanism, and uses the native Erlang method for passing configuration into an Erlang application.  The file may be used for advanced configuration purposes, to supply environment variables to Riak which are not currently covered by the riak.conf schema files.

The variables need to represented as an Erlang object, which is a list of mappings between an application and a list of key/value pairs, as [described in the Erlang documentation](https://www.erlang.org/doc/apps/kernel/config.html).

e.g.

```erlang
[
    {
        riak_kv,
        [
            {delete_mode, keep},
            {add_paths, "other/"}
        ]
    }
].
```

Any configuration added in advanced.config will override any configuration set in the riak.conf file.

### Setting environment variables at runtime

All configuration in `riak.conf` is converted into erlang environment variables, and it is those variables that are used by the code.  The `riak.conf` is referred to only as the node is started - `riak.conf` changes will have no impact until the next restart.

To change the configuration of Riak at run-time, the variables can be changed via `remote_console` using the [application:set_env/3 function](https://www.erlang.org/doc/apps/kernel/application.html#set_env/3).

Note though, that:

- Some environment variables will be read by long-lived processes at startup, and so making runtime changes will have no effect.
- Configuration parameters will be translated into environment variables values by the cuttlefish schema - the configured value may not be a valid value for the environment variable e.g. `enabled`/`disabled` flags will commonly be translated into `true`/`false` boolean environment variables.  Changing an environment variable to the untranslated value may lead to node crashes.

In general, never change an environment variable at run-time via `remote_console` without first reading and understanding the code that uses it.

### Accessing configuration

From the command line it is possible to view the current description of a configuration option using `riak admin describe <option_name>` e.g. `riak admin describe conditional_put_mode `.

Note that the result of `describe` request is the current schema documentation of an option; whereas if a `riak.conf` file has been kept in place between upgrades, that `riak.conf` file may not have the up to date description.  The command-line `describe` is a more reliable way of understanding the present advice for a configuration option.

## Logging and Statistics

### Logging

All components of Riak use the kernel logger for logging.  The logger can be configured via `riak.conf`, and there are five parts of the configuration:

- Set the log level (`logger.level`);
- Set the file path and file name for each of the log files that are to be used (`logger.file`, `logger.error_file` etc).  The paths required depends on the configuration of `additional_handlers`.
- Set the log format (`logger.format`).
  - See the [erlang logger guide](https://www.erlang.org/doc/apps/kernel/logger.html) for metadata available to add to logs e.g. `mfa`, `pid`, `file`, `line`, `domain`, `msg`. 
- Set the logs to be filtered from the default (console) log file (`logger.default_filters`).
  - This can be used to divert recurring or verbose logs to specific log files (if additional handlers are defined for these logs), or simply have them be ignored.
- Set the additional handlers to defined for logs (`logger.additional_handlers`);
  - Where logs are filtered from defaults, they can be diverted to alternative files using `additional_handlers`;
  - Adding json to `additional_handlers` will write all logs to separate json file in a json format.

For example, an alternative configuration in `riak.conf` could be used such as:

```
logger.format = [time," [",level,"] pid=",pid," mfa=",mfa," ",msg,"\n"].
logger.background_file = $(platform_log_dir)/async.log
logger.default_filters = crash, error, progress, report, sasl, background, backend
logger.additional_handlers = crash, error, background, backend
```

It is possible to change logging at run time via [`remote_console`](#remote-console) by following the [standard Erlang logger guide](https://www.erlang.org/doc/apps/kernel/logger_chapter#example-add-a-handler-to-log-info-events-to-file).

The `background` filter and handler is targeted at `tictacaae` logs, and recurring metric logs which are triggered by frequent ticks.  The `backend` filter and handler will presently only handle leveled logs.  The leveled log level can be set independently to the general log level in `riak.conf` using `leveled.log_level`: though it will not be possible to alter this log level at run-time as with the general log (e.g. the module log level cannot be reduced to a lower log level than the `leveled.log_loglevel` for leveled logs, the leveled filter is applied before the kernel logger filter).

### Riak Stats

Riak collates statistics.  Stats include total counts over all time, counts in the last 60 seconds, and mean, median and approximate percentile response times measured over the previous 60s.  The stats are available via the CLI `riak admin status` or via a http GET request to the `/stats` endpoint.

Riak does not retain history of stats, so to track the change of stats over time it is necessary to request the stats at regular intervals, and index the stats in some other monitoring tool.  Other than the `_total` stats, the stats will always reflect the last 60s, regardless of how frequently the stats are requested (returning stats does not reset any values).  The stats process maintains a rolling view of the last 60 seconds.

All timings are internal timings, and not necessarily fully representative of external application experience.

The stats represent the statistics on the node from which they were requested.  The stats are not cluster-wide, they are always node aggregates e.g. the vnode stats are accumulated over every vnode on the node.

## Monitoring Operational Services

### Monitoring Anti-Entropy

> TODO - Pending PR which will add improved monitoring CLI

### Logging and monitoring of read repairs

> TODO

### Monitoring inter-cluster reconciliation

For information on monitoring inter-cluster reconciliation and repair [refer to the NextGen Repl guide](/docs/NextGenReplGuide.md#monitoring-and-run-time-changes).

### Monitoring node worker pools

Each worker pool will regularly log its current queue length and last checkout time (when it last picked up a new piece of work).  There are also riak stats for each pool, giving the average queue time (how long work is waiting in the queue), and work time (how long each piece of work takes).

> TODO - Will change after PR

## Enabling Riak Security

> Riak is expected to be deployed into secure environments, it is not a database designed for direct exposure on public networks.

Riak does have the optional capability to enable additional security controls, which are disabled by default.  However:

- It is commonly easier to provide stronger security controls than the Riak security measures, by using standard DevOps security tools to protect Riak;
  - Particularly when using the HTTP API, e.g. through the use of Web Application Firewalls or other HTTP proxying and filtering capability.
- The Riak security controls are designed to protect Riak from remote connections;
  - The controls can be applied to local connections, but enabling Riak security is insufficient to protect Riak from localhost attacks.

There are three parts to Riak's security mechanism:

- [TLS enablement](#tls-enablement);
- [Enabling Security and restricting the Source of requests](#enabling-security-and-restricting-source);
- [Granting permissions for specific actions](#granting-permissions-for-specific-actions).

### TLS Enablement

In Riak 3.4, support is provided for TLS 1.2 only.

The process for enabling TLS differs between the HTTP and PB interfaces.  For HTTP, there are two steps to enabling TLS on the API:

- Configure a listener on HTTPS within `riak.conf` - `listener.https.internal = <ip>:<port>`.
- Configure file paths within `riak.conf` to valid [`PEM` files](https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail) for three components:
  - `ssl.certfile = <file_path>`;
    - the pem file for the server certificate to be used by the Riak node in TLS negotiation.
  - `ssl.keyfile = <file_path>`;
    - the private key for the server certificate, which may be the same file as the `ssl.certfile`.
  - `ssl.cacertfile = <file_path>`;
    - the CA certificate that signed the server certificate.

The configuration will start a HTTPS listener, and any HTTP client will be able to send any supported HTTP request via TLS using that listener.

For the PB interface, it is not possible to enable TLS in isolation without [adding further security measures](#enabling-security-and-restricting-source).  The configuration of file paths to certificate and key files is required as a prerequisite for applying those measures.  No independent listener is used for PB when security is enabled, the standard listener will expect TLS negotiation if and only if security is enabled.

> Riak does not support any automated certificate management, or notification on pending certificate expiry.

### Enabling Security and Restricting Source

To provide further security, the Riak security mechanism should be enabled. This is not possible through configuration; it must be enabled via the command line:

```console
riak admin security enable
```

This is a cluster-wide setting, and will change the behaviour across the cluster with almost immediate effect.  Once security is enabled, any request to Riak sent without TLS enablement and a valid username will be blocked.

> Security enablement is not per API, both the HTTP and PB transports are impacted by enabling security, cluster-wide.

If enablement causes unexpected problems, it may be disabled again:

```console
riak admin security disable
```

Prior to Riak 3.4, some HTTP API requests could still be sent to the plain text listener after the enablement of security, and also sent without passing a valid username to the HTTPS listener.  The rest endpoints that remained insecure were: stats, AAE folds and the queue API.  This allowed operational queries to continue unimpaired by the enablement of security.

To preserve the old behaviour, and allow insecure use via HTTP of operational calls when security is enabled, the configuration option in `riak.conf` of `permit_insecure_http_ops = enabled` can be used.

> Although the CLI uses the terms `user` and `password`; these would normally translate to an `application_instance` and `shared_secret` in an actual implementation.  There is no expectation that Riak security should manage the real-world usernames and passwords of operators, developers or application end-users.

Once security is enabled, all requests will need to have a valid `user` and a valid `source`.  There are three types of `source`:

- `trust`;
  - Applies no conditions beyond a source IP address filter;
- `password`;
  - Requires the user provide a valid password, as well as matching on a source IP address filter,
  - Passwords should be assigned via the `riak admin security` CLI, as use of PAM-based authentication is deprecated.
- `certificate`;
  - Supported for **the PB API only**,
  - Requires that the username match the certificate name,
  - By inference requires the session to have included a valid client certificate in the TLS negotiation,
  - The client certificate must be signed by the same CA as the server certificate, the `ssl.cacertfile`.

A very basic setup would be:

```console
riak admin security enable
riak admin security add-user proxy_waf
riak-admin security add-source all 192.168.6.7/32 trust
```

This would permit access to the APIs only from the IP address `192.168.6.7` (this may be the address of a web application firewall, for example), and trust all access from that source as long as the username of `proxy_waf` is provided within the Authorization header.

> In this simple case, this is functionally equivalent to applying an IP filter on the node through a standard filter utility, but it is not the security equal of that measure.  An IP filter would prevent connections being made from an unauthorised host, whereas the Riak security control allows connections and requests to be sent, but blocks requests during the processing of those requests; which presents a broader attack surface.

On the PB API, a stronger level of security could be applied with:

```console
riak admin security enable
riak admin security add-user app.acme.org
riak-admin security add-source all 192.168.8.0/24 certificate
```

This would permit access from the whole of the network `192.168.8.0/24` (this may be a network hosting application instances allowed to send Riak requests) for any application instance with a valid certificate as long as the certificate name matches `app.acme.org`.

> In this case, this is functionally equivalent to requiring TLS mutual authentication on the PB API, but it is not the security equal of that measure.  A connection would still be accepted from any IP address, and an unauthenticated TLS negotiation allowed; at this stage the PB API will only accept an authentication request, and this will now only work if the IP address is valid and the certificate matches.

There are a number of options around the configuration of security sources in Riak, and further information can be found in the [legacy documentation](https://docs.riak.com/riak/kv/latest/using/security/managing-sources/index.html).

> The use of PAM-based authentication is deprecated and may be removed in a future release.

When enabling the use of certificates, the CRL within the configured CA certificate will be checked for every connection attempt.  If there are issues with either the performance of the CRL check, or the reachability of the CRL endpoint; the crl check can be disabled via a hidden `riak.conf` setting - `check_crl = disabled`.

### Granting permissions for specific actions

There are specific actions within the API, to which specific permissions can be granted - restrictions both on the action alone, and constraints on performing the action by bucket type.

The actions supported by permission grants are:

- [`riak_kv.get`](/docs/ObjectAPI.md#http-api-definition---fetch)
- [`riak_kv.put`](/docs/ObjectAPI.md#http-api-definition---store)
- [`riak_kv.delete`](/docs/ObjectAPI.md#http-api-definition---delete)
- [`riak_kv.list_keys`](/docs/OtherAPI.md#the-list-api)
- [`riak_kv.list_buckets`](/docs/OtherAPI.md#the-list-api)
- [`riak_kv.mapreduce`](/docs/OtherAPI.md#the-mapreduce-api)
- `riak_kv.index`;
  - used to control both the [legacy query api](/docs/OtherAPI.md#legacy-query-api) and the [Query API](/docs/QueryAPI.md).

For all other API endpoints, only `source` protection is applied.

> With the PB API, authentication is provided at the start of a connection, and grants are assessed and cached for that connection to be used against each request.  On the HTTP API, each request on a connection is authenticated and has grant checks made independently on a per-request basis.

There are a number of options for the granting of permissions in Riak, and further information can be found in the [legacy documentation](https://docs.riak.com/riak/kv/latest/using/security/basics/index.html).

## Garbage Collection - Reap, Erase and Scheduled Compaction

### Riak KV Eraser and Riak KV Reaper

The `riak_kv_eraser` is a process that receives requests to delete keys, queues those requests, and continuously erases keys from that queue.  Refer to the [API guide for AAE Fold](/docs/OtherAPI.md#aae-fold-api) for information on triggering a `erase_keys` AAE fold to feed the eraser queue.

Likewise the `riak_kv_reaper` process receives requests to delete tombstones, queues those requests, and continuously reaps keys referenced in the queue.  Refer to the [API guide for AAE Fold](/docs/OtherAPI.md#aae-fold-api) for information on triggering a `reap_tombs` AAE fold to feed the reaper queue.

Filters within the AAE folds can be used to select specific key_ranges, or last modified date ranges for the erase or reap process.

When queueing large volumes of changes, note that:

- The number of vnodes per node on which the fold is run will be restricted by the size of the `AF4_QUEUE` if the `dscp` worker strategy is used.  This will lead to a situation where items on the queue will be grouped by vnode, and dequeued in batches containing objects within the same preflist.
- The pace of which items are dequeued and processed is limited by the `tombstone_pause` configuration.  The pause should be increased if the rate of reaps or erases cause pressure within the cluster, or any clusters receiving replicas of the reap/erase events.  The pause can be adjusted at run-time by changing the underlying environment variable.
- In multi-data centre configurations reap events must be specifically configured to be replicated - this is controlled through the `repl_reap` configuration setting.  Otherwise reap jobs must be run separately on each cluster (with reconciliation suspended until the reaps complete).
- Each queue on each node has a limit to the size of reaps or erases it can hold - this is controlled through the `eraser_overflow_limit` and the `reaper_overflow_limit`.  The queue, except for a small number, is held on disk; and so increasing this limit can be achieved without hitting memory constraints.
- As of Riak 3.4, if a reap is dequeued, but the primaries are not all available, then the reap will be acted on all available primaries and an item will be queued to act on the remaining primaries once they are available.
- Large reap jobs should not be queued while cluster change operations are planned on the cluster, or any cluster linked by replication.

Whether reaps are required depends on the `delete_mode` setting of the cluster.

### bitcask merge window

The bitcask backend operates at PUT time as an append-only database.  As bitcask does not provide a sorted view of objects, there is no immediate mutation on the file-system prompted by new object writes, other than the append.  If objects are immutable, there is no activity required to re-organise a bitcask store, it is a purely append-only operation.

If objects change; they are updated, deleted or they expire due to TTL - then bitcask must perform infrequent `merge` operations to update files so that replaced objects no longer consume space on disk.  Bitcask does not orchestrate merge operations so that they do not coincide, and the merge operations may have a significant impact on cluster performance when they are initiated.

If storing mutable objects in Bitcask, then it is important to configure merge windows, windows in which merges are permitted to take place such that either:

- merges take place at different times on different nodes (or locations) so that only a single replica for each partition is impacted by a concurrent merge;
- merges take place outside of peak hours of database usage.

When testing the potential throughput of a bitcask-backed Riak database it is important to test with appropriate levels of mutation, and a realistic configuration of the bitcask merge window.

For information on configuring bitcask merge see the `bitbask.merge` sections [within the bitcask schema file](https://github.com/OpenRiak/bitcask/blob/openriak-3.2/priv/bitcask.schema). 

### leveled compaction high/low hour

The leveled backend is split into two parts:

- the Journal which like bitcask is generally an append-only file-based log of writes in the order they were received;
- the Ledger which stores only the keys index entries and object metadata; and is a log-structured merge tree, where immutable files are periodically merged and re-written to preserve an on-disk ordering of the keys by level.

The ledger compaction is continuous and the backend will enter a `slow offer` state if a backlog of compaction occurs due to excessive write activity.   When in this state, new writes will be slowed by pauses until the backend is up-to-date with its ledger compaction work.

The journal is generally immutable, but periodically compaction runs will be made which will compact a set of contiguous journal files, if the volume of space to be freed by that compaction is considered to be of sufficient value relative to the cost of the action.  Unlike the ledger compaction, the journal compaction is non-blocking; a backlog of compaction work will result in overuse of disk space, not a slowing of the storage system.

The leveled backend makes use of randomness to reduce the probability of overlapping compaction activity.  It is possible to configure a compaction window, however, all volume testing of Riak with a leveled backend is performed with continuous compaction activity.  The Journal compaction is relatively efficient and low-impact in comparison to bitcask merge - it is generally considered a safe practice to run compaction continuously throughout the day.

Although not necessary, the compaction high/low hour can be used to provide a window for compaction to take place, if required, such that either:

- journal compaction will take place at different times on different nodes (or locations) so that only a single replica for each partition is impacted by a concurrent journal compaction;
- journal compaction will take place outside of peak hours of database usage.

For information on configuring leveled journal compaction see the `journal.compaction` sections [within the leveled schema file](https://github.com/OpenRiak/leveled/blob/openriak-3.4/priv/leveled.schema).

Each compaction job will output a log with `log_ref=ic003` that gives the compaction score of the run of files which would yield the most benefit.  This score is calibrated so that `0.0` is the threshold for prompting compaction - greater than 0.0 and the benefit is considered sufficient.  If tracking these scores over time, if the scores are almost always `< 0.0`, then this is an indication that many of the compaction runs are unnecessary.  If the scores are consistently `> 0.0`, and especially if the scores are increasing over time - then more compaction runs will be required in the future to ensure there is efficient recovery of space in the store.

A backlog of compaction work within the ledger can be monitored by tracking leveled logs with `log_ref=p0024`.

### Garbage collecting `.bak` files in leveled

The leveled backend will in some cases store work in progress during compaction, and then find that work in progress orphaned if it is interrupted by a restart before the change can be applied.  At the next restart, within the leveled ledger, such orphaned files will be renamed as `*.bak` files.  Clearing up the history of these orphaned files is a manual process.  It is always safe to delete `*.bak` files, but for extra security some users may prefer to only delete those files unmodified since before the previous start.

The journal may also orphan files, but in Riak 3.4 there is no automated process for detecting such files and renaming them.  They can though be [detected and renamed through operator intervention](https://github.com/martinsumner/leveled/issues/444).

## Data inspection

To understand more about the data being held in the cluster, information can be found using AAE folds. Refer to the [API guide for AAE Fold](/docs/OtherAPI.md#aae-fold-api) for information on triggering data inspection folds - `find_keys`, `find_tombs`, `list_buckets` and `object_stats`.

## Volume and performance testing

The volume and performance testing of databases is challenging because of the number of variable factors relevant to performance tests, which when set incorrectly will result in unrealistic results.  Common factors to consider when building up a database non-functional test suite:

- Objects being inserted should be suitably live-like, especially with regards to;
  - the size of the object,
  - the compression ratio,
  - the number of index entries.
- Unless objects are expected to arrive in key order, volume tests should avoid either writing or reading in key order.
- Read requests should be distributed across the key space in a realistic manner, e.g. 80% of the requests to 20% of the keys.
- Correlation of factors need to be considered;
  - larger objects may be bigger as they're updated more frequently.
- Exceptional items should be randomly included in the test case e.g.;
  - super-size objects,
  - index keys with very high hit match counts.
- Avoid counting `not_found`;
  - A `not_found` operation is not equivalent to a GET, it is extremely low cost operation is Riak,
  - Validate that the test software is reading keys that are actually present.
- Avoid counting inserts, if the application mainly updates;
  - Altering an object has extra costs over inserting objects,
  - The size of the delta in index keys impacts the cost of an update.

Generally tests should be run for days not hours, to ensure that:

- there is a realistic volume of data in the store being tested;
- that testing covers activity in merge windows or compaction periods.

All databases improve short-term performance through deferring work (e.g. batched compaction), avoiding work (e.g. optimising for sequential keys) and caching.  Always examine any performance test report with these factors in mind.  Plan tests to challenge worst-case scenarios, and focus more attention on high-percentile latencies rather than means.

For performance testing Riak [`basho_bench`](https://github.com/OpenRiak/basho_bench) or [rcl-bench](https://github.com/riak-core-lite/rcl_bench) are commonly used.  Note though, that most large-scale Riak users depend on heavy modifications to these tests to create sufficiently realistic scenarios.

## Backup options

Before considering backups, it is worth noting that as a distributed database there is no single commit position that represents a point of truth.  Therefore there is no way to effectively backup at a point, and restore to a point.  The overall state is eventually consistent.

- Riak is designed to operate as a resilient cluster, and also as a broader system of resilience from having multiple clusters that both replicate between each other and have continuous reconciliation to ensure they are in-sync.
- Major changes to the database will often occur in the application, not the database.  The application is in control of the data schema, and hence the migration of objects between schema versions.
- Self-healing is used to handle repair scenarios - i.e. recovering data from peers within the cluster, and the system is designed to perform predictably during the healing process.

Production users of Riak commonly have relatively lightweight backup and recovery strategies when compared to traditional database management systems; eventual consistency allows the global recovery of state without the need to focus on recovering state first back to a point in time.  In general, greater effort is placed into building the resilience of the system, and also the management of change within the application i.e. ensuring the application adopts lazy migration strategies for schema changes that don't require large point-in-time migration events.

If an individual node fails, do not restore an individual node from backup.  It is generally much more efficient and reliable to use [the `repair` process](#reactive-replace) to recover data on a node.  It is not normal practice to keep backups simply for the purpose of restoring individual nodes, even where those nodes may rely on ephemeral disks.

Note that in cloud environments, if an inefficient backup method is chosen (e.g. snapshots of block-service file-system volumes), then backup costs may consume a dominant proportion of overall Riak infrastructure costs.

### Backup - the preferred building block

As part of the replication approach of Riak it is possible to replicate, and reconcile between clusters with different `n_val`s, different node counts, different vnode counts (i.e. ring sizes) and different storage backends.  It is common in Riak production systems to maintain a single-node, `n_val=1` cluster, with a potentially lower ring size, that represents a backup; where that cluster may be in a diverse geographical location to the primary production clusters.

Having a backup cluster may be considered as a backup in itself, or as a staging post from which to take further backups.

The real-time replication process that keeps the backup in-sync uses a queue on the source cluster, and that queue will grow (as small on-disk references to changes) should the sink (i.e. the backup cluster) pause consumption.  After resuming replication, the sink cluster will catch-up on the missing changes from the queue, and when reconciliation is re-enabled that catch-up can be confirmed.  There is flexibility to disconnect a backup cluster, hold it at a point in time, and then in the future reconnect and fast-forward to the current state, then prove that the fast-forward was successful - with automatic resolution of any unexpected deltas.

Backup clusters are not a prerequisite for taking backups, but they can be a flexible and efficient starting point; and in some cases act as an alternative.

#### Leveled - hot backups

If a cluster uses only the leveled backend, a hot backup may be taken across the cluster using the `riak_client:hotbackup/4` function from [the `remote_console`](#remote-console) on any node within the cluster.

There are four inputs to the function required:

- A backup path; all nodes will be required to support the same path, the path cannot be to the current folder in which leveled is running, but the path must be on the same volume as the current data path (e.g. you could use `<PLATFORM_DATA_DIR>/backup` as a backup to `<PLATFORM_DATA_DIR>/leveled`).
- The `n_val` of the cluster.
- The coverage plan `n_val` of the cluster; to prompt a backup on all vnodes concurrently these two results should match.  It is possible to backup only one copy of the data i.e. by setting the `n_val` to 3 and the coverage `n_val` to 1. It is, though, easier to understand and reason about the result of the backup if the cluster uses the same `n_val` for all bucketsm, and the coverage plan `n_val` is set to that `n_val`.
- A client; e.g. a `C` where `{ok, C} = riak:local_client()`.

The backup at each vnode backend will first:

- roll the active journal file, and start a new empty active journal file, so that the Journal of the leveled store is now an entirely immutable set of files;
- take a snapshot of the store, and return it to the controlling process (so that the vnode is free to continue its work).  This snapshot process is an extremely fast in-memory process that spawns a new snapshot "Inker" (the managing process for the Journal), which has the same knowledge of the current Inker of the file structure of the Journal.

The actual backup is then called on the snapshot:

- It will write the Journal manifest (the data structure that defines the list of file references that represents the Journal) to the backup path;
- It will then for each file within that Journal manifest write a hard-link to a new file in the backup path;
- The snapshot will then close, and any pending changes held in the active Journal because of the snapshot will be released.

It is important to note that there will be minimal impact on the disk footprint within the volume from taking the backup.  The hard-link only requires the backup partition to grow when the files are mutated - but the journal files are not mutated, they are immutable.

Because the active journal file was "rolled" at the start of the process, new writes to the database will go into a new active journal that is not linked to the backup directory.  It is only when journal compaction is run, that there may be a disk-space impact from the existence of the backup.  If journal compaction compacts a set of files it will re-write a new set of files (that are not linked to the backup), and then delete the old files.  If a backup link still exists for those deleted files, the space will now not be reclaimed due to those hard links.

The best practice for copying the hot backup to an alternative location, should that be required, is not defined.  There are example solutions, such as [the S3 sync](https://github.com/OpenRiak/leveled-hotbackup-s3-sync) project which may provide a potential approach.  The S3 sync project is particularly interesting, as before copying the Journal files to S3 it creates "hints" files so that it is possible to read individual objects in the back using S3 commands - without requiring the backup to be restored.  Other standard solutions may be used (e.g. `rsync` to offline the backup).

#### Leveled - restore a backup

If the database is stopped, and the contents of the `data/leveled` folders replaced by the content of the `data/backup` folders, then when Riak is restarted each vnode on each node will [rebuild the leveled ledger](#repair-an-individual-leveled-store), as the ledger is not part of the backup. The cluster will then return to the same distributed consensus as when the backup was taken (given the constraint that the backup coverage plan took time to be distributed).

#### Bitcask - backups

There is no Riak-managed solution for backing up bitcask backends.  Note though, that other than for bitcask merge each backend consists of immutable files and a single active, append-only file.

The tested mechanism for backing up a bitcask store, requires the node to be stopped for the backup to be taken.

#### Backup - ring folder, and cluster metadata

As well as the storage backend data folder, a Riak node also stores data in a ring folder, and in a cluster metadata folder - with both found in the `platform_data_dir` with a standard configuration.  Backing up these folders is critical to the recovery should all nodes in the cluster be lost.  They are required for the cluster to understand the distribution of data.  The restored data alone, without this metadata, will be inaccessible.

## Advanced - troubleshoot via the Erlang VM

For more advanced troubleshooting, [the `remote_console`](#remote-console) can be used to access specialist troubleshooting tools.

### Recon

Riak 3.4 includes the recon library, which is primarily useful for troubleshooting memory issues within the Erlang VM.  For guidance on using the library see the [documentation](https://ferd.github.io/recon/overview.html) and the [related book](https://www.erlang-in-anger.com/).

Note that Riak 3.4 uses an OTP version that will free memory from shared carriers using `MADV_FREE` not `MADV_DONTNEED`; and this may lead to false reporting of memory usage of the `beam` by the operating system - some kernel stats packages may not report `MADV_FREE` memory as having been returned until it is required to use it, creating the misleading impression of a memory leak.  Check kernel documentation to be clear on how to correctly monitor when systems use `MADV_FREE`.  [Future Riak versions will switch to enforcing `MADV_DONTNEED`](https://github.com/OpenRiak/riak/issues/20).

The [riak_kv_util module](https://github.com/OpenRiak/riak_kv/blob/openriak-3.4/src/riak_kv_util.erl) also supports some functions helpful for memory analysis, and these functions may be called via `remote_console`:

- `top_n_binary_total_memory/1`
- `summarise_binary_memory_by_initial_call/1`
- `top_n_process_total_memory/1`
- `summarise_process_memory_by_initial_call/1`

All these functions take a single argument N (the N in Top N), though the `summarise` functions can also be directly passed the output of the related top_n function, so that a recalculation of the Top N is not required.

### Microstate accounting

To examine the spread of CPU-related work by scheduler (there should be one standard erlang scheduler for each CPU core), microstate accounting may be used.  The [erlang documentation](https://www.erlang.org/doc/apps/runtime_tools/msacc.html) gives basic information on analysing the output, but note that the functionality of microstate accounting may vary significantly between OTP releases.

To alter the configuration on the Erlang VM to adjust the operation of schedulers, see the `erlang.schedulers` options within the [Riak schema file](https://github.com/OpenRiak/riak/blob/openriak-3.4/priv/riak.schema).  It is recommended to seek expert advice, and run realistic performance test exercises before adjusting any default settings.

### Eprof

Within the Riak development process testing with eprof profiling is enabled to discover on which functions CPU is used.  For more information on [eprof see the erlang documentation](https://www.erlang.org/doc/apps/tools/eprof.html).  There exists a helper function in the [riak_kv_util module](https://github.com/OpenRiak/riak_kv/blob/openriak-3.4/src/riak_kv_util.erl) `profile_riak/1` which takes an argument N ms, and will profile riak for N ms.

Note with eprof:

- Riak, especially with the leveled backend, uses a huge amount of (lightweight Erlang) processes both temporary and permanent.  Profiling over all processes may fail, especially when profiling for longer periods.
- There may be measurement effects when profiling functions which respond per call in `< 0.1 microseconds` (i.e. the impact of the function call may be proportionally inflated by the cost of measurement).
- Profiling may record the time spent waiting in receive loops as processing time (e.g. `gen_server:loop/7` may appear to have a processing overhead which is in fact mainly wait time).

### Tracing with dbg

Erlang has a [tracing utility](https://www.erlang.org/doc/apps/runtime_tools/dbg_guide) which may be useful for tracing function calls within a system, and to understand the path taken to reach certain function calls.

Note that the output from running `dbg` will by default be sent to `erlang.log` files and potentially write a huge volume of information to these log files very rapidly.  When indexing log files, this may impact licensing and CPU constraints in the indexing system.