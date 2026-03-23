---
title: Object API
nav_order: 10
layout : default
---

# Riak Control API

Riak Control API provides an AWS-style set of HTTP requests to enable
authenticated clients to:

* monitor the state of a cluster and perform operations on nodes,
  emulating CLI commands under `riak admin cluster`;
* view backend status details on selected nodes and partitions,
  similar to `riak admin vnode-status`;
* view TictacAAE tree rebuild status (`riak admin tictacaae
  treestatus`);
* manage users and groups (`riak admin security`).

All requests are POSTs, with body as a JSON of the form:

```
{
    "action": ACTION,
    "params": PARAMETER_MAP
}
```
where ACTION is the command name, and PARAMETER_MAP is a map of
parameters, detailed below.

A response will have a JSON object specific to the request under key
`result`, or an error message string under key `error`.


## Cluster monitoring and operations

### ClusterGetStatus
**Permissions required**: cluster\_observer.
#### Parameters
None.

#### Response
Description of the current status of the cluster. As an
example, for a cluster of two devrel nodes with no staged changes and
all transfers completed, the response is as follows:

```
{
   "result" : {
      "current_cluster" : [
         {
            "claimant" : false,
            "is_me" : true,
            "low_mem" : false,
            "mem_erlang" : 91029576,
            "mem_total" : 16694644736,
            "mem_used" : 7158771712,
            "name" : "dev1@127.0.0.1",
            "pending_pct" : 0.5,
            "reachable" : true,
            "replacement" : null,
            "ring_pct" : 0.5,
            "services" : [
               "riak_repl",
               "riak_pipe",
               "riak_kv"
            ],
            "staged_action" : null,
            "status" : "valid",
            "system_info" : {
               "nodename" : "dev1@127.0.0.1",
               "riak_version" : "3.4.0",
               "system_version" : "Erlang/OTP 26 [erts-14.2.4] [source] [64-bit] [smp:8:8] [ds:8:8:10] [async-threads:64] [jit:ns]",
               "uptime" : 2954,
               "uptime_str" : "49 minutes, 14 sec"
            }
         },
         {
            "claimant" : true,
            "is_me" : false,
            "low_mem" : false,
            "mem_erlang" : 77394144,
            "mem_total" : 16694644736,
            "mem_used" : 7158771712,
            "name" : "dev2@127.0.0.1",
            "pending_pct" : 0.5,
            "reachable" : true,
            "replacement" : null,
            "ring_pct" : 0.5,
            "services" : [
               "riak_repl",
               "riak_pipe",
               "riak_kv"
            ],
            "staged_action" : null,
            "status" : "valid",
            "system_info" : {
               "nodename" : "dev2@127.0.0.1",
               "riak_version" : "3.4.0",
               "system_version" : "Erlang/OTP 26 [erts-14.2.4] [source] [64-bit] [smp:8:8] [ds:8:8:10] [async-threads:64] [jit:ns]",
               "uptime" : 2959,
               "uptime_str" : "49 minutes, 19 sec"
            }
         }
      ],
      "down_nodes" : [],
      "final_cluster" : [],
      "staged_changes" : [],
      "transfers" : []
   }
}
```

### ClusterClearPlan
**Permissions required**: cluster\_admin.
#### Parameters
None.
#### Response
On success,
```
{"result": "ok"}
```
On error,
```
{"error": ERROR_STRING}
```

### ClusterCommitPlan
**Permissions required**: cluster\_admin.
#### Parameters
None.
#### Response
On success,
```
{"result": "ok"}
```
On error,
```
{"error": ERROR_STRING}
```


### ClusterStageJoin
**Permissions required**: cluster\_admin.
#### Parameters
```
{"node": NODENAME}
```
where NODENAME is the node to stage for joining the cluster.

#### Response
On success,
```
{"result": "ok"}
```
On error,
```
{"error": ERROR_STRING}
```


### ClusterStageLeave
**Permissions required**: cluster\_admin.
#### Parameters
```
{"node": NODENAME}
```
where NODENAME is the node to stage for leaving the cluster.

#### Response
On success,
```
{"result": "ok"}
```
On error,
```
{"error": ERROR_STRING}
```


### ClusterStageRemove
**Permissions required**: cluster\_admin.
#### Parameters
```
{"node": NODENAME}
```
where NODENAME is the node to stage for removing from the cluster.
#### Response
On success,
```
{"result": "ok"}
```
On error,
```
{"error": ERROR_STRING}
```


### ClusterStageReplace
**Permissions required**: cluster\_admin.
#### Parameters
```
{"node": NODENAME, "with": REPLACEMENT}
```
where NODENAME is the node to be replaced with REPLACEMENT.
#### Response
On success,
```
{"result": "ok"}
```
On error,
```
{"error": ERROR_STRING}
```


### ClusterStageForceReplace
**Permissions required**: cluster\_admin.
#### Parameters
```
{"node": NODENAME, "with": REPLACEMENT}
```
where NODENAME is the node to be force-replaced with REPLACEMENT.
#### Response
On success,
```
{"result": "ok"}
```
On error,
```
{"error": ERROR_STRING}
```


### ClusterDownNode
**Permissions required**: cluster\_admin.
#### Parameters
```
{"node": NODENAME}
```
where NODENAME is the node to down.
#### Response
On success,
```
{"result": "ok"}
```
On error,
```
{"error": ERROR_STRING}
```


### ClusterStopNode
**Permissions required**: cluster\_admin.
#### Parameters
```
{"node": NODENAME}
```
where NODENAME is the node to stop.
#### Response
On success,
```
{"result": "ok"}
```
On error,
```
{"error": ERROR_STRING}
```


### NodeGetAppEnv
**Permissions required**: cluster\_observer.
#### Parameters
```
{"node": NODENAME}
```
where NODENAME is the node to collect application environment
variables on.
#### Response
On success,
```
{"result": APPENV}
```
where APPENV is a proplist of all Erlang applications and their
environments, as a string produced by passing it through
`io_lib:format("~120p\n", [AllAppEnvAsProplist])`.
On error,
```
{"error": ERROR_STRING}
```

### NodePutAppEnv
**Permissions required**: cluster\_admin.
#### Parameters
```
{"node": NODENAME}
```
where NODENAME is the node to collect application environment
variables on.
#### Response
On success,

### NodeGetAdvancedConfig
#### Parameters
None.
#### Response
On success,

### NodePutAdvancedConfig
#### Parameters
None.
#### Response

### NodeRestart
#### Parameters
None.
#### Response
On success,


VnodeGetStatus
TictacaaeGetStatus

SystemGetVersionInfo

SecurityListUsers
SecurityCreateUser
SecurityUpdateUser
SecurityDeleteUser
SecurityListGroups
SecurityCreateGroup
SecurityUpdateGroup
SecurityDeleteGroup
SecurityAddUserGroup
SecurityDeleteUserGroup
SecurityAddUserGrant
SecurityDeleteUserGrant
SecurityAddGroupGrant
SecurityDeleteGroupGrant
SecurityListPermissions
