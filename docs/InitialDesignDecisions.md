# Initial Design Decisions when Using Riak

When starting with Riak a number of initial design decisions need to be made at the outset of the project.  This is a summary of those decisions, and the factors relevant to making each choice.

The initial design decisions are split into the following categories:
- Database backend
- Ring size
- Anti-entropy and reconciliation
- Replication and data resilience
- Bucket properties
- Mapping data to objects

This document will cover each decision point, and also discuss how to monitor the validity of that choice and transition where a sub-optimal choice has been made.

## Database backend

### Database backend - making a choice

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

Use of multi-backend should generally be avoided.  It may specifically be used ot manage multiple expiry schedules across multiple bitcask backends, but not if anti-entropy requirements exist beyond read-repair or if inter-cluster reconciliation is required (where the use of reaper and eraser is preferred). 

#### Leveled

The leveled backend has the following characteristics and features:
- Pure erlang log-structured-merge (LSM) tree backend, designed and developed specifically for us within Riak.
  - Implementation in Erlang reduces risk as CPU scheduling of all Riak activity is under the control of the Erlang virtual machine.  
- Differs from most other LSM implementations in that values are set-aside in a sequence-ordered journal, and only keys and metadata are placed in key-ordered the LSM-based ledger.  This provides for lower cost and more efficient reads when only keys and metadata is required (which in riak is internally usually the case, even when the external user requires the value).
- Specific internal optimisations to increase efficiency within Riak for Tictac-based method of anti-entropy and inter-cluster reconciliation.
- Supports index entries as well as objects in the key-ordered ledger, to allow full use of the Riak query API.
- Is the priority backend used within the OpenRiak community for both functional and non-functional testing of new releases.
- Generally requires significantly less memory than the total size of all the keys.
  - A fixed overhead of about 20K keys and metadata is kept in memory (per vnode), plus 1% of the keys, plus 2-bytes per key.

#### Bitcask

The bitcask backend has the following characteristics and features:
- A simple low-code, journal based key-value store, originally design and developed by the Riak team; but has been used as a general implementation and model for such stores.
- Written primarily in Erlang, but including around 3K lines of C code to provide the in-memory database of keys.  Access to C-code is generally for short-lived functions, which limits the impact on the scheduling requirements of the BEAM.  The C-code is also extremely stable, and is low-risk in terms of overheads associated with platform evolution, and changes to C standards.
- Supports for pure objects only, no support for index entries and any part of the Riak Query API.
- Requires an out-of-hours merge window to be available and configured, for compaction of mutated objects.  Merging under database load may lead to highly unpredictable performance.
- Requires a separate key-store backend if anti-entropy of inter-cluster reconciliation features are required.
- All keys are kept in-memory, and so sufficient memory is required as the number of keys expand.
- No current support for optimised HEAD requests, which can have significant impact on overall efficiency within Riak.
  - Some implementations of bitcask have been produced with this optimisation, and may be open-sourced in the future. 

#### Eleveldb

The eleveldb backend has the following characteristics and features:
- A heavily-adapted version of the google leveldb store - a LSM tree backend written in C++.  The adapted version is now deprecated, and the original version is subject to only limited maintenance activity.
  - has specific optimisations when compared with google leveldb to: reduce stalling; share and schedule resources where multiple instances operate on the same server; recover disk space following deletion; support automated object expiry.
- Supports secondary index entries, but will not support the full Riak Query API.
- Potentially faster and more efficient than leveled, especially when values are small.
- Moves the majority of CPU and memory management away from the BEAM, to be managed directly within the C++ code.
  - This provides some additional capabilities, in particular the ability to fix the percentage of memory used used across all vnodes on a node.
  - This has some long-term maintenance overheads, which the OpenRiak community will not continue to support after the release of Riak 3.4.

#### In-memory

The in-memory backend has the following characteristics and features:
- Not persisted, all data will be lost on restart (though not that Riak is resilient to the loss of data on an individual node).
- Based on the erlang ETS tables.
- Has crude and imperfect handling of out-of-memory issues to help limit the size of each individual vnode store.
- Supports secondary index entries, but will not support the full Riak Query API.

#### Multi-backend

The multi-backend has the following characteristics and features:
- Allows different data buckets to be mapped to different backends, so that different buckets can utilise the different capabilities of those backends.
- Generally not recommended for production use, unless there are specific issues that cannot otherwise be handled.
  - Behaviour of individual backends is better understood, and subject to greater testing - especially when requiring anti-entropy and inter-cluster reconciliation. 

### Database backend - changing the choice

The database backend configuration is local to a node.  Some cluster-wide behaviour is dependent on the backend configuration being consistent across nodes (i.e. if some nodes use bitcask, but others use leveled - 2i queries will not work within the cluster as the bitcask-based nodes will not support the queries).  However, accounting for this, it is possible to change backend through a "rolling replace" within a cluster - by replacing one or more nodes at a time to a node with a previous backend configuration, to one with a new (where the new configuration has more capabilities then the old).  For example, a multi-backend configuration with bitcask and in-memory backends and parallel-mode tictac aae, can be upgraded to a single leveled backend with native tictac aae by migrating one node at a time using `riak admin cluster replace` - assuming the TTL capability requirement is not being utilised.

## Ring size

### Ring size - making a choice



### Ring size - changing the choice

## Anti-entropy and reconciliation

### Anti-entropy and reconciliation - making a choice

### Anti-entropy and reconciliation - changing the choice

## Replication and data resilience

### Replication and data resilience - making a choice

### Replication and data resilience - changing the choice

## Bucket properties

### Bucket properties - making a choice

### Bucket properties - changing the choice

## Mapping data to objects

### Mapping data to objects - making a choice

### Mapping data to objects - changing the choice

