# Riak KV

![Riak KV OpenRiak Status](https://github.com/OpenRiak/riak_kv/actions/workflows/erlang.yml/badge.svg?branch=openriak-3.4)

## Overview

Riak KV is an open source Erlang application that is distributed using the [riak_core](https://github.com/OpenRiak/riak_core) Erlang library. Riak KV provides a key/value datastore and features MapReduce, lightweight data relations, and several different client APIs.

## OTP version support

Riak is built on top of the [Erlang/OTP platform](https://github.com/erlang/otp).  Supported versions for this release are:

![OTP Recommended](https://img.shields.io/badge/OTP_Recommended_Version-_OTP_26_-green)

![OTP Supported](https://img.shields.io/badge/OTP_Backwards_Compatible-_OTP_24_-blue)

For later OTP versions, an alternative `openriak-<release>` branch will be required.  See [the roadmap discussion](https://github.com/orgs/OpenRiak/discussions/19) for further details.

## Quick Start

You should have [Erlang/OTP 26](http://erlang.org/download.html) to compile and run this version Riak KV. The [Riak application](https://github.com/OpenRiak/riak) is the parent application for Riak KV, providing a set of scripts to build, package, deploy and run a Riak KV store.

## Quick Docs

Overview documentation on is available in this repository.  The documentation is split into the following pages:

- [Initial Design Decisions](/docs/InitialDesignDecisions.md);
- [Install and Start Guide](/docs/InstallAndStartGuide.md);
- [Building and then Scaling a Cluster](/docs/BuildAndScaleClusterGuide.md);
- [Replication Guide](/docs/ReplicationGuide.md);
- [Object API](/docs/ObjectAPI.md);
- [Query API](/docs/QueryAPI.md);
- [Other API](/docs/OtherAPI.md);
- [Operations and Troubleshooting Guide](/docs/OperationsAndTroubleshootingGuide.md);
- [Riak Theory Guide](/docs/RiakTheoryGuide.md).

An [online HTML version of this overview is available](https://openriak.github.io/riak_kv/).

## Discussions

For discussions on Riak development see https://github.com/orgs/OpenRiak/discussions.  For direct contact with the OpenRiak development community please use the `open-riak` channel on the [Slack channel for the Erlang Ecosystem Foundation](https://erlef.org/slack-invite/erlef). 

## Testing

```console
./rebar3 do xref, dialyzer, eunit
./rebar3 as test eqc --testing_budget 600
```

For a more complete set of tests, update riak_kv in the full Riak application and run any appropriate [Riak riak_test groups](https://github.com/OpenRiak/riak_test/tree/openriak-3.4/groups)
