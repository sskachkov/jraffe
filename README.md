# Jraffe
An implementation of the Raft consensus algorithm in Java, backing a key-value store. Built to work through the actual correctness problems that show up in distributed consensus — leader election, log replication, linearizable reads, and the subtle races between them.

<p >
  <img src="assets/jraffe.png" alt="jraffe logo" width="300"/>
</p>


## Verified with Jepsen Maelstrom

The cluster has been run under Maelstrom/Knossos lin-kv workload with simulated network partitions, process pauses, and process kills, and checked for linearizability violations.

A 600-second run at high concurrency with `partition`, `pause`, and `kill` nemeses combined checked 958 keys and found zero linearizability violations.

Two correctness bugs were found and fixed this way:
- a leader stepping down could tell a client its write had definitely failed with a "not leader" error, while the write was actually midflight to a majority and went on to commit under the next leader;
- a freshly-elected leader assumed it was immediately safe to serve reads the moment it won — before its own state machine had actually caught up to entries a previous leader had already committed, even though the election itself was entirely legitimate.

## Core features

- **Core Raft**: leader election with randomized timeouts, log replication, and commit-index advancement that respects the paper's Figure 8 safety rule (a leader only directly commits entries from its own term). A freshly-elected leader appends a no-op entry and won't serve reads until it's confirmed caught up — otherwise it could hand back stale data from before it was reelected.
- **Linearizable reads** via a ReadIndex/lease-read style optimization: reads don't go through the log, but are held until a majority has reconfirmed the leader's leadership since the read was submitted.
- **KV store** with `GET`, `SET`, and `CVAS` (compare-value-and-swap, implemented this way to comply with maelstrom lin-kv workload).
- **Two client-facing paths**:
  - a production-style path (`server` module) — gRPC between Raft peers, a plaintext RESP (Redis-like) protocol for clients.
  - a [Jepsen Maelstrom](https://github.com/jepsen-io/maelstrom) test harness (`maelstrom-server` module) speaking Maelstrom's `lin-kv` workload protocol over stdin/stdout, used to fault-inject the cluster and check linearizability.
- **Deterministic unit tests** in `raft-core`, running multiple `RaftNode`s in-process against each other (no real network or timing dependency), covering election, replication, failover, and specific bugs found via Maelstrom.

## Modules
```
┌──────────────┐                 ┌────────────────────┐
│ RESP Clients │                 │  Jepsen Maelstrom  │
└─────┬────────┘                 └────────┬───────────┘
      │                                   │
      ▼                                   ▼
┌────────┐                       ┌──────────────────┐
│ server │                       │ maelstrom-server │
└──┬──┬──┘                       └──────┬──────┬────┘
   │  │                                 │      │
   │  └────────────────┐    ┌───────────┘      │
   ▼                   ▼    ▼                  ▼
┌────────┐          ┌───────────┐          ┌──────────┐
│  wire  │          │ raft-core │          │ kv-store │
└────────┘          └───────────┘          └──────────┘
```

- **raft-core** — the consensus engine: `RaftNode`, `RaftLog`, `RaftNodeReplicator`, `ReplicationState`. Transport and backend agnostic. No production dependency on any other module — it only pulls in `kv-store` for its own unit tests, to exercise the algorithm against a real state machine.
- **kv-store** — the KV state machine: `SET`/`GET`/`CVAS` commands, their codecs, and the in-memory store. Zero dependencies of its own.
- **wire** — a standalone RESP protocol codec (reader/writer). Zero dependencies, not tied to KV semantics.
- **server** — the production-style deployment: wires `raft-core` and `kv-store` together, gRPC between peers, plaintext RESP (via `wire`) for clients.
- **client** — a minimal RESP client library, depends only on `wire`.
- **maelstrom-server** — wires `raft-core` and `kv-store` into Jepsen Maelstrom's stdin/stdout protocol for linearizability testing.


## Building

```
mvn clean package
```

## Running a cluster

There is a `run-local-cluster.sh` script in the scripts/ folder. Use it to quick launch a cluster on localhost. Without arguments, it will start a three node cluster, or it can accept a string defining each node:

`scripts/run-local-cluster.sh n1:127.0.0.1:8090 n2:127.0.0.1:8091 n3:127.0.0.1:8092 n4:127.0.0.1:8093 n5:127.0.0.1:8094`

To run each node separately, just provide environment variables:

```
NODE_ID=n1 PORT=5001 CLIENT_PORT=6001 PEERS=n1:localhost:5001,n2:localhost:5002,n3:localhost:5003 java -jar server/target/server-*.jar
```

Start one process per node — same `PEERS` list every time, different `NODE_ID`/`PORT`/`CLIENT_PORT` each time. `PORT` is the inter-node Raft (gRPC) port; `CLIENT_PORT` (defaults to `PORT + 1000` if unset) is where clients speak RESP.

## Usage

The RESP layer accepts plain inline commands, `telnet` works directly against a node's `CLIENT_PORT`:

```
telnet localhost 9092
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
STATUS
$161
role=LEADER term=1 nodeId=node3 commitIndex=2
node2:matchIndex=2 nextIndex=3 lastConfirmedDispatchMs=51
node1:matchIndex=2 nextIndex=3 lastConfirmedDispatchMs=51
SET key value1
+OK
GET key
$6
value1
CVAS key value1 value2
+OK
GET key
$6
value2
CVAS key value3 value4
-ERR value mismatch, actual value: value2
QUIT
+OK
Connection closed by foreign host
```

Only the leader accepts `SET`/`GET`/`CVAS` — a follower replies `-ERR not leader leader is <id>` instead (there's no transparent forwarding yet, see "What's missing"). Check `STATUS` on any node to find out who's leader.

Supported commands:

- `SET key value` — write a key.
- `GET key` — read a key; returns `Nil` if unset (same as real Redis's `GET` on a missing key).
- `CVAS key from to` — compare-value-and-swap: sets `key` to `to` only if its current value is `from`.
- `STATUS` — this node's role, term, commit index, and per-peer replication progress.
- `STATS` — Micrometer metrics for the node, formatted as text.
- `QUIT` — close the connection.

## Running under Maelstrom

maelstrom-server module has a `run.sh` script dedicated for running maelstrom workloads: 

```
lein run test -w lin-kv --bin /path/to/maelstrom-server/run.sh --node-count 5 --time-limit 600 --rate 20 --concurrency 4n --nemesis partition,pause,kill --nemesis-interval 5  
```

The `pause` and `kill` nemesis faults are only available on Maelstrom's `main` branch — they aren't in any tagged release binary. Build Maelstrom from source (`git clone` + `lein run` from a checkout of `main`) rather than downloading a release tarball, or drop `pause,kill` from `--nemesis` if you're using a released binary.

See [jepsen-io/maelstrom](https://github.com/jepsen-io/maelstrom) for installation and the full set of nemesis/workload options.

## Scope & Limitations
This is an educational implementation built to explore the hard parts of distributed systems. To keep the focus narrow and centered on algorithm correctness and testing, the following features are omitted:

- **No persistence.** `currentTerm`, `votedFor`, and the log all live in memory only. A process restart currently loses them, which can violate Raft's safety guarantee — a restarted node can grant a second, conflicting vote in a term it already voted in.
- **No log compaction/snapshotting** — the log grows unbounded.
- **No cluster membership changes** — the peer set is fixed at startup.
- **No request deduplication** — a client retrying a timed-out `CVAS` isn't guaranteed idempotent.
- The production (`server`) path lacks the Maelstrom path's transparent leader-forwarding — a non-leader node just returns an error with a hint, instead of forwarding the request itself.

## Development notes
The core consensus algorithm (raft-core) and state machine logic were written by hand to ensure strict correctness and an understanding of the Raft paper. Claude LLM was utilized as a coding assistant to generate the boilerplate adapters, specifically the RESP protocol parser (wire), the Jepsen Maelstrom test harness (maelstrom-server), and most of the unit tests.