# millrace-cluster

Raft consensus and a gRPC control plane for a Millrace cluster: cluster-aware
topic creation (partition placement across nodes) and cluster/topic
introspection.

## Why Go

This is the cluster's control plane, not its data plane: gRPC service
definitions, node lifecycle, and light coordination logic. Go's standard
library networking plus a mature, widely-deployed Raft implementation
(`hashicorp/raft` — the same library Consul and Nomad build on) is the
standard toolset for exactly this kind of service, and it keeps this repo
consistent with `millrace-cli`'s language.

## A deliberate scope decision: library Raft, not hand-rolled Raft

The original plan for this repo said "Raft consensus." Implementing Raft
correctly from scratch — leader election, log replication, safety proofs
around term/commit-index invariants — is its own multi-week project, and a
hand-rolled implementation that *looks* like Raft but has a subtle
correctness bug under this timeline would be strictly worse than being
honest about the trade-off. So this repo uses `hashicorp/raft` for
consensus itself, and puts its own engineering effort into what's built on
top: the FSM (deterministic partition placement), the gRPC control plane,
and cluster wiring. That's a defensible, common professional choice — it's
also what `millrace-core`'s custom storage engine and `millrace-columnar`'s
planned custom kernel are *not* doing, i.e. this repo isn't the one
reaching for "implement everything from scratch" as the portfolio flex.

## Architecture

- **`internal/fsm`** — the replicated state machine. It holds topic →
  partition → node assignment *and* cluster membership (node id → broker
  addr), both mutated only through Raft commands (`CreateTopic`, `AddNode`,
  `RemoveNode`) so every replica converges deterministically. `CreateTopic`
  assigns partitions round-robin over whoever is a live member **at that
  moment** — a node added or removed after bootstrap changes placement for
  topics created *after* the change; existing topics never move (see
  "honest limitations").
- **`internal/raftnode`** — wires up one `hashicorp/raft` node: real TCP
  transport, file-backed snapshot store, and either a one-time
  `BootstrapCluster` (static `--peers`, a new cluster) or, if `Peers` is left
  empty, a blank start that waits for an existing leader to add it via
  `raft.AddVoter` — the join path.
- **`internal/control`** + **`proto/control.proto`** — the gRPC `Control`
  service: `CreateTopic` (routes through `raft.Apply`, rejecting with a
  "not leader" error plus the current leader's raft address if called on a
  follower) and `ClusterState` (a direct FSM read; each `PartitionAssignment`
  carries the owning node's `broker_addr` from `--brokers`).
- **`internal/broker`** — a minimal `millrace-core` client (`CreateTopic`,
  `DescribeTopic`) and `Mirror`, the FSM hook that creates committed topics on
  the node's local broker.
- **`cmd/millrace-cluster`** — the node binary. Bootstrap a new cluster with
  `--node-id`, `--raft-addr`, `--grpc-addr`, `--data-dir`, `--peers`, plus
  optional `--brokers` (`id=millrace-core_addr`, same on every node),
  `--http-addr`, and `--group-session-timeout`. **Or** join an already-running
  one with `--join` (comma-separated `--http-addr`s of existing members)
  instead of `--peers`/`--brokers`, plus `--broker-addr` for this node's own
  broker. A joining node's first move is `GET /cluster/nodes` against
  whichever `--join` address answers first, to seed its own FSM with the
  cluster's *current* membership — the original bootstrap members were never
  themselves logged through Raft (only membership *changes* are), so a fresh
  node has no other way to learn about them.
- **`internal/control/http.go`** — a stdlib-only JSON API for clients that
  shouldn't need gRPC: `GET /route?topic=T&partition=P` →
  `{"node_id","broker_addr","partitions"}` (any node can answer; `partitions` is the topic's partition count), and `POST /topics`
  (leader only; a follower answers 409 "not leader", so try another node).
  `millrace-sdk`'s `RoutedClient` uses it to send each produce/fetch to the
  broker that owns the partition.
- **`internal/groups`** + `POST /groups/{join,heartbeat,leave}` — the
  consumer-group coordinator. A `(topic, group)`'s partitions are shared
  fairly (within one of each other) across its live members, **sticky**: a
  join or leave only moves the partitions it has to (freed from a member who
  left, or taken from whoever is over their fair share), not a full
  from-scratch reshuffle. Every membership change bumps a generation; a member
  silent for `--group-session-timeout` (default 10s) is evicted. `join` returns `{member, generation, partitions}`; `heartbeat`
  returns the same (or 404 if the member is unknown: re-join). Leader only,
  409 otherwise. `millrace-sdk`'s `GroupConsumer` drives it. `GET
  /groups?topic=T` lists the topic's groups that have live members — each
  group's generation and every member's partitions — which is what
  `millrace-cli groups` and the dashboard's members table show.
  `POST /groups/commit` is the **fence**: it forwards a commit to the
  partition's broker only if the member is alive *and* the partition is
  currently assigned to it, answering 404 (evicted: re-join) or 403 (no longer
  yours) otherwise.
- **Dynamic cluster membership**: `GET /cluster/nodes` (any node answers —
  membership, generation-free) → `{"leader_id","nodes":[{"node_id","broker_addr"}]}`.
  `POST /cluster/join {"node_id","raft_addr","broker_addr"}` (leader only, 409
  otherwise) calls `raft.AddVoter` then commits an `AddNode` FSM command, so
  the new node is both a real Raft voter *and* known to every replica's
  placement logic. `POST /cluster/leave {"node_id"}` is the mirror
  (`raft.RemoveServer` + `RemoveNode`). `millrace-cli cluster-nodes` /
  `cluster-leave` wrap these; joining is done by *starting* a new node with
  `--join`, not through the CLI.

## Honest limitations (current state)

- **Group membership is soft state on the Raft leader.** Heartbeats are not
  replicated (they'd swamp the log). After a leader change the new leader
  knows no members, so each gets 404, re-joins under its old id and resumes
  from its broker-side commits — a brief rebalance, no lost progress, but
  possible re-delivery. Groups are never garbage-collected, and assignment
  balances by count only — it doesn't know which partitions are expensive to
  re-warm, so "sticky" means "moves the fewest partitions", not "moves the
  cheapest ones".
- **Commit fencing has limits.** It covers commits sent through
  `POST /groups/commit` (what `GroupConsumer` uses); brokers know nothing about
  membership, so a client that commits to a broker directly (`Consumer(group=…)`,
  `millrace-cli commit`) is not fenced. The check and the forward to the broker
  are not atomic, so an eviction in that instant can still let one commit
  through. Every fenced commit takes an extra hop through the leader. There is
  no per-generation check — the invariant enforced is "only the partition's
  current owner may commit".

- **Membership is dynamic; rebalancing is not.** A node can join a running
  cluster (`--join`) or be removed (`millrace-cli cluster-leave`) without
  restarting anyone else — tested end to end two ways: directly against
  `raftnode.Start`/`control.Server` (`internal/raftnode/join_test.go`, 3 real
  HTTP servers + 3 real Raft nodes) and through the actual compiled
  `millrace-cluster.exe` binary's `--join` flag
  (`millrace-cli`'s `TestCLIClusterNodesJoinAndLeave`). But joining or leaving
  only changes placement for topics created *afterwards* — an existing
  topic's partitions never move, so removing a node doesn't relocate the data
  it was holding (there is none to relocate: see `millrace-core`'s single-copy
  limitation below). A join/leave right after the *other* just committing can
  race the same way a produce right after `create-topic` can (see below);
  retry.
- **A real bug found building this, worth naming:** the join/discovery HTTP
  calls in `cmd/millrace-cluster` (`fetchClusterNodes`, `joinCluster`) were
  first written with Go's zero-value `http.Client` — which has **no**
  timeout — while every other HTTP client in this whole project (`millrace-cli`'s
  `clusterDo`, `millrace-sdk`'s `RoutedClient`, the dashboard's `coreClient.ts`)
  already sets one explicitly. Under test, a join occasionally landed on a
  target that was momentarily slow (mid-election), and the unbounded client
  just hung — no error, no retry, the whole test process wedged until Go's
  own test-timeout panic. Caught by running the new join test in a loop (10
  runs), not by a single green run. Fixed with a package-level
  `http.Client{Timeout: 5 * time.Second}`.
- **In-memory Raft log/stable store.** A restarted node starts with a
  clean slate rather than replaying its own prior log (it still catches up
  by replication from the live quorum). Disk-backed stores
  (`raft-boltdb`) are a documented fast-follow, not implemented here.
- **No auth/TLS** on either the Raft transport or the gRPC control plane —
  matches the trusted-network scope of `millrace-core` at this stage.
- **Ownership is fixed at topic creation.** With `--brokers`, every node
  creates each committed topic on its own local `millrace-core` via
  `CreateTopicOwned`, listing only the partitions the FSM assigned to it; the
  broker then rejects produce/fetch for the rest, and `/route` tells clients
  where each partition lives. There is no rebalancing: moving a partition
  means a new assignment plus data movement, neither of which exists. A broker
  with no `--brokers` entry (or a topic made with plain `CreateTopic` directly
  on it) still serves everything. Unowned partitions still preallocate a
  segment on every broker. Mirroring is async and not retried if a broker is
  down, so a produce right after `POST /topics` can briefly fail with
  "unknown topic"; topics restored from a Raft snapshot are not re-mirrored;
  the Compose file doesn't run brokers yet.

## Running a 3-node cluster

### With Docker Compose (real, isolated containers)

```bash
docker compose up --build
```

Brings up `n1`, `n2`, `n3` on a compose network, each bootstrapped with
the same static 3-node configuration. gRPC control planes are published on
`localhost:8001`, `8002`, `8003`.

**Not yet verified end to end:** the image builds, but Docker Desktop's engine
would not start in the environment this was developed in, so `up` has never
been run. The Raft behaviour it would demo is what the `go test` below proves.

### Locally (three processes, one machine)

```bash
PEERS=n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003

go run ./cmd/millrace-cluster --node-id=n1 --raft-addr=127.0.0.1:7001 --grpc-addr=127.0.0.1:8001 --data-dir=./data/n1 --peers=$PEERS
go run ./cmd/millrace-cluster --node-id=n2 --raft-addr=127.0.0.1:7002 --grpc-addr=127.0.0.1:8002 --data-dir=./data/n2 --peers=$PEERS
go run ./cmd/millrace-cluster --node-id=n3 --raft-addr=127.0.0.1:7003 --grpc-addr=127.0.0.1:8003 --data-dir=./data/n3 --peers=$PEERS
```

Add a 4th node to that running cluster without touching n1–n3:

```bash
go run ./cmd/millrace-cluster --node-id=n4 --raft-addr=127.0.0.1:7004 --grpc-addr=127.0.0.1:8004 \
  --http-addr=127.0.0.1:9004 --data-dir=./data/n4 \
  --join=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
```

(this needs each of n1–n3 also started with an `--http-addr`, since `--join`
and `--brokers`/routing both go through that API). Remove one later with
`millrace-cli cluster-leave --node-id=n2 --cluster=...` — **include the node
being removed in that address list too**: it's the current leader as often
as any other node, and `cluster-leave` (like every other write) needs to
reach the leader specifically.

Tried by hand against a real 4-node cluster: joining n4 briefly destabilized
leadership (quorum just grew from 3 to 4; this repo's election timeouts are
tuned to ~200ms for fast tests, which trades some churn tolerance for that
speed) — `cluster-leave` right after the join failed twice with "not
leader" before a couple of seconds' wait let it settle, then it worked. Not
a bug, but a real timing characteristic worth knowing before you script
against `--join` immediately followed by a write: retry, the same way the
automated tests do.

## Tests (all real, all passing)

```bash
go test ./... -v
```

`internal/raftnode/failover_test.go` is the MVP proof this repo was scoped
around: it brings up **three real `hashicorp/raft` nodes** (real TCP
transports on localhost, real file snapshot stores — not mocks), waits for
a real leader election, applies a `CreateTopic` command and confirms it
replicates to every node, then **kills the leader process's Raft instance**
and confirms the two survivors elect a new leader and keep accepting and
replicating writes.

Last real run: `ok millrace-cluster/internal/groups 0.160s`,
`ok millrace-cluster/internal/raftnode 1.817s` — leader elected in ~200ms,
failover re-election in ~130ms, both within this repo's tuned (sub-second)
timeouts. The join test was also run 10x in a loop (not just once) after the
HTTP-timeout fix above, to make sure it was actually fixed and not just
lucky: 10/10.

`internal/raftnode/wired_test.go` covers the broker wiring: three real Raft
nodes, each with its own **real compiled `millrace-core` subprocess** (built
in `../millrace-core`; the test skips if it isn't), commit a `CreateTopic`
through Raft and assert it appears — with the right partition count — on
all three brokers. The HTTP routing API is tested end to end from
`millrace-sdk` (`tests/test_cluster.py`): three real cluster processes + three
real brokers, records produced through `RoutedClient`; then every broker is
queried directly, asserting the owner has the record and every other broker
rejects both a fetch and a produce for that partition ("not owned").

`internal/groups/groups_test.go` unit-tests the coordinator on a fake clock:
generation bumps, exact fair-share splits, eviction after the timeout,
re-join under the old id, leave, group/topic isolation, more members than
partitions, `Describe` (only groups with live members, sorted, one topic),
`Authorize` (owner allowed; stranger, evicted member and a partition lost to a
rebalance all refused; a commit alone doesn't keep a session alive), and
stickiness itself: after a 3-way split, one member leaving moves only its own
partitions — the other two members' assignments are asserted unchanged. The whole group flow is tested end to end from `millrace-sdk`
(`test_consumer_group_splits_partitions_and_rebalances`) against three real
cluster processes and three real brokers — see that README.

`internal/raftnode/join_test.go` is dynamic membership's own proof: 2 real
Raft nodes bootstrap and take a topic; a 3rd starts blank (no `--peers`),
seeds itself via a real `GET /cluster/nodes`, and joins via `POST
/cluster/join`; every node's own view of membership is polled independently
(not just the leader's) until it shows all three; a topic created *after*
the join lands partitions on the new node — the behavioural proof, not just
a membership list. Then a leave: the departed node drops out of every
survivor's view independently, and the *next* topic avoids it.
`millrace-cli`'s `TestCLIClusterNodesJoinAndLeave` repeats the same join →
placement → leave → placement sequence through the **actual compiled
`millrace-cluster.exe` binary's `--join` flag** and the `cluster-nodes`
/ `cluster-leave` commands — the thing an operator would actually type,
not just the internal Go API.

## Wire/API formats

`proto/control.proto` is the gRPC contract; generate the Go bindings with:

```bash
protoc --proto_path=proto \
  --go_out=. --go_opt=module=millrace-cluster \
  --go-grpc_out=. --go-grpc_opt=module=millrace-cluster \
  proto/control.proto
```
