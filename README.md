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

- **`internal/fsm`** — the replicated state machine. It holds
  topic → partition → node assignment and applies `CreateTopic` commands
  deterministically: partitions are assigned round-robin over a **static**
  node-ID list every node was started with identically, so placement
  converges to the same answer on every replica without node membership
  itself needing to go through the replicated log.
- **`internal/raftnode`** — wires up one `hashicorp/raft` node: real TCP
  transport, file-backed snapshot store, and a one-time `BootstrapCluster`
  using the full static `--peers` configuration.
- **`internal/control`** + **`proto/control.proto`** — the gRPC `Control`
  service: `CreateTopic` (routes through `raft.Apply`, rejecting with a
  "not leader" error plus the current leader's raft address if called on a
  follower) and `ClusterState` (a direct FSM read; each `PartitionAssignment`
  carries the owning node's `broker_addr` from `--brokers`).
- **`internal/broker`** — a minimal `millrace-core` client (`CreateTopic`,
  `DescribeTopic`) and `Mirror`, the FSM hook that creates committed topics on
  the node's local broker.
- **`cmd/millrace-cluster`** — the node binary: `--node-id`, `--raft-addr`,
  `--grpc-addr`, `--data-dir`, `--peers`, plus optional `--brokers`
  (`id=millrace-core_addr`, same on every node), `--http-addr`, and
  `--group-session-timeout`.
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

- **Static membership.** The cluster's node set is fixed at bootstrap via
  `--peers`, identical on every node. There's no `Join`/`Leave` RPC and no
  rebalancing on membership change yet — killing a node triggers Raft
  failover (a new leader is elected, writes keep flowing) but does **not**
  move that node's assigned partitions elsewhere. Dynamic membership and
  rebalance-on-change is the natural next milestone for this repo.
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

Last real run: `ok millrace-cluster/internal/raftnode 1.684s` — leader
elected in ~200ms, failover re-election in ~130ms, both within this
repo's tuned (sub-second) timeouts.

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

## Wire/API formats

`proto/control.proto` is the gRPC contract; generate the Go bindings with:

```bash
protoc --proto_path=proto \
  --go_out=. --go_opt=module=millrace-cluster \
  --go-grpc_out=. --go-grpc_opt=module=millrace-cluster \
  proto/control.proto
```
