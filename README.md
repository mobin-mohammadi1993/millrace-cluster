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
  follower) and `ClusterState` (a direct FSM read).
- **`internal/broker`** — a minimal `millrace-core` client (`CreateTopic`,
  `DescribeTopic`) and `Mirror`, the FSM hook that creates committed topics on
  the node's local broker.
- **`cmd/millrace-cluster`** — the node binary: `--node-id`, `--raft-addr`,
  `--grpc-addr`, `--data-dir`, `--peers`, optional `--broker-addr`.

## Honest limitations (current state)

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
- **Cluster ↔ broker wiring is only half-way.** With `--broker-addr`, every
  node creates each committed topic on its own local `millrace-core`
  (`internal/broker`, hooked into the FSM). But: every node's broker gets *all*
  partitions of the topic (core has no "own only partitions 0 and 3"), nothing
  routes or enforces that clients produce to the assigned node, mirroring is
  async and not retried if the broker is down, and topics restored from a
  Raft snapshot are not re-mirrored. The Compose file doesn't run brokers yet.

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
all three brokers.

## Wire/API formats

`proto/control.proto` is the gRPC contract; generate the Go bindings with:

```bash
protoc --proto_path=proto \
  --go_out=. --go_opt=module=millrace-cluster \
  --go-grpc_out=. --go-grpc_opt=module=millrace-cluster \
  proto/control.proto
```
