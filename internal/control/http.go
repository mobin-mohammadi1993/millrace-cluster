package control

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/hashicorp/raft"

	"millrace-cluster/internal/broker"
	pb "millrace-cluster/internal/controlpb"
	"millrace-cluster/internal/fsm"
	"millrace-cluster/internal/groups"
)

// HTTPHandler is a stdlib-only JSON face for clients that shouldn't need a
// gRPC stack just to ask "where does this partition live?".
//
//	GET  /route?topic=T&partition=P  -> {"node_id": ..., "broker_addr": ..., "partitions": <count in T>}
//	     (node_id/broker_addr are the partition's current leader)
//	POST /topics {"name":..,"partitions":N,"replication_factor":R} -> 201, or 409 {"error": ...}
//	     (409 includes "not leader" -- POST to another node; replication_factor
//	     0 or 1 means no replication, the pre-existing behavior)
//	POST /partitions/promote {"topic","partition","node_id"} -> {"fenced": bool} (leader only, 409 otherwise)
//	     manual failover: promotes node_id (must be a current replica) to leader for
//	     that partition, fencing (epoch-gated) at both the broker and cluster-metadata
//	     level; also best-effort demotes the old leader if reachable ("fenced": true) --
//	     does not detect a dead leader on its own, and can't fence one it can't reach
//	GET  /groups?topic=T -> {"groups":[{"group","generation","members":[{"member","partitions"}]}]}
//	     (groups with live members only; leader only, 409 otherwise)
//	POST /groups/{join,heartbeat,leave} {"topic","group","member"} -> {"member","generation","partitions"}
//	     (leader only, 409 otherwise; heartbeat of an unknown member is 404: re-join)
//	POST /groups/commit {"topic","group","member","partition","offset"} -> {}
//	     fenced: 404 if the member is unknown (evicted: re-join), 403 if the partition
//	     is not currently assigned to it; otherwise forwarded to the owning broker
//	GET  /cluster/nodes -> {"leader_id","nodes":[{"node_id","broker_addr"}]} (any node answers)
//	POST /cluster/join {"node_id","raft_addr","broker_addr"} -> {} (leader only, 409 otherwise)
//	POST /cluster/leave {"node_id"} -> {} (leader only, 409 otherwise)
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /route", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		partition, err := strconv.Atoi(r.URL.Query().Get("partition"))
		if err != nil || partition < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "need ?topic=&partition=<non-negative int>"})
			return
		}
		t, ok := s.FSM.Topic(topic)
		if !ok || partition >= len(t.Partitions) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown topic or partition"})
			return
		}
		node := t.Partitions[partition].NodeID
		addr, ok := s.FSM.NodeBroker(node)
		if !ok || addr == "" {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "no broker configured for node " + node})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"node_id": node, "broker_addr": addr, "partitions": len(t.Partitions)})
	})

	mux.HandleFunc("POST /topics", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Name              string `json:"name"`
			Partitions        uint32 `json:"partitions"`
			ReplicationFactor uint32 `json:"replication_factor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		resp, err := s.CreateTopic(r.Context(), &pb.CreateTopicRequest{
			Name:              body.Name,
			NumPartitions:     body.Partitions,
			ReplicationFactor: body.ReplicationFactor,
		})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if !resp.Ok {
			writeJSON(w, http.StatusConflict, map[string]string{"error": resp.Error})
			return
		}
		writeJSON(w, http.StatusCreated, resp.Topic)
	})

	// Manual failover: promote a follower to leader. Callers pick the node
	// (typically because the current leader is unreachable); nothing here
	// detects a dead leader automatically. It DOES attempt to fence the old
	// leader (best-effort DemoteToFollower, reported as "fenced" below) --
	// see the README for exactly what that does and doesn't cover.
	mux.HandleFunc("POST /partitions/promote", func(w http.ResponseWriter, r *http.Request) {
		if s.Raft.State() != raft.Leader {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "not leader"})
			return
		}
		var body struct {
			Topic     string `json:"topic"`
			Partition uint32 `json:"partition"`
			NodeID    string `json:"node_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Topic == "" || body.NodeID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "need JSON {topic, partition, node_id}"})
			return
		}
		fenced, err := s.PromotePartition(body.Topic, body.Partition, body.NodeID)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"fenced": fenced})
	})

	mux.HandleFunc("GET /groups", func(w http.ResponseWriter, r *http.Request) {
		if s.Raft.State() != raft.Leader {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "not leader"})
			return
		}
		t, ok := s.FSM.Topic(r.URL.Query().Get("topic"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown topic"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"groups": s.Groups.Describe(t.Name, len(t.Partitions))})
	})

	// Consumer-group coordination (soft state on the leader; see package groups).
	mux.HandleFunc("POST /groups/{op}", func(w http.ResponseWriter, r *http.Request) {
		if s.Raft.State() != raft.Leader {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "not leader"})
			return
		}
		var body struct {
			Topic     string `json:"topic"`
			Group     string `json:"group"`
			Member    string `json:"member"`
			Partition uint32 `json:"partition"` // commit only
			Offset    uint64 `json:"offset"`    // commit only
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Group == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "need JSON {topic, group, member?}"})
			return
		}
		t, ok := s.FSM.Topic(body.Topic)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown topic"})
			return
		}
		n := len(t.Partitions)
		switch r.PathValue("op") {
		case "join":
			writeJSON(w, http.StatusOK, s.Groups.Join(body.Topic, body.Group, body.Member, n))
		case "heartbeat":
			st, err := s.Groups.Heartbeat(body.Topic, body.Group, body.Member, n)
			if err != nil { // unknown member: the caller should re-join
				writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, st)
		case "commit":
			// Fenced commit: forwarded to the partition's broker only if this
			// member is alive and currently owns the partition. (Check-then-forward
			// is not atomic: an eviction in that instant can still slip one through.)
			if int(body.Partition) >= n {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unknown partition"})
				return
			}
			switch err := s.Groups.Authorize(body.Topic, body.Group, body.Member, body.Partition, n); {
			case errors.Is(err, groups.ErrUnknownMember):
				writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
				return
			case err != nil: // groups.ErrNotAssigned
				writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
				return
			}
			addr, ok := s.FSM.NodeBroker(t.Partitions[body.Partition].NodeID)
			if !ok || addr == "" {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "no broker configured for the partition's node"})
				return
			}
			if err := broker.CommitOffset(addr, body.Topic, body.Partition, body.Group, body.Offset); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{})
		case "leave":
			s.Groups.Leave(body.Topic, body.Group, body.Member)
			writeJSON(w, http.StatusOK, map[string]string{})
		default:
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown group operation"})
		}
	})

	// Dynamic cluster membership. A node joins by contacting any existing
	// member here (see cmd/millrace-cluster --join); it needs the leader for
	// this specific call, so a follower answers 409 and the caller tries the
	// next address it knows. GET /cluster/nodes is a direct FSM read, so any
	// node answers -- including a brand-new joiner asking who else to seed from.
	mux.HandleFunc("GET /cluster/nodes", func(w http.ResponseWriter, r *http.Request) {
		_, leaderID := s.Raft.LeaderWithID()
		nodes := s.FSM.Nodes()
		type nodeInfo struct {
			NodeID     string `json:"node_id"`
			BrokerAddr string `json:"broker_addr"`
		}
		list := make([]nodeInfo, 0, len(nodes))
		for id, addr := range nodes {
			list = append(list, nodeInfo{id, addr})
		}
		sort.Slice(list, func(i, j int) bool { return list[i].NodeID < list[j].NodeID })
		writeJSON(w, http.StatusOK, map[string]any{"leader_id": string(leaderID), "nodes": list})
	})

	mux.HandleFunc("POST /cluster/join", func(w http.ResponseWriter, r *http.Request) {
		if s.Raft.State() != raft.Leader {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "not leader"})
			return
		}
		var body struct {
			NodeID     string `json:"node_id"`
			RaftAddr   string `json:"raft_addr"`
			BrokerAddr string `json:"broker_addr"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.NodeID == "" || body.RaftAddr == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "need JSON {node_id, raft_addr, broker_addr?}"})
			return
		}
		// AddVoter also covers a node re-joining at the same id/address (a no-op
		// on the raft side); the FSM upsert below then just re-affirms its broker addr.
		if err := s.Raft.AddVoter(raft.ServerID(body.NodeID), raft.ServerAddress(body.RaftAddr), 0, 5*time.Second).Error(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		cmd := fsm.Command{Type: fsm.CommandAddNode, AddNode: &fsm.AddNodeCommand{NodeID: body.NodeID, BrokerAddr: body.BrokerAddr}}
		if err := s.applyCommand(cmd); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{})
	})

	mux.HandleFunc("POST /cluster/leave", func(w http.ResponseWriter, r *http.Request) {
		if s.Raft.State() != raft.Leader {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "not leader"})
			return
		}
		var body struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.NodeID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "need JSON {node_id}"})
			return
		}
		if err := s.Raft.RemoveServer(raft.ServerID(body.NodeID), 0, 5*time.Second).Error(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		cmd := fsm.Command{Type: fsm.CommandRemoveNode, RemoveNode: &fsm.RemoveNodeCommand{NodeID: body.NodeID}}
		if err := s.applyCommand(cmd); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
