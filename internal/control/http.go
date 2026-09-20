package control

import (
	"encoding/json"
	"net/http"
	"strconv"

	pb "millrace-cluster/internal/controlpb"
)

// HTTPHandler is a stdlib-only JSON face for clients that shouldn't need a
// gRPC stack just to ask "where does this partition live?".
//
//	GET  /route?topic=T&partition=P  -> {"node_id": ..., "broker_addr": ..., "partitions": <count in T>}
//	POST /topics {"name":..,"partitions":N} -> 201, or 409 {"error": ...}
//	     (409 includes "not leader" -- POST to another node)
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
		addr, ok := s.Brokers[node]
		if !ok {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "no broker configured for node " + node})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"node_id": node, "broker_addr": addr, "partitions": len(t.Partitions)})
	})

	mux.HandleFunc("POST /topics", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Name       string `json:"name"`
			Partitions uint32 `json:"partitions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		resp, err := s.CreateTopic(r.Context(), &pb.CreateTopicRequest{Name: body.Name, NumPartitions: body.Partitions})
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

	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
