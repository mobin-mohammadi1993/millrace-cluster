package fsm

import "encoding/json"

type CommandType string

const (
	CommandCreateTopic CommandType = "create_topic"
	CommandAddNode     CommandType = "add_node"
	CommandRemoveNode  CommandType = "remove_node"
)

// Command is what actually goes into the Raft log: a JSON-encoded,
// deterministic instruction every node applies identically.
type Command struct {
	Type        CommandType         `json:"type"`
	CreateTopic *CreateTopicCommand `json:"create_topic,omitempty"`
	AddNode     *AddNodeCommand     `json:"add_node,omitempty"`
	RemoveNode  *RemoveNodeCommand  `json:"remove_node,omitempty"`
}

func (c Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

type CreateTopicCommand struct {
	Name          string `json:"name"`
	NumPartitions uint32 `json:"num_partitions"`
}

// AddNodeCommand registers a node (and its millrace-core address, "" if it
// has none) in the cluster's membership. Applying it twice for the same
// NodeID is harmless -- an upsert, not an error -- since a joining node
// seeds this locally (see cmd/millrace-cluster) and then receives the same
// fact again via normal log replication.
type AddNodeCommand struct {
	NodeID     string `json:"node_id"`
	BrokerAddr string `json:"broker_addr"`
}

type RemoveNodeCommand struct {
	NodeID string `json:"node_id"`
}
