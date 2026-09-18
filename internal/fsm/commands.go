package fsm

import "encoding/json"

type CommandType string

const CommandCreateTopic CommandType = "create_topic"

// Command is what actually goes into the Raft log: a JSON-encoded,
// deterministic instruction every node applies identically.
type Command struct {
	Type        CommandType         `json:"type"`
	CreateTopic *CreateTopicCommand `json:"create_topic,omitempty"`
}

func (c Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

type CreateTopicCommand struct {
	Name          string `json:"name"`
	NumPartitions uint32 `json:"num_partitions"`
}
