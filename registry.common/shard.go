package common

import (
	"encoding/json"
)

type Segment struct {
	Start int
	End   int
}

type Shard struct {
	NodeId string
	// The shard info stored by the current machine
	Segments []Segment
}

func (s Segment) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"start": s.Start,
		"end":   s.End,
	})
}

func (s Shard) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"nodeId":   s.NodeId,
		"segments": s.Segments,
	})
}
