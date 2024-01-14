package slot

import (
	"encoding/json"
)

type Segment struct {
	start int
	end   int
}

type Shard struct {
	nodeId string
	// The shard info stored by the current machine
	segments []Segment
}

func (s Segment) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"start": s.start,
		"end":   s.end,
	})
}

func (s Shard) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"nodeId":   s.nodeId,
		"segments": s.segments,
	})
}
