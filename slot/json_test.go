package slot

import (
	"encoding/json"
	"testing"
)

func TestJson(t *testing.T) {
	segs := make([]Segment, 0, 3)
	for i := 0; i < 3; i++ {
		segment := Segment{
			start: i * 10,
			end:   i*10 + 9,
		}
		segs = append(segs, segment)
	}
	shard := Shard{
		nodeId:   "1",
		segments: segs,
	}
	bytes, err := json.Marshal(shard)
	if err != nil {
		t.Error(err)
	}
	println(string(bytes))
	shardMap := make(map[string][]Shard)
	shardMap["1"] = []Shard{shard}
	bytes, err = json.Marshal(shardMap)
	if err != nil {
		t.Error(err)
	}
	println(string(bytes))
}
