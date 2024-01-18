package slot

import (
	"encoding/json"
	"github.com/linkypi/hiraeth.registry/common"
	"testing"
)

func TestJson(t *testing.T) {
	segs := make([]common.Segment, 0, 3)
	for i := 0; i < 3; i++ {
		segment := common.Segment{
			Start: i * 10,
			End:   i*10 + 9,
		}
		segs = append(segs, segment)
	}
	shard := common.Shard{
		nodeId:   "1",
		Segments: segs,
	}
	bytes, err := json.Marshal(shard)
	if err != nil {
		t.Error(err)
	}
	println(string(bytes))
	shardMap := make(map[string][]common.Shard)
	shardMap["1"] = []common.Shard{shard}
	bytes, err = json.Marshal(shardMap)
	if err != nil {
		t.Error(err)
	}
	println(string(bytes))
}
