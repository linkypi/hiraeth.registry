package cluster

import (
	"encoding/json"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core/slot"
)

// MetaData Refer to the implementation of Redis to map machines and slots
type MetaData struct {
	LeaderId        string                     `json:"leaderId"`
	Term            int                        `json:"term"`
	ExpectedNodeMap map[string]config.NodeInfo `json:"expectedNodeMap"`
	ActualNodeMap   map[string]config.NodeInfo `json:"actualNodeMap"`
	ActualNodes     []config.NodeInfo          `json:"actualNodes"`

	State         State                `json:"state"`
	NodeConfig    config.NodeConfig    `json:"nodeConfig"`
	ClusterConfig config.ClusterConfig `json:"clusterConfig"`

	// Data sharding info, record the shard info stored on each machine
	Shards map[string]slot.Shard `json:"shards"`
	// Replica sharding info, record the shard info replicated on each machine
	Replicas   map[string][]slot.Shard `json:"replicas"`
	CreateTime string                  `json:"createTime"`
}

func (s MetaData) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"leaderId":        s.LeaderId,
		"term":            s.Term,
		"expectedNodeMap": s.ExpectedNodeMap,
		"actualNodeMap":   s.ActualNodeMap,
		"actualNodes":     s.ActualNodes,
		"state":           s.State,
		"nodeConfig":      s.NodeConfig,
		"clusterConfig":   s.ClusterConfig,
		"shards":          s.Shards,
		"replicas":        s.Replicas,
	})
}
