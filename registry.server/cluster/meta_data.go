package cluster

import (
	"encoding/json"
	"errors"
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/linkypi/hiraeth.registry/server/config"
	"strconv"
)

// MetaData Refer to the implementation of Redis to map machines and slots
type MetaData struct {
	ClusterId       uint64                     `json:"clusterId"`
	LeaderId        string                     `json:"leaderId"`
	Term            uint64                     `json:"term"`
	ExpectedNodeMap map[string]config.NodeInfo `json:"expectedNodeMap"`
	ActualNodeMap   map[string]config.NodeInfo `json:"actualNodeMap"`
	ActualNodes     []config.NodeInfo          `json:"actualNodes"`

	State         string               `json:"state"`
	NodeConfig    config.NodeConfig    `json:"nodeConfig"`
	ClusterConfig config.ClusterConfig `json:"clusterConfig"`

	// Data sharding info, record the shard info stored on each machine
	Shards map[string]common.Shard `json:"shards"`
	// Replica sharding info, record the shard info replicated on each machine
	Replicas   map[string][]common.Shard `json:"replicas"`
	CreateTime string                    `json:"createTime"`
}

func (m MetaData) GetReplicaServerIds(index int) ([]string, error) {
	serverId := ""
	for id, shard := range m.Shards {
		for _, segment := range shard.Segments {
			if index >= segment.Start && index <= segment.End {
				serverId = id
			}
		}
	}

	if serverId == "" {
		return nil, errors.New("server id not found, bucket index " + strconv.Itoa(index))
	}

	shards, ok := m.Replicas[serverId]
	if !ok {
		return nil, errors.New("replica shard not found for server " + serverId)
	}

	ids := make([]string, 0, len(shards))
	for _, shard := range shards {
		ids = append(ids, shard.NodeId)
	}
	return ids, nil
}

func (m MetaData) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"leaderId":        m.LeaderId,
		"term":            m.Term,
		"expectedNodeMap": m.ExpectedNodeMap,
		"actualNodeMap":   m.ActualNodeMap,
		"actualNodes":     m.ActualNodes,
		"state":           m.State,
		"nodeConfig":      m.NodeConfig,
		"clusterConfig":   m.ClusterConfig,
		"shards":          m.Shards,
		"replicas":        m.Replicas,
		"clusterId":       m.ClusterId,
		"createTime":      m.CreateTime,
	})
}
