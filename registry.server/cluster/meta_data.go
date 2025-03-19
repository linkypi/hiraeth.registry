package cluster

import (
	"encoding/json"
	"errors"
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/linkypi/hiraeth.registry/server/config"
	"strconv"
	"sync"
)

// MetaData Refer to the implementation of Redis to map machines and slots
type MetaData struct {
	ClusterId       uint64            `json:"clusterId"`
	LeaderId        string            `json:"leaderId"`
	Term            uint64            `json:"term"`
	ExpectedNodeMap sync.Map          `json:"expectedNodeMap"`
	ActualNodeMap   sync.Map          `json:"actualNodeMap"`
	ActualNodes     []config.NodeInfo `json:"actualNodes"`

	State         string               `json:"state"`
	NodeConfig    config.NodeConfig    `json:"nodeConfig"`
	ClusterConfig config.ClusterConfig `json:"clusterConfig"`

	// Data sharding info, record the shard info stored on each machine
	Shards map[string]common.Shard `json:"shards"`
	// Replica sharding info, record the shard info replicated on each machine
	Replicas   map[string][]common.Shard `json:"replicas"`
	CreateTime string                    `json:"createTime"`
}

func (m *MetaData) ToJSON() (string, error) {
	// 将 sync.Map 转换为普通 map
	actualNodeMap := make(map[string]config.NodeInfo)
	expectedNodeMap := make(map[string]config.NodeInfo)
	m.ActualNodeMap.Range(func(key, value interface{}) bool {
		actualNodeMap[key.(string)] = value.(config.NodeInfo)
		return true
	})
	m.ExpectedNodeMap.Range(func(key, value interface{}) bool {
		expectedNodeMap[key.(string)] = value.(config.NodeInfo)
		return true
	})
	// 创建临时结构体用于序列化
	temp := struct {
		ClusterId       uint64                     `json:"clusterId"`
		LeaderId        string                     `json:"leaderId"`
		Term            uint64                     `json:"term"`
		ExpectedNodeMap map[string]config.NodeInfo `json:"expectedNodeMap"`
		ActualNodeMap   map[string]config.NodeInfo `json:"actualNodeMap"`
		ActualNodes     []config.NodeInfo          `json:"actualNodes"`
		State           string                     `json:"state"`
		NodeConfig      config.NodeConfig          `json:"nodeConfig"`
		ClusterConfig   config.ClusterConfig       `json:"clusterConfig"`
		Shards          map[string]common.Shard    `json:"shards"`
		Replicas        map[string][]common.Shard  `json:"replicas"`
		CreateTime      string                     `json:"createTime"`
	}{
		ClusterId:       m.ClusterId,
		LeaderId:        m.LeaderId,
		Term:            m.Term,
		ExpectedNodeMap: expectedNodeMap,
		ActualNodeMap:   actualNodeMap,
		ActualNodes:     m.ActualNodes,
		State:           m.State,
		NodeConfig:      m.NodeConfig,
		ClusterConfig:   m.ClusterConfig,
		Shards:          m.Shards,
		Replicas:        m.Replicas,
		CreateTime:      m.CreateTime,
	}

	// 序列化为 JSON 字符串
	jsonBytes, err := json.Marshal(temp)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

func (m *MetaData) DeepCopy() *MetaData {
	// 创建一个新的 MetaData 实例
	copyMeta := &MetaData{
		ClusterId:     m.ClusterId,
		LeaderId:      m.LeaderId,
		Term:          m.Term,
		State:         m.State,
		CreateTime:    m.CreateTime,
		NodeConfig:    m.NodeConfig,
		ClusterConfig: m.ClusterConfig,
	}

	// 复制 ExpectedNodeMap
	copyMeta.ExpectedNodeMap = sync.Map{}
	m.ExpectedNodeMap.Range(func(k, v interface{}) bool {
		copyMeta.ExpectedNodeMap.Store(k, v)
		return true
	})
	// 复制 ActualNodeMap
	copyMeta.ActualNodeMap = sync.Map{}
	m.ActualNodeMap.Range(func(k, v interface{}) bool {
		copyMeta.ActualNodeMap.Store(k, v)
		return true
	})

	// 复制 ActualNodes
	copyMeta.ActualNodes = make([]config.NodeInfo, len(m.ActualNodes))
	copy(copyMeta.ActualNodes, m.ActualNodes)

	// 复制 Shards
	copyMeta.Shards = make(map[string]common.Shard)
	for k, v := range m.Shards {
		copyMeta.Shards[k] = v
	}

	// 复制 Replicas
	copyMeta.Replicas = make(map[string][]common.Shard)
	for k, v := range m.Replicas {
		replicas := make([]common.Shard, len(v))
		copy(replicas, v)
		copyMeta.Replicas[k] = replicas
	}

	return copyMeta
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
