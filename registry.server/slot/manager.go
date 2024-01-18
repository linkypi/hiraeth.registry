package slot

import (
	"encoding/json"
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/linkypi/hiraeth.registry/server/log"
	"github.com/sirupsen/logrus"
	"math/rand"
)

type Manager struct {
	buckets []*Bucket

	log              *logrus.Logger
	numberOfReplicas int
	selfId           string
	dataDir          string
	shutdownCh       chan struct{}
}

func NewManager(selfId string, dataDir string, numberOfReplicas int, shutdownCh chan struct{}) *Manager {
	return &Manager{
		log:              log.Log,
		buckets:          make([]*Bucket, common.SlotsCount),
		numberOfReplicas: numberOfReplicas,
		selfId:           selfId,
		dataDir:          dataDir,
		shutdownCh:       shutdownCh,
	}
}

func (m *Manager) InitSlotsForStandAlone(nodeId string, clusterConfig config.ClusterConfig, shutdownCh chan struct{}) {

	m.buckets = make([]*Bucket, common.SlotsCount)
	for i := 0; i < common.SlotsCount; i++ {
		b := newBucket(i, nodeId, true, clusterConfig, shutdownCh, m.log)
		m.buckets = append(m.buckets, b)
	}
}

func (m *Manager) AllocateSlots(leaderId string, followerIds []string, clusterConfig config.ClusterConfig) (map[string]common.Shard, map[string][]common.Shard) {

	shards := ExecuteSlotsAllocation(leaderId, followerIds)
	allNodes := append(followerIds, leaderId)
	replicas := ExecuteSlotReplicasAllocation(shards, allNodes, m.numberOfReplicas)

	jsonBytes, _ := json.Marshal(shards)
	replicaBytes, _ := json.Marshal(replicas)
	m.log.Debugf("allocated shards: %s, replicas: %s", string(jsonBytes), string(replicaBytes))

	m.InitSlotsAndReplicas(m.selfId, shards, replicas, clusterConfig)
	m.log.Debug("init metadata success.")

	return shards, replicas
}

func (m *Manager) InitSlotsAndReplicas(nodeId string, shards map[string]common.Shard, replicas map[string][]common.Shard, clusterConfig config.ClusterConfig) {
	shard := shards[nodeId]
	m.buckets = make([]*Bucket, 0, common.SlotsCount)
	for _, segment := range shard.Segments {
		for i := segment.Start; i <= segment.End; i++ {
			b := newBucket(i, shard.NodeId, true, clusterConfig, m.shutdownCh, m.log)
			m.buckets = append(m.buckets, b)
		}
	}

	for _, shard := range replicas[nodeId] {
		for _, segment := range shard.Segments {
			for i := segment.Start; i <= segment.End; i++ {
				b := newBucket(i, shard.NodeId, false, clusterConfig, m.shutdownCh, m.log)
				m.buckets = append(m.buckets, b)
			}
		}
	}
	m.log.Debugf("init slots and replicas success.")
}

func ExecuteSlotsAllocation(leaderId string, followers []string) map[string]common.Shard {
	totalServers := len(followers) + 1
	slotsPerNode := common.SlotsCount / totalServers
	reminds := common.SlotsCount - slotsPerNode*totalServers
	leaderSlotsCount := slotsPerNode + reminds
	index := 0
	slotsAllocation := make(map[string]common.Shard, totalServers)
	for _, node := range followers {
		if node == leaderId {
			continue
		}
		segment := common.Segment{Start: index, End: index + slotsPerNode - 1}
		shard := common.Shard{NodeId: node}
		shard.Segments = make([]common.Segment, 0, 1)
		shard.Segments = append(shard.Segments, segment)
		slotsAllocation[node] = shard
		index += slotsPerNode
	}
	// If there are remaining slots after divisible, they are assigned to the leader node
	shard := common.Shard{NodeId: leaderId}
	shard.Segments = make([]common.Segment, 1)
	segment := common.Segment{Start: index, End: index + leaderSlotsCount - 1}
	shard.Segments[0] = segment
	slotsAllocation[leaderId] = shard
	return slotsAllocation
}

func ExecuteSlotReplicasAllocation(slots map[string]common.Shard, allNodes []string, numberOfReplicas int) map[string][]common.Shard {

	if numberOfReplicas > len(allNodes) {
		numberOfReplicas = len(allNodes) - 1
	}

	// Each node's replica should be stored on which other nodes
	slotsReplicasMap := make(map[string][]common.Shard)
	for nodeId := range slots {
		var replicaNodeId string
		replicas := 0
		tempReplicas := make(map[string][]common.Shard)

		for replicas < numberOfReplicas {
			index := rand.Intn(len(allNodes))
			replicaNodeId = allNodes[index]
			nodeReplicas := tempReplicas[nodeId]

			// Replicas must be placed on different nodes
			if nodeId == replicaNodeId {
				continue
			}

			allocated := false
			for _, a := range nodeReplicas {
				if a.NodeId == replicaNodeId {
					allocated = true
					break
				}
			}

			if allocated {
				continue
			}

			shard := slots[replicaNodeId]
			shards, ok := tempReplicas[nodeId]
			if !ok {
				tempReplicas[nodeId] = make([]common.Shard, 0, 1)
			}
			tempReplicas[nodeId] = append(shards, shard)
			replicas++
		}

		slotsReplicasMap[nodeId] = tempReplicas[nodeId]
	}

	return slotsReplicasMap
}

func (m *Manager) GetSlot(serviceName string) *Bucket {
	bucketIndex := common.GetBucketIndex(serviceName)
	if bucketIndex < 0 || bucketIndex >= common.SlotsCount {
		return nil
	}
	return m.buckets[bucketIndex]
}

func (m *Manager) GetSlotByIndex(bucketIndex int) *Bucket {
	if bucketIndex < 0 || bucketIndex >= common.SlotsCount {
		return nil
	}
	return m.buckets[bucketIndex]
}
