package slot

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"math/rand"
)

type Manager struct {
	buckets []*bucket

	log              *logrus.Logger
	numberOfReplicas int
	selfId           string
	dataDir          string
}

const (
	SlotsCount = 16384
)

func NewManager(log *logrus.Logger, selfId string, dataDir string, numberOfReplicas int) *Manager {
	return &Manager{
		log:              log,
		buckets:          make([]*bucket, SlotsCount),
		numberOfReplicas: numberOfReplicas,
		selfId:           selfId,
		dataDir:          dataDir,
	}
}

func (m *Manager) AllocateSlots(leaderId string, followerIds []string) (map[string]Shard, map[string][]Shard) {

	shards := ExecuteSlotsAllocation(leaderId, followerIds)
	allNodes := append(followerIds, leaderId)
	replicas := ExecuteSlotReplicasAllocation(shards, allNodes, m.numberOfReplicas)

	jsonBytes, _ := json.Marshal(shards)
	replicaBytes, _ := json.Marshal(replicas)
	m.log.Debugf("allocated shards: %s, replicas: %s", string(jsonBytes), string(replicaBytes))

	m.InitSlotsAndReplicas(m.selfId, shards, replicas)
	m.log.Debug("init metadata success.")

	return shards, replicas
}

func (m *Manager) InitSlotsAndReplicas(nodeId string, shards map[string]Shard, replicas map[string][]Shard) {
	shard := shards[nodeId]
	m.buckets = make([]*bucket, SlotsCount)
	for _, segment := range shard.segments {
		for i := segment.start; i <= segment.end; i++ {
			b := newBucket(i, shard.nodeId, true)
			m.buckets = append(m.buckets, b)
		}
	}

	for _, shard := range replicas[nodeId] {
		for _, segment := range shard.segments {
			for i := segment.start; i <= segment.end; i++ {
				b := newBucket(i, shard.nodeId, false)
				m.buckets = append(m.buckets, b)
			}
		}
	}
	m.log.Debugf("init slots and replicas success.")
}

func ExecuteSlotsAllocation(leaderId string, followers []string) map[string]Shard {
	totalServers := len(followers) + 1
	slotsPerNode := SlotsCount / totalServers
	reminds := SlotsCount - slotsPerNode*totalServers
	leaderSlotsCount := slotsPerNode + reminds
	index := 0
	slotsAllocation := make(map[string]Shard, totalServers)
	for _, node := range followers {
		if node == leaderId {
			continue
		}
		segment := Segment{start: index, end: index + slotsPerNode - 1}
		shard := Shard{nodeId: node}
		shard.segments = make([]Segment, 0, 1)
		shard.segments = append(shard.segments, segment)
		slotsAllocation[node] = shard
		index += slotsPerNode
	}
	// If there are remaining slots after divisible, they are assigned to the leader node
	shard := Shard{nodeId: leaderId}
	shard.segments = make([]Segment, 1)
	segment := Segment{start: index, end: index + leaderSlotsCount - 1}
	shard.segments[0] = segment
	slotsAllocation[leaderId] = shard
	return slotsAllocation
}

func ExecuteSlotReplicasAllocation(slots map[string]Shard, allNodes []string, numberOfReplicas int) map[string][]Shard {

	if numberOfReplicas > len(allNodes) {
		numberOfReplicas = len(allNodes) - 1
	}

	// Each node's replica should be stored on which other nodes
	slotsReplicasMap := make(map[string][]Shard)
	for nodeId := range slots {
		var replicaNodeId string
		replicas := 0
		tempReplicas := make(map[string][]Shard)

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
				if a.nodeId == replicaNodeId {
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
				tempReplicas[nodeId] = make([]Shard, 0, 1)
			}
			tempReplicas[nodeId] = append(shards, shard)
			replicas++
		}

		slotsReplicasMap[nodeId] = tempReplicas[nodeId]
	}

	return slotsReplicasMap
}
