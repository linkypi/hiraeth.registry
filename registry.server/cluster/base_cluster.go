package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/cluster/network"
	"github.com/linkypi/hiraeth.registry/server/config"
	raft2 "github.com/linkypi/hiraeth.registry/server/raft"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"strconv"
	"sync"
	"time"
)

type BaseCluster struct {
	*network.Manager
	SlotManager *slot.Manager

	State       State
	StateMtx    sync.Mutex
	metaDataMtx sync.Mutex

	Leader      *Leader
	Raft        *raft2.RaftNode
	StartUpMode config.StartUpMode
	Config      *config.ClusterConfig
	NodeConfig  *config.NodeConfig
	ShutDownCh  chan struct{}
	SelfNode    *config.NodeInfo
	notifyCh    chan bool

	lastStateTime time.Time
	ClusterId     uint64
	MetaData      *MetaData

	// all cluster nodes configured in the configuration
	ClusterExpectedNodes sync.Map

	// The actual cluster nodes
	ClusterActualNodes sync.Map

	OtherCandidateNodes []config.NodeInfo
	joinCluster         bool

	lastJoinRequestLeaderId string     // 记录最近一次发送加入集群请求的 Leader ID
	lastJoinRequestTerm     uint64     // 记录最近一次发送加入集群请求的 Term
	joinRequestMtx          sync.Mutex // 保护 lastJoinRequestLeaderId 和 lastJoinRequestTerm 的互斥锁

}

type State int

const (
	NoneState State = iota
	Initializing
	// Active The cluster is active and can be read and written to
	Active
	// Transitioning The cluster is transitioning to a new state
	// In this process, the cluster will resize the data shards, just
	// processes read requests and rejects write requests
	Transitioning

	Down
	// StandBy The cluster is in a standby state and can only be read
	// It's possible that the cluster is electing a leader
	StandBy
	// Maintenance The cluster is in a maintenance state and cannot be read or written
	Maintenance
	Unknown
)

func (s State) ToClusterState() pb.ClusterState {
	switch s {
	case NoneState:
		return pb.ClusterState_NONE_STATE
	case Initializing:
		return pb.ClusterState_INITIALIZING
	case Active:
		return pb.ClusterState_ACTIVE
	case Transitioning:
		return pb.ClusterState_TRANSITIONING
	case Down:
		return pb.ClusterState_DOWN
	case StandBy:
		return pb.ClusterState_STANDBY
	case Maintenance:
		return pb.ClusterState_MAINTENANCE
	default:
		return pb.ClusterState_UNKNOWN
	}
}
func FromClusterState(state pb.ClusterState) State {
	switch state {
	case pb.ClusterState_NONE_STATE:
		return NoneState
	case pb.ClusterState_INITIALIZING:
		return Initializing
	case pb.ClusterState_ACTIVE:
		return Active
	case pb.ClusterState_TRANSITIONING:
		return Transitioning
	case pb.ClusterState_DOWN:
		return Down
	case pb.ClusterState_STANDBY:
		return StandBy
	case pb.ClusterState_MAINTENANCE:
		return Maintenance
	default:
		return Unknown
	}
}
func (s State) String() string {
	names := [...]string{"NoneState", "Initializing", "Active", "Transitioning", "Down", "StandBy", "Maintenance", "Unknown"}
	if s < 0 || s > State(len(names)-1) {
		return "Unknown"
	}
	return names[s]
}

func NewBaseCluster(conf *config.Config, selfNode *config.NodeInfo, slotManager *slot.Manager, shutDownCh chan struct{}) *BaseCluster {
	cluster := BaseCluster{
		StartUpMode: conf.StartupMode,
		joinCluster: conf.JoinCluster,
		Config:      &conf.ClusterConfig,
		NodeConfig:  &conf.NodeConfig,
		ShutDownCh:  shutDownCh,
		SelfNode:    selfNode,
		notifyCh:    make(chan bool, 10),
		SlotManager: slotManager,
		MetaData: &MetaData{
			State: Initializing.String(),
		},
		State: Initializing,
	}
	return &cluster
}

func (b *BaseCluster) SetState(state State) {
	b.StateMtx.Lock()
	defer b.StateMtx.Unlock()
	b.State = state
	b.MetaData.State = state.String()
	common.Debugf("cluster state changed to %s", state)
}

func (b *BaseCluster) setNet(net *network.Manager) {
	b.Manager = net
}

func (b *BaseCluster) RaftExistNode(id string) bool {
	servers := b.Raft.GetConfiguration().Configuration().Servers
	for _, server := range servers {
		if string(server.ID) == id {
			return true
		}
	}
	return false
}

// GetOtherCandidateServers get other candidate servers, exclude self node
func (b *BaseCluster) GetOtherCandidateServers() []*config.NodeInfo {
	var filtered []*config.NodeInfo
	b.ClusterExpectedNodes.Range(func(key, value interface{}) bool {
		node := value.(*config.NodeInfo)
		if node.IsCandidate {
			filtered = append(filtered, node)
		}
		return true
	})

	return filtered
}

func (b *BaseCluster) GetOtherNodes(selfId string) []*config.NodeInfo {
	var filtered = make([]*config.NodeInfo, 0, 8)
	b.ClusterExpectedNodes.Range(func(key, value interface{}) bool {
		node := value.(*config.NodeInfo)
		if node.Id != selfId {
			filtered = append(filtered, node)
		}
		return true
	})
	return filtered
}

func (b *BaseCluster) MapToList(nodes map[string]config.NodeInfo) []config.NodeInfo {
	var result = make([]config.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, node)
	}
	return result
}

func (b *BaseCluster) GetAddresses(nodes []*config.NodeInfo) []string {
	var result = make([]string, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, node.Addr)
	}
	return result
}

func (b *BaseCluster) CopyClusterNodes(nodes map[string]*config.NodeInfo) map[string]config.NodeInfo {
	var result = make(map[string]config.NodeInfo)
	for id, node := range nodes {
		result[id] = *node
	}
	return result
}

func (b *BaseCluster) GetNodeIdsIgnoreSelf(clusterServers []*config.NodeInfo) []string {
	arr := make([]string, 0, len(clusterServers))
	for _, node := range clusterServers {
		if node.Id != b.SelfNode.Id {
			arr = append(arr, node.Id)
		}
	}
	return arr
}

func (b *BaseCluster) getNodesByRaftServers(servers []raft.Server) []*config.NodeInfo {
	var result = make([]*config.NodeInfo, 0, len(servers))
	for _, server := range servers {
		b.ClusterExpectedNodes.Range(func(key, value interface{}) bool {
			node := value.(*config.NodeInfo)
			if node.Addr == string(server.Address) {
				if server.Suffrage == raft.Voter {
					node.IsCandidate = true
				}
				result = append(result, node)
				return false
			}
			return true
		})
	}
	return result
}

func (b *BaseCluster) GetClusterExpectedNodeIds(clusterServers map[string]*config.NodeInfo) []string {
	arr := make([]string, 0, len(clusterServers))
	for _, node := range clusterServers {
		_, ok := b.Connections[node.Id]
		if ok {
			arr = append(arr, node.Id)
		}
	}
	return arr
}

func (b *BaseCluster) RemoveNode(nodeId string) {
	b.ClusterExpectedNodes.Delete(nodeId)
}

func (b *BaseCluster) UpdateRemoteNode(remoteNode *config.NodeInfo, selfNode config.NodeInfo, throwEx bool) error {

	if remoteNode.Id == selfNode.Id {
		msg := fmt.Sprintf("[cluster] the remote node id [%s][%s] conflicts with the current node id [%s][%s]",
			remoteNode.Id, remoteNode.Addr, selfNode.Id, selfNode.Addr)
		//common.Warnf(msg)
		if throwEx {
			panic(msg)
		}
		return errors.New(msg)
	}

	node, ok := b.ClusterExpectedNodes.Load(remoteNode.Id)
	if !ok {
		// it is possible that a node is missing from the cluster address
		// configuration in the node, or it may be a new node
		msg := fmt.Sprintf("[cluster] remote node [%s][%s] not found int cluster servers, add it to cluster.", remoteNode.Id, remoteNode.Addr)
		common.Info(msg)
		b.ClusterExpectedNodes.Store(remoteNode.Id, &remoteNode)
		b.ClusterActualNodes.Store(remoteNode.Id, &remoteNode)
		if remoteNode.IsCandidate {
			b.OtherCandidateNodes = append(b.OtherCandidateNodes, *remoteNode)
		}
	} else {
		nd := node.(*config.NodeInfo)
		if nd.Addr != remoteNode.Addr {
			msg := fmt.Sprintf("[cluster] update remote node info failed, node id exist, but addr not match: %s, %s, %s",
				remoteNode.Id, nd.Addr, remoteNode.Addr)
			common.Error(msg)
			if throwEx {
				panic(msg)
			}
			return errors.New(msg)
		}
		nd.IsCandidate = remoteNode.IsCandidate
		nd.ExternalHttpPort = remoteNode.ExternalHttpPort
		nd.ExternalTcpPort = remoteNode.ExternalTcpPort
		b.ClusterActualNodes.Store(nd.Id, nd)
	}

	common.Infof("[cluster] update remote node info: %s, %s", remoteNode.Id, remoteNode.Addr)
	return nil
}

func (b *BaseCluster) IsLeader() bool {
	return b.Raft.State() == raft.Leader
}

func (b *BaseCluster) IsActive() bool {
	return b.State == Active
}

func (b *BaseCluster) GetLeaderInfoFromRaft() (string, string, uint64) {
	leaderAddr, leaderId := b.Raft.LeaderWithID()
	stats := b.Raft.Stats()
	term, _ := strconv.Atoi(stats[TermKey])
	return string(leaderId), string(leaderAddr), uint64(term)
}

func (b *BaseCluster) GetNodeIdByIndex(index int) (string, error) {
	return common.GetNodeIdByIndex(index, b.MetaData.Shards)
}

func (b *BaseCluster) ForwardRequest(nodeId string, requestType pb.RequestType, syncReplica bool, payload []byte) (*pb.ForwardCliResponse, error) {

	defer func() {
		if err := recover(); err != nil {
			common.Errorf("[cluster] forward request failed, %s", err)
		}
	}()
	cliRequest := pb.ForwardCliRequest{
		ClusterId:   b.ClusterId,
		LeaderId:    b.Leader.Id,
		Term:        b.Leader.Term,
		RequestType: requestType,
		Payload:     payload,
		SyncReplica: syncReplica,
	}
	interRpcClient := b.GetInterRpcClient(nodeId)
	response, err := (*interRpcClient).ForwardClientRequest(context.Background(), &cliRequest)
	if err != nil {
		common.Errorf("failed to forward %s request, %v", requestType.String(), err)
		return nil, err
	}

	if response.ErrorType == pb.ErrorType_None {
		return response, nil
	}
	if response.ErrorType == pb.ErrorType_ClusterStateNotMatch {
		common.Errorf("failed to forward %s request, cluster state not match: %v", requestType.String(), err)
		return nil, errors.New("cluster state not match")
	}
	if response.ErrorType == pb.ErrorType_ClusterIdNotMatch {
		common.Errorf("failed to forward %s request, cluster id not match: %v", requestType.String(), err)
		return nil, errors.New("cluster id not match")
	}
	if response.ErrorType == pb.ErrorType_LeaderIdNotMatch {
		common.Errorf("failed to forward %s request, leader id not match: %v", requestType.String(), err)
		return nil, errors.New("leader id not match")
	}
	if response.ErrorType == pb.ErrorType_TermNotMatch {
		common.Errorf("failed to forward %s request, term not match: %v", requestType.String(), err)
		return nil, errors.New("term not match")
	}
	if response.ErrorType == pb.ErrorType_MetaDataChanged {
		common.Errorf("failed to forward %s request, meta data changed: %v", requestType.String(), err)
		return nil, errors.New("meta data changed")
	}
	common.Errorf("failed to forward %s request, unknown error type: %v", requestType.String(), response.ErrorType)
	return nil, errors.New("unknown error type: " + response.ErrorType.String())
}

func (b *BaseCluster) CheckNodeRouteForServiceName(target string) pb.ErrorType {
	// Double-check that the current node is responsible for storing the service,
	// and it's likely that the cluster has been readjusted
	bucketIndex := common.GetBucketIndex(target)
	common.Debugf("bucket index %d", bucketIndex)
	nodeId, err := b.GetNodeIdByIndex(bucketIndex)

	if err != nil {
		common.Errorf("target node id not found by index [%d] in current node [%s], %v", bucketIndex, nodeId, err.Error())
		return pb.ErrorType_MetaDataChanged
	}

	// The cluster metadata has changed
	if nodeId != b.SelfNode.Id {
		common.Errorf("cluster metadata has changed,"+
			" current node id %s does not match to the route node %s", b.SelfNode.Id, nodeId)
		return pb.ErrorType_MetaDataChanged
	}
	return pb.ErrorType_None
}

func (b *BaseCluster) CheckClusterInfo(clusterId uint64, leaderId string, term uint64) pb.ErrorType {
	if b.State != Active {
		common.Errorf("cluster %d is not active", clusterId)
		return pb.ErrorType_ClusterStateNotMatch
	}

	if b.ClusterId != clusterId {
		common.Errorf("cluster id not match, current cluster id: %d, "+
			"remote cluster id: %d", b.ClusterId, clusterId)
		return pb.ErrorType_ClusterIdNotMatch
	}

	// It rarely happens, and as long as the ClusterId is the same, then the leaderId and term must be the same
	if b.Leader.Id != leaderId {
		common.Errorf("the cluster id match, but the leader id don't, "+
			"current leader id: %s, remote leader id: %s", b.Leader.Id, leaderId)
		return pb.ErrorType_LeaderIdNotMatch
	}

	if b.Leader.Term != term {
		common.Errorf("the cluster id match, but the term don't, current term: %d,"+
			" remote term: %d", b.Leader.Term, term)
		return pb.ErrorType_TermNotMatch
	}
	return pb.ErrorType_None
}
