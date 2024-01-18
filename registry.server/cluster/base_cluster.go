package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/linkypi/hiraeth.registry/server/cluster/network"
	"github.com/linkypi/hiraeth.registry/server/config"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"github.com/sirupsen/logrus"
	"strconv"
	"sync"
)

type BaseCluster struct {
	*network.Manager
	SlotManager *slot.Manager
	Log         *logrus.Logger

	State       State
	StateMtx    sync.Mutex
	metaDataMtx sync.Mutex

	Leader      *Leader
	Raft        *raft.Raft
	StartUpMode config.StartUpMode
	Config      *config.ClusterConfig
	NodeConfig  *config.NodeConfig
	ShutDownCh  chan struct{}
	SelfNode    *config.NodeInfo
	notifyCh    chan bool

	ClusterId uint64
	MetaData  MetaData

	// all cluster nodes configured in the configuration
	ClusterExpectedNodes map[string]*config.NodeInfo

	// The actual cluster nodes
	ClusterActualNodes map[string]*config.NodeInfo

	OtherCandidateNodes []config.NodeInfo
	joinCluster         bool
}

type State int

const (
	Initializing State = iota
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

func (s State) String() string {
	names := [...]string{"Initializing", "Active", "Transitioning", "Down", "StandBy", "Maintenance", "Unknown"}
	if s < 0 || s > State(len(names)-1) {
		return "Unknown"
	}
	return names[s]
}

func (b *BaseCluster) SetState(state State) {
	b.StateMtx.Lock()
	defer b.StateMtx.Unlock()
	b.State = state
	b.MetaData.State = state
	b.Log.Debugf("cluster state changed to %s", state)
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
func (b *BaseCluster) GetOtherCandidateServers() []config.NodeInfo {
	var filtered []config.NodeInfo
	for _, node := range b.ClusterExpectedNodes {
		if node.IsCandidate {
			filtered = append(filtered, *node)
		}
	}
	return filtered
}

func (b *BaseCluster) GetOtherNodes(selfId string) []config.NodeInfo {
	var filtered = make([]config.NodeInfo, 0, 8)
	for _, node := range b.ClusterExpectedNodes {
		if node.Id != selfId {
			filtered = append(filtered, *node)
		}
	}
	return filtered
}

func (b *BaseCluster) MapToList(nodes map[string]config.NodeInfo) []config.NodeInfo {
	var result = make([]config.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, node)
	}
	return result
}

func (b *BaseCluster) GetAddresses(nodes []config.NodeInfo) []string {
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

func (b *BaseCluster) GetNodeIdsIgnoreSelf(clusterServers []config.NodeInfo) []string {
	arr := make([]string, 0, len(clusterServers))
	for _, node := range clusterServers {
		if node.Id != b.SelfNode.Id {
			arr = append(arr, node.Id)
		}
	}
	return arr
}

func (b *BaseCluster) CopyClusterNodesWithPtr(nodes map[string]config.NodeInfo) map[string]*config.NodeInfo {
	var result = make(map[string]*config.NodeInfo)
	for id, node := range nodes {
		result[id] = &node
	}
	return result
}

func (b *BaseCluster) getNodesByRaftServers(servers []raft.Server) []config.NodeInfo {
	var result = make([]config.NodeInfo, 0, len(servers))
	for _, server := range servers {
		for _, node := range b.ClusterExpectedNodes {
			if node.Addr == string(server.Address) {
				if server.Suffrage == raft.Voter {
					node.IsCandidate = true
				}
				result = append(result, *node)
				break
			}
		}
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
	for _, node := range b.ClusterExpectedNodes {
		if node.Id != nodeId {
			delete(b.ClusterExpectedNodes, node.Id)
		}
	}
}

func (b *BaseCluster) UpdateRemoteNode(remoteNode config.NodeInfo, selfNode config.NodeInfo, throwEx bool) error {

	if remoteNode.Id == selfNode.Id {
		msg := fmt.Sprintf("[cluster] the remote node id [%s][%s] conflicts with the current node id [%s][%s]",
			remoteNode.Id, remoteNode.Addr, selfNode.Id, selfNode.Addr)
		//b.Log.Warnf(msg)
		if throwEx {
			panic(msg)
		}
		return errors.New(msg)
	}

	node, ok := b.ClusterExpectedNodes[remoteNode.Id]
	if !ok {
		// it is possible that a node is missing from the cluster address
		// configuration in the node, or it may be a new node
		msg := fmt.Sprintf("[cluster] remote node [%s][%s] not found int cluster servers, add it to cluster.", remoteNode.Id, remoteNode.Addr)
		b.Log.Info(msg)
		node = &remoteNode
		b.ClusterExpectedNodes[remoteNode.Id] = node
	} else {
		if node.Addr != remoteNode.Addr {
			msg := fmt.Sprintf("[cluster] update remote node info failed, node id exist, but addr not match: %s, %s, %s",
				remoteNode.Id, node.Addr, remoteNode.Addr)
			b.Log.Error(msg)
			if throwEx {
				panic(msg)
			}
			return errors.New(msg)
		}
		node.IsCandidate = remoteNode.IsCandidate
		node.ExternalHttpPort = remoteNode.ExternalHttpPort
		node.ExternalTcpPort = remoteNode.ExternalTcpPort
	}

	if node.IsCandidate {
		b.OtherCandidateNodes = append(b.OtherCandidateNodes, *node)
	}
	b.Log.Infof("[cluster] update remote node info: %s, %s", remoteNode.Id, remoteNode.Addr)
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

// FindRouteAndExecOrForward Check the machine deployment mode first, and if it is a stand-alone mode, the registration logic
// will be executed by doFunc directly, and if it is a cluster mode, the cluster status will be checked first.
// If the cluster is normal, select the target node according to the consistent hashing algorithm, and if the
// target node is the current node, directly execute doFunc, otherwise getForwardArgs is used to forward the request to the target node
func (b *BaseCluster) FindRouteAndExecOrForward(target string, doFunc func(bucket *slot.Bucket) (any, error),
	getForwardArgs func() (pb.RequestType, []byte, error)) (any, error) {
	return b.FindRouteAndExec(target, doFunc, true, getForwardArgs)
}

func (b *BaseCluster) FindRouteAndExecNoForward(target string, doFunc func(bucket *slot.Bucket) (any, error),
	getForwardArgs func() (pb.RequestType, []byte, error)) (any, error) {
	return b.FindRouteAndExec(target, doFunc, false, getForwardArgs)
}

func (b *BaseCluster) FindRouteAndExec(target string, doFunc func(bucket *slot.Bucket) (any, error), forward bool,
	getForwardArgs func() (pb.RequestType, []byte, error)) (any, error) {

	bucketIndex := common.GetBucketIndex(target)
	// If it is a stand-alone mode, you can register directly
	if b.StartUpMode == config.StandAlone {
		bucket := b.SlotManager.GetSlotByIndex(bucketIndex)
		res, err := doFunc(bucket)
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	// If it is a cluster mode, you need to check the cluster status first
	if b.State != Active {
		b.Log.Errorf("failed to find the route: %s, cluster state not active: %v", target, b.State.String())
		return nil, errors.New("cluster state not active: " + b.State.String())
	}

	nodeId, err := b.GetNodeIdByIndex(bucketIndex)
	if err != nil {
		b.Log.Errorf("failed to find node id for the target: %s, %v", target, err.Error())
		return nil, errors.New("target node id not found")
	}
	if nodeId == b.SelfNode.Id {
		bucket := b.SlotManager.GetSlotByIndex(bucketIndex)
		res, err := doFunc(bucket)
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	// For some special requests, we cannot forward, such as service subscriptions
	// Subscription requests do not need to be forwarded because the server needs to be directly
	// connected to the client to send service change notifications
	if !forward {
		return nil, common.ErrorMetadataChanged
	}
	// forwarding to the right node
	requestType, payload, err := getForwardArgs()
	if err != nil {
		return nil, err
	}
	response, err := b.ForwardRequest(nodeId, requestType, payload)
	if err != nil {
		return nil, err
	}
	return response.Payload, nil
}

func (b *BaseCluster) ForwardRequest(nodeId string, requestType pb.RequestType, payload []byte) (*pb.ForwardCliResponse, error) {

	cliRequest := pb.ForwardCliRequest{
		ClusterId:   b.ClusterId,
		LeaderId:    b.Leader.Id,
		Term:        b.Leader.Term,
		RequestType: requestType,
		Payload:     payload,
	}
	interRpcClient := b.GetInterRpcClient(nodeId)
	response, err := (*interRpcClient).ForwardClientRequest(context.Background(), &cliRequest)
	if err != nil {
		b.Log.Errorf("failed to forward %s request, %v", requestType.String(), err)
		return nil, err
	}

	var res pb.ForwardCliResponse
	err = common.Decode(response.Payload, &res)
	if err != nil {
		b.Log.Errorf("failed to decode %s forward response, %v", requestType.String(), err)
		return nil, err
	}

	if res.ErrorType == pb.ErrorType_None {
		return &res, nil
	}
	if response.ErrorType == pb.ErrorType_ClusterStateNotMatch {
		b.Log.Errorf("failed to forward %s request, cluster state not match: %v", requestType.String(), err)
		return nil, errors.New("cluster state not match")
	}
	if response.ErrorType == pb.ErrorType_ClusterIdNotMatch {
		b.Log.Errorf("failed to forward %s request, cluster id not match: %v", requestType.String(), err)
		return nil, errors.New("cluster id not match")
	}
	if response.ErrorType == pb.ErrorType_LeaderIdNotMatch {
		b.Log.Errorf("failed to forward %s request, leader id not match: %v", requestType.String(), err)
		return nil, errors.New("leader id not match")
	}
	if response.ErrorType == pb.ErrorType_TermNotMatch {
		b.Log.Errorf("failed to forward %s request, term not match: %v", requestType.String(), err)
		return nil, errors.New("term not match")
	}
	if response.ErrorType == pb.ErrorType_MetaDataChanged {
		b.Log.Errorf("failed to forward %s request, meta data changed: %v", requestType.String(), err)
		return nil, errors.New("meta data changed")
	}
	b.Log.Errorf("failed to forward %s request, unknown error type: %v", requestType.String(), response.ErrorType)
	return nil, errors.New("unknown error type: " + response.ErrorType.String())
}

func (b *BaseCluster) CheckNodeRouteForServiceName(target string) pb.ErrorType {
	// Double-check that the current node is responsible for storing the service,
	// and it's likely that the cluster has been readjusted
	bucketIndex := common.GetBucketIndex(target)
	b.Log.Debugf("bucket index %d", bucketIndex)
	nodeId, err := b.GetNodeIdByIndex(bucketIndex)

	if err != nil {
		b.Log.Errorf("target node id not found by index [%d] in current node [%s], %v", bucketIndex, nodeId, err.Error())
		return pb.ErrorType_MetaDataChanged
	}

	// The cluster metadata has changed
	if nodeId != b.SelfNode.Id {
		b.Log.Errorf("cluster metadata has changed,"+
			" current node id %s does not match to %s", b.SelfNode.Id, nodeId)
		return pb.ErrorType_MetaDataChanged
	}
	return pb.ErrorType_None
}

func (b *BaseCluster) CheckClusterInfo(clusterId uint64, leaderId string, term uint64) pb.ErrorType {
	if b.State != Active {
		b.Log.Errorf("cluster %d is not active", clusterId)
		return pb.ErrorType_ClusterStateNotMatch
	}

	if b.ClusterId != clusterId {
		b.Log.Errorf("cluster id not match, current cluster id: %d, "+
			"remote cluster id: %d", b.ClusterId, clusterId)
		return pb.ErrorType_ClusterIdNotMatch
	}

	// It rarely happens, and as long as the ClusterId is the same, then the leaderId and term must be the same
	if b.Leader.Id != leaderId {
		b.Log.Errorf("the cluster id match, but the leader id don't, "+
			"current leader id: %s, remote leader id: %s", b.Leader.Id, leaderId)
		return pb.ErrorType_LeaderIdNotMatch
	}

	if b.Leader.Term != term {
		b.Log.Errorf("the cluster id match, but the term don't, current term: %d,"+
			" remote term: %d", b.Leader.Term, term)
		return pb.ErrorType_TermNotMatch
	}
	return pb.ErrorType_None
}
