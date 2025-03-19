package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fatih/set"
	"github.com/hashicorp/raft"
	common "github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"sync/atomic"
	"time"
)

type Leader struct {
	*BaseCluster
	SlotManager *slot.Manager
	Id          string
	Addr        string
	Term        uint64
}

func NewLeader(term uint64, id, addr string) *Leader {
	return &Leader{Id: id, Term: term, Addr: addr}
}
func (l *Leader) setBaseCluster(b *BaseCluster) {
	l.BaseCluster = b
}

var (
	buildClusterErr = errors.New("build cluster failed")
)

func (l *Leader) buildCluster(nodes []*config.NodeInfo) bool {
	// Notify the follower node that the leader has been transferred,
	// and the cluster no longer receives write requests and only processes read requests
	// After the data migration is complete, the cluster is back to Active

	// We must wait for all nodes to receive a notification and respond
	// if some nodes are down or unable to connect, the cluster cannot be created
	ret := l.notifyAllNodesLeaderShipTransferStatus(nodes, pb.TransferStatus_Transitioning)
	if !ret {
		common.Errorf("[leader] failed to notify follower transfer leadership to [transitioning], some followers are not ready")
		return false
	}
	common.Debugf("[leader] notify all followers transfer leadership to [transitioning] success.")

	common.Debugf("[leader] verifing all the followers...")
	ackNum, actualClusterNodes, total := l.verifyAllFollowers()
	addresses := l.GetAddresses(actualClusterNodes)
	// Check whether the number of quorums in the healthy nodes of the cluster has been met
	jsonStr, _ := json.Marshal(addresses)
	if ackNum < l.Config.ClusterQuorumCount {
		common.Errorf("[leader] failed to create cluster, healthy nodes quorum not met, "+
			"ackNum: %d, ack nodes: %s, total required: %d", ackNum, jsonStr, total)
		return false
	}

	common.Debugf("[leader] the followers verified: %s", jsonStr)
	// We have to remove the invalid nodes, because raft has already joined all nodes in the configuration to the cluster at startup
	// raft constantly tries to automatically connect all the nodes and then join them to the cluster
	// however, in some cases, it may not be possible to connect to some nodes, or some nodes may become obsolete
	// therefore, we have to remove these invalid nodes from the cluster to avoid assigning cluster metadata to these invalid nodes
	l.removeInvalidServerInCluster(actualClusterNodes)

	err := l.buildMetaData(actualClusterNodes)
	if err != nil {
		return false
	}

	success := l.notifyAllNodesLeaderShipTransferStatus(actualClusterNodes, pb.TransferStatus_Completed)
	if !success {
		common.Errorf("[leader] failed to notify follower transfer leadership to [completed], some followers are not ready")
		return false
	}

	l.SetState(Active)
	common.Infof("[leader] the cluster is healthy, start receiving client requests")
	return true
}

func (l *Leader) removeInvalidServerInCluster(actualClusterNodes []*config.NodeInfo) {
	a := set.New(set.ThreadSafe)
	l.ClusterExpectedNodes.Range(func(key, value interface{}) bool {
		node := value.(*config.NodeInfo)
		a.Add(node.Id)
		return true
	})

	b := set.New(set.ThreadSafe)
	for _, server := range actualClusterNodes {
		b.Add(server.Id)
	}
	diff := set.Difference(a, b)
	for _, s := range diff.List() {
		serverId := fmt.Sprintf("%s", s)
		future := l.Raft.RemoveServer(raft.ServerID(serverId), 0, time.Millisecond*500)
		if err := future.Error(); err != nil {
			common.Debugf("[cluster] failed to remove server %s from raft cluster, err: %v", s, err)
			continue
		}
		common.Warnf("[cluster] removed invalid node %s in raft", s)
	}
	l.ClusterActualNodes.Range(func(key, value interface{}) bool {
		node := value.(*config.NodeInfo)
		exist := false
		for _, n := range actualClusterNodes {
			if node.Id == n.Id {
				exist = true
				node.IsCandidate = n.IsCandidate
				break
			}
		}
		if !exist {
			l.ClusterActualNodes.Delete(node.Id)
		}
		return true
	})
}

func (l *Leader) buildMetaData(clusterNodes []*config.NodeInfo) error {
	if clusterNodes == nil || len(clusterNodes) == 0 {
		return errors.New("cluster nodes is empty")
	}
	// allocate slots
	followerIds := l.GetNodeIdsIgnoreSelf(clusterNodes)
	shards, replicas := l.SlotManager.AllocateSlots(l.Leader.Id, followerIds, *l.Config)

	toJSON, err := common.SyncMapToJSON(&l.ClusterActualNodes)
	if err != nil {
		return err
	}
	common.Debugf("cluster actual nodes before build meta data: %s", toJSON)

	actualNodes := make([]config.NodeInfo, 5)
	l.ClusterActualNodes.Range(func(key, value interface{}) bool {
		node := value.(*config.NodeInfo)
		actualNodes = append(actualNodes, *node)
		return true
	})
	formattedTime := time.Now().Format("2006-01-02 15:04:05")

	atomic.AddUint64(&l.ClusterId, 1)
	// persist the meta data
	metaData := MetaData{
		State:         l.State.String(),
		ClusterId:     l.ClusterId,
		LeaderId:      l.Leader.Id,
		Term:          l.Leader.Term,
		Shards:        shards,
		Replicas:      replicas,
		NodeConfig:    *l.NodeConfig,
		ClusterConfig: *l.Config,
		ActualNodes:   actualNodes,
		CreateTime:    formattedTime,
	}

	common.CopySyncMap(&l.ClusterActualNodes, &metaData.ActualNodeMap)
	common.CopySyncMap(&l.ClusterExpectedNodes, &metaData.ExpectedNodeMap)

	marshal, err := json.Marshal(&metaData)
	if err != nil {
		return err
	}
	common.Debugf("meta data: %s", string(marshal))

	jsonStr, _ := metaData.ToJSON()
	err = common.PersistToJsonFileWithCheckSum(l.NodeConfig.DataDir+MetaDataFileName, jsonStr)
	if err != nil {
		common.Errorf("persist meta data failed: %v", err)
		return err
	}

	l.MetaData = &metaData
	success := l.publishMetaDataToAllNodes(&metaData)
	if !success {
		return buildClusterErr
	}

	return nil
}

func (l *Leader) AddNewNode(remoteNode *config.NodeInfo) error {

	if l.Raft == nil {
		common.Errorf("[cluster] raft is nil, can not add new node, node id: %s, node addr: %s", remoteNode.Id, remoteNode.Addr)
		return errors.New("raft is nil, can not add new node")
	}
	// Check whether the connection of the new node exists, and if not, establish the connection first
	if !l.ExistConn(remoteNode.Id) {
		l.ConnectToNode(remoteNode)
	}
	if l.State != Active {
		common.Errorf("[cluster] cluster state is not active: %s, can not add new node, id: %s, addr: %s",
			l.State.String(), remoteNode.Id, remoteNode.Addr)
		return errors.New("cluster state is not active")
	}
	// only the leader node has the permission to add nodes, otherwise the addition fails
	if !l.IsLeader() {
		return errors.New("only the leader node has the permission to add nodes")
	}
	var indexFur raft.IndexFuture
	if remoteNode.IsCandidate {
		indexFur = l.Raft.AddVoter(
			raft.ServerID(remoteNode.Id),
			raft.ServerAddress(remoteNode.Addr),
			0,
			3*time.Second,
		)
	} else {
		indexFur = l.Raft.AddNonvoter(
			raft.ServerID(remoteNode.Id),
			raft.ServerAddress(remoteNode.Addr),
			0,
			3*time.Second,
		)
	}

	err := indexFur.Error()
	if err != nil {
		// add later , configuration changed since 30 (latest is 1)
		common.Errorf("[cluster] add new node to cluster failed, node id: %s, addr: %s. %v", remoteNode.Id, remoteNode.Addr, err)
		return err
	}

	common.Infof("[cluster] add new node to cluster success, node id: %s, addr: %s", remoteNode.Id, remoteNode.Addr)
	return nil

}
