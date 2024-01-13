package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core/slot"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"github.com/linkypi/hiraeth.registry/util"
	"gopkg.in/fatih/set.v0"
	"time"
)

type Leader struct {
	*BaseCluster
	SlotManager *slot.Manager
	Id          string
	Addr        string
	Term        int
}

func NewLeader(term int, id, addr string) *Leader {
	return &Leader{Id: id, Term: term, Addr: addr}
}
func (l *Leader) setBaseCluster(b *BaseCluster) {
	l.BaseCluster = b
}

var (
	buildClusterErr = errors.New("build cluster failed")
)

func (l *Leader) buildCluster(nodes []config.NodeInfo) bool {
	// Notify the follower node that the leader has been transferred,
	// and the cluster no longer receives write requests and only processes read requests
	// After the data migration is complete, the cluster is back to Active

	// We must wait for all nodes to receive a notification and respond
	// if some nodes are down or unable to connect, the cluster cannot be created
	ret := l.notifyAllNodesLeaderShipTransferStatus(nodes, pb.TransferStatus_Transitioning)
	if !ret {
		l.Log.Errorf("[leader] failed to notify follower transfer leadership to [transitioning], some followers are not ready")
		return false
	}
	l.Log.Debugf("[leader] notify all followers transfer leadership to [transitioning] success.")

	l.Log.Debugf("[leader] verifing all the followers...")
	ackNum, actualClusterNodes, total := l.verifyAllFollowers()
	addresses := l.GetAddresses(actualClusterNodes)
	// Check whether the number of quorums in the healthy nodes of the cluster has been met
	jsonStr, _ := json.Marshal(addresses)
	if ackNum < l.Config.ClusterQuorumCount {
		l.Log.Errorf("[leader] failed to create cluster, healthy nodes quorum not met, "+
			"ackNum: %d, ack nodes: %s, total required: %d", ackNum, jsonStr, total)
		return false
	}

	l.Log.Debugf("[leader] the followers verified: %s", jsonStr)
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
		l.Log.Errorf("[leader] failed to notify follower transfer leadership to [completed], some followers are not ready")
		return false
	}

	l.Log.Infof("[leader] the cluster is healthy, start receiving client requests")
	return true
}

func (l *Leader) removeInvalidServerInCluster(actualClusterNodes []config.NodeInfo) {
	a := set.New(set.ThreadSafe)
	for _, node := range l.ClusterExpectedNodes {
		a.Add(node.Id)
	}

	b := set.New(set.ThreadSafe)
	for _, server := range actualClusterNodes {
		b.Add(server.Id)
	}
	diff := set.Difference(a, b)
	for _, s := range diff.List() {
		serverId := fmt.Sprintf("%s", s)
		future := l.Raft.RemoveServer(raft.ServerID(serverId), 0, time.Millisecond*500)
		if err := future.Error(); err != nil {
			l.Log.Debugf("[cluster] failed to remove server %s from raft cluster, err: %v", s, err)
			continue
		}
		l.Log.Infof("[cluster] removed invalid node %s in raft", s)
	}

	for _, an := range l.ClusterActualNodes {
		exist := false
		for _, node := range actualClusterNodes {
			if an.Id == node.Id {
				exist = true
				an.IsCandidate = node.IsCandidate
				break
			}
		}
		if !exist {
			delete(l.ClusterActualNodes, an.Id)
		}
	}
}

func (l *Leader) buildMetaData(clusterNodes []config.NodeInfo) error {
	if clusterNodes == nil || len(clusterNodes) == 0 {
		return errors.New("cluster nodes is empty")
	}
	// allocate slots
	followerIds := l.GetNodeIdsIgnoreSelf(clusterNodes)
	shards, replicas := l.SlotManager.AllocateSlots(l.Leader.Id, followerIds)

	actualNodesMap := l.CopyClusterNodes(l.ClusterActualNodes)
	actualNodes := l.MapToList(actualNodesMap)

	formattedTime := time.Now().Format("2006-01-02 15:04:05")
	// persist the meta data
	metaData := MetaData{
		LeaderId:        l.Leader.Id,
		Term:            l.Leader.Term,
		Shards:          shards,
		Replicas:        replicas,
		NodeConfig:      *l.nodeConfig,
		ClusterConfig:   *l.Config,
		ExpectedNodeMap: l.CopyClusterNodes(l.ClusterExpectedNodes),
		ActualNodeMap:   actualNodesMap,
		ActualNodes:     actualNodes,
		CreateTime:      formattedTime,
	}

	marshal, err := json.Marshal(metaData)
	if err != nil {
		return err
	}
	l.Log.Infof("meta data: %s", string(marshal))

	err = util.PersistToJsonFileWithCheckSum(l.nodeConfig.DataDir+MetaDataFileName, metaData)
	if err != nil {
		l.Log.Errorf("persist meta data failed: %v", err)
		return err
	}

	success := l.publishMetaDataToAllNodes(metaData)
	if !success {
		return buildClusterErr
	}

	return nil
}

func (l *Leader) AddNewNode(remoteNode *config.NodeInfo) error {

	if l.Raft == nil {
		l.Log.Errorf("[cluster] raft is nil, can not add new node, node id: %s, node addr: %s", remoteNode.Id, remoteNode.Addr)
		return errors.New("raft is nil, can not add new node")
	}
	// Check whether the connection of the new node exists, and if not, establish the connection first
	if !l.ExistConn(remoteNode.Id) {
		l.ConnectToNode(*remoteNode)
	}
	if l.State != Active {
		l.Log.Errorf("[cluster] cluster state is not active: %s, can not add new node, id: %s, addr: %s",
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
		l.Log.Errorf("[cluster] add new node to cluster failed, node id: %s, addr: %s. %v", remoteNode.Id, remoteNode.Addr, err)
		return err
	}

	l.Log.Infof("[cluster] add new node to cluster success, node id: %s, addr: %s", remoteNode.Id, remoteNode.Addr)
	return nil

}
