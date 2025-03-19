package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/config"
	raft2 "github.com/linkypi/hiraeth.registry/server/raft"
	"github.com/sourcegraph/conc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
	"strconv"
	"time"
)

func (b *BaseCluster) startRaftNode(dataDir string) {
	raftNode := raft2.RaftNode{}
	raftNode.SetNetWorkManager(b.Manager)

	selfNode := b.SelfNode
	propFsm := &raft2.PropFsm{}

	// The nodes that come in here should be the nodes that
	// have been connected and satisfy the election quorum number
	// not the nodes in the cluster.server.addr configuration
	connectedNodes := b.GetConnectedNodes(&b.ClusterExpectedNodes)
	connectedNodes = append(connectedNodes, b.SelfNode)
	var peers = make([]raft.Server, 0, len(connectedNodes))
	for _, node := range connectedNodes {
		suffrage := raft.Voter
		if !node.IsCandidate {
			suffrage = raft.Nonvoter
		}
		server := raft.Server{
			// Suffrage specify whether it is a candidate node or not, it can participate in the leader election
			Suffrage: suffrage,
			ID:       raft.ServerID(node.Id),
			Address:  raft.ServerAddress(node.Addr),
		}
		peers = append(peers, server)
	}

	err := raftNode.Start(selfNode.Id, dataDir, peers, *b.Config, b.notifyCh, propFsm)
	if err != nil {
		common.Errorf("failed to start raft node: %v", err.Error())
		b.Shutdown()
		return
	}
	b.Raft = &raftNode
	common.Infof("raft node started.")
}

func (b *BaseCluster) UpdateLeader(term uint64, id, addr string) {
	if b.Leader == nil {
		leader := NewLeader(term, id, addr)
		leader.SlotManager = b.SlotManager
		leader.setBaseCluster(b)
		b.Leader = leader
		return
	}

	b.Leader.Id = id
	b.Leader.Addr = addr
	b.Leader.Term = term
}

func (b *BaseCluster) notifyAllNodesLeaderShipTransferStatus(clusterNodes []*config.NodeInfo, status pb.TransferStatus) bool {

	total := len(clusterNodes)
	numOfAck, _, success := common.WaitForAllExecDone(clusterNodes, func(n *config.NodeInfo) bool {
		return b.notifyLeaderShipTransferStatus(n, status)
	})

	if !success {
		common.Errorf("failed to notify trasnfer leadership completed to %d of %d nodes", total-int(numOfAck), total)
		return false
	}

	return true
}

func (b *BaseCluster) verifyAllFollowers() (int, []*config.NodeInfo, int) {

	raftConf := b.Raft.GetConfiguration().Configuration()
	nodes := b.getNodesByRaftServers(raftConf.Servers)

	total := len(nodes)

	numOfAck, ackFollowers, _ := common.WaitForAllExecDone(nodes, func(server *config.NodeInfo) bool {
		addr := string(server.Addr)
		if b.Leader != nil && addr == b.Leader.Addr {
			return true
		}
		return b.verifyFollower(addr)
	})

	return int(numOfAck), ackFollowers, total
}

func (b *BaseCluster) verifyFollower(addr string) bool {
	con, err := b.Manager.GetConnByAddr(addr)
	if err != nil {
		common.Warnf("failed to verify follower, %s", addr)
		return false
	}

	request := pb.FollowerInfoRequest{
		LeaderId: b.Leader.Id,
		Term:     b.Leader.Term,
	}
	response, err := (*con.PeerClient).GetFollowerInfo(context.Background(), &request)
	if err != nil {
		common.Errorf("failed to verify follower, get follower info error: %s", err)
		return false
	}
	if response.LeaderId != b.Leader.Id {
		common.Errorf("failed to verify follower, leader id not match, current leader id: %s,"+
			" follower %s leader id: %s", b.Leader.Id, addr, response.LeaderId)
		return false
	}
	if response.Term != b.Leader.Term {
		common.Errorf("failed to verify follower, term not match, current term: %d,"+
			" follower %s term: %d", b.Leader.Term, addr, response.Term)
		return false
	}
	return true
}

func (b *BaseCluster) notifyLeaderShipTransferStatus(node *config.NodeInfo, status pb.TransferStatus) bool {

	if node.Id == b.Leader.Id {
		return true
	}
	request := pb.TransferRequest{
		Term:     b.Leader.Term,
		LeaderId: b.Leader.Id,
		Addr:     b.Leader.Addr,
		Status:   status,
	}

	err := common.WaitUntilExecSuccess(time.Second*3, b.ShutDownCh, func(...any) error {
		rpcClient := b.Manager.GetInterRpcClient(node.Id)
		if rpcClient == nil {
			common.Warnf("failed to notify leadership transfer to [%s], connection not ready: %s", status.String(), node.Id)
			return errors.New("failed to get rpc client")
		}
		response, err := (*rpcClient).TransferLeadership(context.Background(), &request)

		if response.ErrorType == pb.ErrorType_ClusterStateNotMatch {
			common.Errorf("[leader] failed to trasnfer leadership [%s] to %s:%s, cluster state not match: %s",
				status.String(), node.Id, node.Addr, response.ClusterState)
			return nil
		}
		if response.ErrorType == pb.ErrorType_LeaderIdNotMatch {
			common.Errorf("[leader] failed to trasnfer leadership [%s] to %s:%s, leader id not match, "+
				"current leader id: %s, remote leader id: %s",
				status.String(), node.Id, node.Addr, b.Leader.Id, response.LeaderId)
			return nil
		}
		if response.ErrorType == pb.ErrorType_TermNotMatch {
			common.Errorf("[leader] failed to trasnfer leadership [%s] to %s:%s, term not match, "+
				"current term: %d, remote term: %d",
				status.String(), node.Id, node.Addr, b.Leader.Term, response.Term)
			return nil
		}

		if err != nil {
			common.Errorf("failed to notify trasnfer leadership %s to %s:%s, will try agian, %v", status.String(), node.Id, node.Addr, err)
			time.Sleep(200 * time.Millisecond)
			return err
		}
		return nil
	})

	if err != nil {
		common.Errorf("failed to notify trasnfer leadership %s to %s:%s, %v", status.String(), node.Id, node.Addr, err)
		return false
	}
	return true
}

func (b *BaseCluster) publishMetaDataToAllNodes(metaData *MetaData) bool {

	nodes := b.GetConnectedNodes(&b.ClusterExpectedNodes)
	numOfAck, _, success := common.WaitForAllExecDone(nodes, func(n *config.NodeInfo) bool {
		return b.publishMetaData(n, metaData)
	})

	if !success {
		common.Errorf("failed to publish metadata to %d of %d nodes", len(nodes)-int(numOfAck), len(nodes))
		return false
	}

	return true
}

func (b *BaseCluster) publishMetaData(node *config.NodeInfo, metaData *MetaData) bool {

	jsonBytes, err := json.Marshal(metaData)
	if err != nil {
		common.Errorf("marshal meta data failed: %v", err)
		return false
	}
	request := pb.PublishMetadataRequest{
		LeaderId: b.Leader.Id,
		Term:     b.Leader.Term,
		MetaData: string(jsonBytes),
	}

	rpcClient := b.Manager.GetInterRpcClient(node.Id)
	err = common.WaitUntilExecSuccess(time.Second*3, b.ShutDownCh, func(...any) error {
		resp, err := (*rpcClient).PublishMetadata(context.Background(), &request)
		if resp.ErrorType == pb.ErrorType_ClusterStateNotMatch {
			common.Errorf("failed to publish metadata to %s:%s, %v", node.Id, node.Addr, err)
			return nil
		}
		if resp.ErrorType == pb.ErrorType_LeaderIdNotMatch {
			common.Errorf("failed to publish metadata to %s:%s, %v", node.Id, node.Addr, err)
			return nil
		}
		if resp.ErrorType == pb.ErrorType_TermNotMatch {
			common.Errorf("failed to publish metadata to %s:%s, %v", node.Id, node.Addr, err)
			return nil
		}
		if err != nil {
			common.Errorf("failed to publish metadata to %s:%s, will try agian, %v", node.Id, node.Addr, err)
			time.Sleep(200 * time.Millisecond)
			return err
		}
		common.Infof("publish metadata to %s:%s success", node.Id, node.Addr)
		return nil
	})

	if err != nil {
		common.Errorf("failed to publish metadata to %s:%s, %v", node.Id, node.Addr, err)
		return false
	}
	return true
}

func (b *BaseCluster) connectOtherCandidateNodes() {
	otherNodes := b.GetOtherNodes(b.SelfNode.Id)
	if len(otherNodes) == 0 {
		// this rarely happens because the list of servers in cluster mode
		// has already been determined when the node starts
		common.Error("failed to get other candidate nodes")
		b.Shutdown()
		return
	}

	// we can't know if the other nodes in the cluster is candidate node,
	// so we can't get the node information from Config.OtherCandidateServers(exclude self node)
	// we can only use Config.ClusterServers to connect to the cluster nodes and then
	// use GetNodeInfo to get information about each node in the cluster
	var wg conc.WaitGroup
	for _, node := range b.GetOtherNodes(b.SelfNode.Id) {
		remote := node
		wg.Go(func() {
			b.ConnectToNode(remote)
		})
	}
	wg.Wait()
	common.Info("all the other candidate nodes are connected.")
}

func (b *BaseCluster) Shutdown() {
	common.Infof("shutting down cluster node")
	if b.Raft != nil {
		future := b.Raft.Shutdown()
		err := future.Error()
		if err != nil {
			common.Errorf("raft shutdown failed: %v", err)
		}
	}

	err := b.Manager.CloseAllConn()
	if err != nil {
		common.Errorf("close all connections failed: %v", err)
	}
	b.ShutDownCh <- struct{}{}
	time.Sleep(time.Second)
}

func (b *BaseCluster) ConnectToNode(remoteNode *config.NodeInfo) {

	if remoteNode.Id == b.SelfNode.Id {
		return
	}
	retries := 1
	var kacp = keepalive.ClientParameters{
		// send pings every ClusterHeartbeatInterval seconds if there is no activity
		Time: time.Duration(b.Config.ClusterHeartbeatInterval) * time.Second,
		// wait 1 second for ping ack before considering the connection dead
		Timeout: time.Second,
		// send pings even without active streams
		PermitWithoutStream: true,
	}

	printTime := time.Now()
	for {
		select {
		case <-b.ShutDownCh:
			return
		default:
		}
		conn, err := grpc.Dial(remoteNode.Addr, grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp))
		if err != nil {
			common.Errorf("failed to dial server: %s, retry time: %d. %v", remoteNode.Addr, retries, err)
			retries++
			time.Sleep(time.Second)
			continue
		}

		conn.Connect()
		ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelFunc()
		conn.WaitForStateChange(ctx, connectivity.Connecting)

		if conn.GetState() == connectivity.Ready {
			common.Infof("connected to node: %s, %s", remoteNode.Id, remoteNode.Addr)

			// record remote connection
			clusterServiceClient := pb.NewClusterServiceClient(conn)
			raftTransportClient := pb.NewRaftTransportClient(conn)
			b.AddConn(remoteNode.Id, remoteNode.Addr, conn, &clusterServiceClient, &raftTransportClient)
			b.getRemoteNodeInfo(remoteNode)
			return
		}

		time.Sleep(5 * time.Second)

		if time.Now().Sub(printTime).Seconds() > 30 {
			common.Warnf("[heartbeat] waiting for %s:%s to be ready", remoteNode.Id, remoteNode.Addr)
			printTime = time.Now()
		}
	}
}

// exchange information between the two nodes in preparation for the creation of a myCluster
func (b *BaseCluster) getRemoteNodeInfo(remoteNode *config.NodeInfo) {

	// handle the problem of cluster configuration mismatch when updating remote node
	// if the cluster configuration does not match, it will exit directly
	defer common.PrintStackTraceWithCallback(func(err any) {
		b.Shutdown()
	})

	retries := 0
	for {
		select {
		case <-b.ShutDownCh:
			return
		default:
		}
		currentNode := b.SelfNode
		request := pb.NodeInfoRequest{
			NodeId:                currentNode.Id,
			NodeIp:                currentNode.Ip,
			InternalPort:          uint32(currentNode.InternalPort),
			IsCandidate:           currentNode.IsCandidate,
			AutoJoinClusterEnable: currentNode.AutoJoinClusterEnable,
			ExternalHttpPort:      uint32(currentNode.ExternalHttpPort),
			ExternalTcpPort:       uint32(currentNode.ExternalTcpPort),
		}
		rpcClient := b.GetInterRpcClient(remoteNode.Id)
		remote, err := (*rpcClient).GetNodeInfo(context.Background(), &request)
		if err != nil {
			connected := b.IsConnected(remoteNode.Id)
			if !connected {
				common.Errorf("remote node [%s][%s] is disconnected.", remoteNode.Id, remoteNode.Addr)
				b.ConnectToNode(remoteNode)
				continue
			}
			common.Errorf("failed to get node info from %s - %s, retry time: %d, . %v", remoteNode.Id, remoteNode.Addr, retries, err)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		if remote.StartupMode == pb.StartupMode_StandAlone {
			common.Warnf("remote node [%s][%s] is in standalone mode, close the remote connection.", remoteNode.Id, remoteNode.Addr)
			b.RemoveNode(remote.NodeId)
			err := b.CloseConn(remote.NodeId)
			if err != nil {
				common.Errorf("failed to close the remote connection of node [%s][%s], %v", remoteNode.Id, remoteNode.Addr, err)
			}
			return
		}

		rn := config.NodeInfo{
			Id:                    remote.NodeId,
			Ip:                    remote.NodeIp,
			Addr:                  remote.NodeIp + ":" + strconv.Itoa(int(remote.InternalPort)),
			InternalPort:          int(remote.InternalPort),
			IsCandidate:           remote.IsCandidate,
			AutoJoinClusterEnable: remote.AutoJoinClusterEnable,
			ExternalHttpPort:      int(remote.ExternalHttpPort),
			ExternalTcpPort:       int(remote.ExternalTcpPort),
		}

		// update cluster node config
		_ = b.UpdateRemoteNode(rn, *b.SelfNode, true)
		return
	}
}

func (b *BaseCluster) ApplyClusterMetaData(err error, req *pb.PublishMetadataRequest) error {

	b.metaDataMtx.Lock()
	defer b.metaDataMtx.Unlock()

	var metaData MetaData
	err = json.Unmarshal([]byte(req.MetaData), &metaData)
	if err != nil {
		common.Errorf("[follower] cluster metadata reception failed, unmarshal metadata failed: %s, %v", req.MetaData, err)
		return errors.New("unmarshal metadata failed")
	}

	toJSON, _ := common.SyncMapToJSON(&metaData.ActualNodeMap)
	common.Debugf("meta data actual node map: %s", toJSON)

	// update cluster node config
	common.CopySyncMap(&metaData.ActualNodeMap, &b.ClusterActualNodes)

	toJSON, _ = common.SyncMapToJSON(&b.ClusterActualNodes)
	common.Debugf("update cluster actual nodes to: %s", toJSON)

	metaData.State = b.State.String()
	metaData.NodeConfig = *b.NodeConfig
	metaData.CreateTime = time.Now().Format("2006-01-02 15:04:05")
	b.MetaData = metaData.DeepCopy()

	b.ClusterId = metaData.ClusterId

	common.Debugf("cluster id update to: %d", b.ClusterId)
	jsonStr, _ := metaData.ToJSON()
	err = common.PersistToJsonFileWithCheckSum(b.NodeConfig.DataDir+MetaDataFileName, jsonStr)
	if err != nil {
		common.Errorf("[follower] persist meta data failed: %v", err)
		return errors.New("persist metadata failed")
	}
	common.Infof("[follower] persist cluster meta data success.")

	b.SlotManager.InitSlotsAndReplicas(b.SelfNode.Id, metaData.Shards, metaData.Replicas, *b.Config)

	common.Infof("[follower] init slots and replicas success.")
	return nil
}
