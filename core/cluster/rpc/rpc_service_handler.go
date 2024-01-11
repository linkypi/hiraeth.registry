package rpc

import (
	"context"
	"errors"
	"github.com/linkypi/hiraeth.registry/config"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"strconv"
)

func buildNodeInfo(req *pb.NodeInfoRequest) config.NodeInfo {
	remoteNode := config.NodeInfo{
		Id:                    req.NodeId,
		Ip:                    req.NodeIp,
		Addr:                  req.NodeIp + ":" + strconv.Itoa(int(req.GetInternalPort())),
		IsCandidate:           req.IsCandidate,
		AutoJoinClusterEnable: req.AutoJoinClusterEnable,
	}
	return remoteNode
}

func buildNodeInfoForJoinReq(req *pb.JoinClusterRequest) config.NodeInfo {
	remoteNode := config.NodeInfo{
		Id:                    req.NodeId,
		Addr:                  req.NodeAddr,
		IsCandidate:           req.IsCandidate,
		AutoJoinClusterEnable: req.AutoJoinClusterEnable,
	}
	return remoteNode
}

func (c *ClusterRpcService) replyNodeInfo(req *pb.NodeInfoRequest) (*pb.NodeInfoResponse, error) {

	remoteNode := buildNodeInfo(req)
	node := c.cluster.SelfNode
	mode := pb.StartupMode_Cluster
	if c.config.StartupMode == config.StandAlone {
		mode = pb.StartupMode_StandAlone
	}
	response := &pb.NodeInfoResponse{
		NodeId:                node.Id,
		NodeIp:                node.Ip,
		InternalPort:          uint64(node.InternalPort),
		IsCandidate:           node.IsCandidate,
		AutoJoinClusterEnable: node.AutoJoinClusterEnable,
		StartupMode:           mode,
	}

	if c.config.StartupMode == config.StandAlone {
		//errors.New("the node is in stand-alone mode, and the information of the node cannot be obtained")
		return response, nil
	}

	// it is very likely that the information of the remote node
	// is not in the current cluster configuration, such as the newly joined node
	// in this case, the current node needs to actively connect to the node in order
	// to maintain a persistent connection between them when the node
	// has sent the command to join the cluster
	err := c.cluster.UpdateRemoteNode(remoteNode, *c.cluster.SelfNode, false)
	if err != nil {
		return &pb.NodeInfoResponse{}, err
	}

	return response, nil
}

func (c *ClusterRpcService) addNodeToCluster(remoteNode config.NodeInfo) error {
	cluster := c.cluster
	if cluster.Raft == nil {
		// this is rarely the case, unless a stand-alone service starts
		cluster.Log.Errorf("[cluster] Raft is not initialized, can't add node to cluster, node id %s, addr : %s", remoteNode.Id, remoteNode.Addr)
		return errors.New("raft is not initialized, can't add node to cluster")
	}
	addr, leaderId := cluster.Raft.LeaderWithID()
	if addr == "" || leaderId == "" {
		cluster.Log.Errorf("[cluster] leader not found, can't add node to cluster, "+
			"current node state: %s, cluster state: %s", cluster.Raft.State().String(), cluster.State.String())
		return errors.New("leader not found, can't add node to cluster")
	}

	// only the leader node has the permission to add nodes, otherwise the addition fails
	if cluster.IsLeader() {
		return cluster.Leader.AddNewNode(&remoteNode)
	}
	// forward the request to the leader node for addition
	cluster.Log.Infof("[cluster] forwarding the request to the leader node for addition, node id: %s, addr: %s", leaderId, addr)
	rpcClient := cluster.GetInterRpcClient(string(leaderId))
	request := &pb.JoinClusterRequest{
		NodeId:                remoteNode.Id,
		NodeAddr:              remoteNode.Addr,
		IsCandidate:           remoteNode.IsCandidate,
		AutoJoinClusterEnable: true,
	}
	_, err := rpcClient.ForwardJoinClusterRequest(context.Background(), request)
	if err != nil {
		cluster.Log.Error("[cluster] failed to forward the request to the leader node for addition,"+
			" node id: %s, addr: %s, error: %s", leaderId, addr, err.Error())
		return err
	}
	cluster.Log.Infof("added node to cluster success: %s, %s", remoteNode.Id, remoteNode.Addr)
	return nil
}

func (c *ClusterRpcService) joinNodeToCluster(req *pb.JoinClusterRequest) (*emptypb.Empty, error) {
	remoteNode := buildNodeInfoForJoinReq(req)
	if !c.cluster.IsActive() {
		c.cluster.Log.Infof("the cluster is not active, can not add remote node to the cluster, "+
			"id: %s, addr: %s", remoteNode.Id, remoteNode.Addr)
		return &emptypb.Empty{}, errors.New("the cluster is not active")
	}
	existNode := c.cluster.RaftExistNode(remoteNode.Id)
	if existNode {
		c.cluster.Log.Infof("raft cluster has already exist node, node id: %s, addr: %s", remoteNode.Id, remoteNode.Addr)
		return &emptypb.Empty{}, nil
	}
	c.cluster.Log.Infof("the cluster is active, remote node id: %s, addr: %s, raft config exist: %v",
		remoteNode.Id, remoteNode.Addr, existNode)
	if !existNode && remoteNode.AutoJoinClusterEnable {
		err := c.addNodeToCluster(remoteNode)
		if err != nil {
			return &emptypb.Empty{}, err
		}
		return &emptypb.Empty{}, nil
	}

	return &emptypb.Empty{}, nil
}
