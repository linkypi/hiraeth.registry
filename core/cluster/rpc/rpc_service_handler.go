package rpc

import (
	"context"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core/cluster"
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

	// it is very likely that the information of the remote node
	// is not in the current cluster configuration, such as the newly joined node
	// in this case, the current node needs to actively connect to the node in order
	// to maintain a persistent connection between them when the node
	// has sent the command to join the cluster
	err := c.UpdateRemoteNode(remoteNode, *c.SelfNode, false)
	if err != nil {
		return &pb.NodeInfoResponse{}, err
	}

	node := c.SelfNode
	return &pb.NodeInfoResponse{
		NodeId:                node.Id,
		NodeIp:                node.Ip,
		InternalPort:          uint64(node.InternalPort),
		IsCandidate:           node.IsCandidate,
		AutoJoinClusterEnable: node.AutoJoinClusterEnable,
	}, nil
}

func (c *ClusterRpcService) addNodeToCluster(remoteNode config.NodeInfo) {
	if c.Raft == nil {
		// this is rarely the case, unless a stand-alone service starts
		c.Log.Errorf("[cluster] Raft is not initialized, can't add node to cluster, node id %s, addr : %s", remoteNode.Id, remoteNode.Addr)
		return
	}
	addr, leaderId := c.Raft.LeaderWithID()
	if addr == "" || leaderId == "" {
		c.Log.Errorf("[cluster] can't add node to cluster, leader not found, current node state: %s", c.Raft.State().String())
		return
	}

	// only the leader node has the permission to add nodes, otherwise the addition fails
	if raft.ServerID(c.SelfNode.Id) == leaderId {
		c.AddNewNode(&remoteNode)
	} else {
		// forward the request to the leader node for addition
		c.Log.Infof("[cluster] forwarding the request to the leader node for addition, node id: %s, addr: %s", leaderId, addr)
		client := c.GetInternalClient(string(leaderId))
		request := &pb.JoinClusterRequest{
			NodeId:                remoteNode.Id,
			NodeAddr:              remoteNode.Addr,
			IsCandidate:           remoteNode.IsCandidate,
			AutoJoinClusterEnable: true,
		}
		_, err := client.ForwardJoinClusterRequest(context.Background(), request)
		if err != nil {
			c.Log.Error("[cluster] failed to forward the request to the leader node for addition,"+
				" node id: %s, addr: %s, error: %s", leaderId, addr, err.Error())
			return
		}
		c.Log.Infof("added node to cluster success: %s, %s", remoteNode.Id, remoteNode.Addr)
	}
}

func (c *ClusterRpcService) joinNodeToCluster(req *pb.JoinClusterRequest) (*emptypb.Empty, error) {
	remoteNode := buildNodeInfoForJoinReq(req)
	// if the remote node is not in the cluster
	// and the property [AutoJoinClusterEnable] is turned on
	// the remote node is added to the cluster
	if c.State == cluster.Active {
		existNode := c.RaftExistNode(remoteNode.Id)
		if existNode {
			c.Log.Infof("Raft cluster has already exist node, node id: %s, addr: %s", remoteNode.Id, remoteNode.Addr)
			return &emptypb.Empty{}, nil
		}
		c.Log.Infof("the cluster is active, remote node id: %s, addr: %s, raft config exist: %v",
			remoteNode.Id, remoteNode.Addr, existNode)
		if !existNode && remoteNode.AutoJoinClusterEnable {
			c.addNodeToCluster(remoteNode)
		}
	} else {
		c.Log.Infof("the cluster is not active, can not add remote node to the cluster, "+
			"id: %s, addr: %s", remoteNode.Id, remoteNode.Addr)
	}

	return &emptypb.Empty{}, nil
}
