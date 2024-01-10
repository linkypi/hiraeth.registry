package cluster

import (
	"context"
	"github.com/linkypi/hiraeth.registry/config"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"strconv"
)

// GetNodeInfo the GRPC interface is implemented using the Node struct to facilitate updating the cluster state
func (c *Cluster) GetNodeInfo(ctx context.Context, req *pb.NodeInfoRequest) (*pb.NodeInfoResponse, error) {

	remoteNode := config.NodeInfo{
		Id:                    req.NodeId,
		Ip:                    req.NodeIp,
		Addr:                  req.NodeIp + ":" + strconv.Itoa(int(req.GetInternalPort())),
		IsCandidate:           req.IsCandidate,
		AutoJoinClusterEnable: req.AutoJoinClusterEnable,
	}

	existNode := c.ExistNode(remoteNode.Id)

	// it is very likely that the information of the remote node
	// is not in the current cluster configuration, such as the newly joined node
	// in this case, the current node needs to actively connect to the node in order
	// to maintain a persistent connection between them when the node
	// has sent the command to join the cluster
	err := c.UpdateRemoteNode(remoteNode, *c.SelfNode, false)
	if err != nil {
		return &pb.NodeInfoResponse{}, err
	}

	// if the remote node is not in the cluster
	// and the property [AutoJoinClusterEnable] is turned on
	// the remote node is added to the cluster
	if !existNode && remoteNode.AutoJoinClusterEnable {
		c.AddNewNode(&remoteNode)
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

func (c *Cluster) ForwardJoinClusterRequest(_ context.Context, req *pb.JoinClusterRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ForwardJoinClusterRequest not implemented")
}
