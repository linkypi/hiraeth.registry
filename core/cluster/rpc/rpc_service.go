package rpc

import (
	"context"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core/cluster"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ClusterRpcService struct {
	*cluster.Cluster
}

func (c *ClusterRpcService) SetCluster(cluster *cluster.Cluster) {
	c.Cluster = cluster
}
func NewCRpcService() *ClusterRpcService {
	return &ClusterRpcService{}
}

// GetNodeInfo the GRPC interface is implemented using the Node struct to facilitate updating the cluster state
func (c *ClusterRpcService) GetNodeInfo(ctx context.Context, req *pb.NodeInfoRequest) (*pb.NodeInfoResponse, error) {
	return c.replyNodeInfo(req)
}

func (c *ClusterRpcService) ForwardJoinClusterRequest(_ context.Context, req *pb.JoinClusterRequest) (*emptypb.Empty, error) {
	remoteNode := config.NodeInfo{
		Id:          req.NodeId,
		IsCandidate: req.IsCandidate,
		Addr:        req.NodeAddr}
	c.addNodeToCluster(remoteNode)
	return &emptypb.Empty{}, nil
}

func (c *ClusterRpcService) JoinCluster(_ context.Context, req *pb.JoinClusterRequest) (*emptypb.Empty, error) {
	return c.joinNodeToCluster(req)
}

func (c *ClusterRpcService) TransferLeadership(_ context.Context, req *pb.TransferRequest) (*emptypb.Empty, error) {
	c.Log.Infof("update leader to  %s, %s", req.NodeId, req.NodeAddr)
	c.UpdateLeader(req.NodeId, req.NodeAddr)
	return &emptypb.Empty{}, nil
}
