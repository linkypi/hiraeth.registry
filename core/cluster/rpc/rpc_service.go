package rpc

import (
	"context"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core/cluster"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ClusterRpcService struct {
	cluster *cluster.Cluster
	config  config.Config
}

func (c *ClusterRpcService) SetCluster(cluster *cluster.Cluster) {
	c.cluster = cluster
}
func NewCRpcService(conf config.Config) *ClusterRpcService {
	return &ClusterRpcService{
		config: conf,
	}
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
	err := c.addNodeToCluster(remoteNode)
	if err != nil {
		return &emptypb.Empty{}, err
	}
	return &emptypb.Empty{}, nil
}

func (c *ClusterRpcService) JoinCluster(_ context.Context, req *pb.JoinClusterRequest) (*emptypb.Empty, error) {
	return c.joinNodeToCluster(req)
}

func (c *ClusterRpcService) TransferLeadership(_ context.Context, req *pb.TransferRequest) (*emptypb.Empty, error) {
	c.cluster.Log.Infof("update leader to  %s, %s", req.NodeId, req.NodeAddr)
	c.cluster.UpdateLeader(int(req.Term), req.NodeId, req.NodeAddr)
	if req.Status == pb.TransferStatus_Transitioning {
		c.cluster.SetState(cluster.Transitioning)
	}
	if req.Status == pb.TransferStatus_Finished {
		c.cluster.SetState(cluster.Active)
	}
	return &emptypb.Empty{}, nil
}
