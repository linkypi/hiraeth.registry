package rpc

import (
	"context"
	cluster2 "github.com/linkypi/hiraeth.registry/cluster"
	"github.com/linkypi/hiraeth.registry/config"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ClusterRpcService struct {
	cluster *cluster2.Cluster
	config  config.Config
}

func (c *ClusterRpcService) SetCluster(cluster *cluster2.Cluster) {
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

func (c *ClusterRpcService) TransferLeadership(_ context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	return c.handleLeadershipTransfer(req)
}

func (c *ClusterRpcService) GetFollowerInfo(_ context.Context, req *pb.FollowerInfoRequest) (*pb.FollowerInfoResponse, error) {
	return c.replyFollowerInfo(req)
}

func (c *ClusterRpcService) PublishMetadata(_ context.Context, req *pb.PublishMetadataRequest) (response *pb.PublishMetadataResponse, err error) {
	return c.handleMetadata(req, response, err)
}
