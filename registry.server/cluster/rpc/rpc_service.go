package rpc

import (
	"context"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	cluster "github.com/linkypi/hiraeth.registry/server/cluster"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/linkypi/hiraeth.registry/server/log"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ClusterRpcService struct {
	cluster *cluster.Cluster
	config  config.Config
	syner   *cluster.Syner
}

func (c *ClusterRpcService) SetCluster(cl *cluster.Cluster) {
	c.cluster = cl
	c.syner = cluster.NewSyner(log.Log, c.cluster)
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

func (c *ClusterRpcService) FetchServiceInstances(_ context.Context, req *pb.FetchServiceRequest) (res *pb.FetchServiceResponse, err error) {
	return c.fetchServiceInstances(req, err)
}
