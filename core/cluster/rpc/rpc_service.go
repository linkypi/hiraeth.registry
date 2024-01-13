package rpc

import (
	"context"
	"errors"
	"fmt"
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

func (c *ClusterRpcService) TransferLeadership(_ context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	return c.handleLeadershipTransfer(req)
}

func (c *ClusterRpcService) GetFollowerInfo(_ context.Context, req *pb.FollowerInfoRequest) (*pb.FollowerInfoResponse, error) {
	return c.replyFollowerInfo(req)
}

func (c *ClusterRpcService) PublishMetadata(_ context.Context, req *pb.PublishMetadataRequest) (response *pb.PublishMetadataResponse, err error) {

	if c.cluster.State != cluster.Transitioning {
		c.cluster.Log.Errorf("cluster metadata reception failed, the cluster state not match: %s, "+
			"shoule be %s", c.cluster.State.String(), cluster.Transitioning.String())
		return &pb.PublishMetadataResponse{ErrorType: pb.ErrorType_ClusterStateNotMatch, ClusterState: c.cluster.State.String()}, nil
	}
	if req.LeaderId != c.cluster.Leader.Id {
		c.cluster.Log.Errorf("cluster metadata reception failed, leader id not match, "+
			"remote leader id: %s, current leader id: %s", req.LeaderId, c.cluster.Leader.Id)
		return &pb.PublishMetadataResponse{ErrorType: pb.ErrorType_LeaderIdNotMatch, LeaderId: c.cluster.Leader.Id}, nil
	}
	if int(req.Term) != c.cluster.Leader.Term {
		c.cluster.Log.Errorf("cluster metadata reception failed, term not match, "+
			"remote term: %d, current leader term: %d", req.Term, c.cluster.Leader.Term)
		return &pb.PublishMetadataResponse{ErrorType: pb.ErrorType_TermNotMatch, Term: int64(c.cluster.Leader.Term)}, nil
	}
	defer func() {
		if e := recover(); e != nil {
			c.cluster.Log.Errorf("transfer leadership panic: %v", e)
			response = nil
			err = errors.New(fmt.Sprintf("%v", e))
		}
	}()

	err = c.cluster.ApplyClusterMetaData(err, req)
	if err != nil {
		return nil, err
	}

	return &pb.PublishMetadataResponse{ErrorType: pb.ErrorType_None}, nil
}
