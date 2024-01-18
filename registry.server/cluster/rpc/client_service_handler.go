package rpc

import (
	"context"
	"errors"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
)

func (c *ClusterRpcService) ForwardClientRequest(ctx context.Context, req *pb.ForwardCliRequest) (res *pb.ForwardCliResponse, e error) {
	defer func() {
		if err := recover(); err != nil {
			c.cluster.Log.Errorf("handle forward reqeuest panic: %v", err)
		}
		e = errors.New("failed forward request, internal error")
	}()
	forwardRes := &pb.ForwardCliResponse{
		ErrorType: pb.ErrorType_None,
		ClusterId: c.cluster.ClusterId,
		LeaderId:  c.cluster.Leader.Id,
		Term:      c.cluster.Leader.Term,
	}
	// Check whether they belong to the same cluster
	errorType := c.cluster.CheckClusterInfo(req.ClusterId, req.LeaderId, req.Term)
	if errorType != pb.ErrorType_None {
		forwardRes.ErrorType = errorType
		return forwardRes, nil
	}

	if req.RequestType == pb.RequestType_Register {
		var request pb.RegisterRequest
		err := common.Decode(req.Payload, &request)
		if err != nil {
			c.cluster.Log.Errorf("failed handle forward %s reqeuest: %v", req.RequestType.String(), err)
			return nil, err
		}
		// Check whether the metadata of the cluster has changed
		errorType := c.cluster.CheckNodeRouteForServiceName(request.ServiceName)
		if errorType != pb.ErrorType_None {
			res := c.doRegisterService(&request)
			return res, nil
		}
		forwardRes.ErrorType = errorType
		return forwardRes, nil
	}

	if req.RequestType == pb.RequestType_Heartbeat {
		var request pb.HeartbeatRequest
		err := common.Decode(req.Payload, &request)
		if err != nil {
			c.cluster.Log.Errorf("failed handle forward %s reqeuest: %v", req.RequestType.String(), err)
			return nil, err
		}
		// Check whether the metadata of the cluster has changed
		errorType := c.cluster.CheckNodeRouteForServiceName(request.ServiceName)
		if errorType != pb.ErrorType_None {
			res := c.doServiceHeartbeat(&request)
			return res, nil
		}
		forwardRes.ErrorType = errorType
		return forwardRes, nil
	}

	// Subscription requests do not need to be forwarded because the server needs to be directly
	// connected to the client to send service change notifications
	//if req.RequestType == pb.RequestType_Subscribe {
	//}

	return &pb.ForwardCliResponse{
		ErrorType: pb.ErrorType_UnknownError,
		ClusterId: c.cluster.ClusterId,
		LeaderId:  c.cluster.Leader.Id,
		Term:      c.cluster.Leader.Term,
	}, nil
}

func (c *ClusterRpcService) doServiceHeartbeat(req *pb.HeartbeatRequest) *pb.ForwardCliResponse {
	response := &pb.ForwardCliResponse{
		ErrorType: pb.ErrorType_None,
		ClusterId: c.cluster.ClusterId,
		LeaderId:  c.cluster.Leader.Id,
		Term:      c.cluster.Leader.Term,
	}

	bucketIndex := common.GetBucketIndex(req.ServiceName)
	bucket := c.cluster.SlotManager.GetSlotByIndex(bucketIndex)
	bucket.Heartbeat(req.ServiceName, req.ServiceIp, int(req.ServicePort))
	return response
}

func (c *ClusterRpcService) doRegisterService(req *pb.RegisterRequest) *pb.ForwardCliResponse {

	response := &pb.ForwardCliResponse{
		ErrorType: pb.ErrorType_None,
		ClusterId: c.cluster.ClusterId,
		LeaderId:  c.cluster.Leader.Id,
		Term:      c.cluster.Leader.Term,
	}

	bucketIndex := common.GetBucketIndex(req.ServiceName)
	bucket := c.cluster.SlotManager.GetSlotByIndex(bucketIndex)
	bucket.Register(req.ServiceName, req.ServiceIp, int(req.ServicePort))
	return response
}
