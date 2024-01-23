package handler

import (
	"context"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/cluster"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"google.golang.org/protobuf/proto"
)

type RegisterHandler struct {
	Handler
	*cluster.Cluster
}

func NewRegisterHandler(cluster *cluster.Cluster) *RegisterHandler {
	return &RegisterHandler{
		Cluster: cluster,
	}
}

// GetRouteKey Use the routing key to find the machine where the data is located, and if it is on the local machine,
// the registration logic will be directly executed by Handle function, otherwise it needs to be forwarded to another machine for execution
func (r *RegisterHandler) GetRouteKey(realReq proto.Message, ctx context.Context) string {
	request := realReq.(*pb.RegisterRequest)
	return request.ServiceName
}

func (r *RegisterHandler) Handle(req any, bucket *slot.Bucket, ctx context.Context) (proto.Message, error) {
	request := req.(*pb.RegisterRequest)

	ip := request.ServiceIp
	port := request.ServicePort

	err := r.serviceImpl.DoRegister(bucket, request.ServiceName, ip, int(port))
	if err != nil {
		r.log.Errorf("failed to register service: %v", err)
		return nil, err
	}
	return &pb.RegisterResponse{Success: true}, nil
}
