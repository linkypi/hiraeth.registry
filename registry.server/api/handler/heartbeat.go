package handler

import (
	"context"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/cluster"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"google.golang.org/protobuf/proto"
)

type HeartbeatHandler struct {
	*cluster.Cluster
	Handler
}

func (r *HeartbeatHandler) GetRouteKey(realReq proto.Message, ctx context.Context) string {
	request := realReq.(*pb.HeartbeatRequest)
	return request.ServiceName
}

func (r *HeartbeatHandler) Handle(req any, bucket *slot.Bucket, ctx context.Context) (proto.Message, error) {
	request := req.(*pb.HeartbeatRequest)

	serviceName := request.ServiceName
	ip := request.ServiceIp
	port := request.ServicePort

	_ = r.serviceImpl.DoHeartbeat(bucket, serviceName, ip, int(port))
	return nil, nil
}
