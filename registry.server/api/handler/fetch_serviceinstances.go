package handler

import (
	"context"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"google.golang.org/protobuf/proto"
)

type FetchServiceInstanceHandler struct {
	Handler
}

func (r *FetchServiceInstanceHandler) GetRouteKey(realReq proto.Message, ctx context.Context) string {
	request := realReq.(*pb.FetchServiceRequest)
	return request.ServiceName
}

func (r *FetchServiceInstanceHandler) Handle(req any, bucket *slot.Bucket, ctx context.Context) (proto.Message, error) {

	request := req.(*pb.FetchServiceRequest)
	serviceName := request.ServiceName

	res, err := r.serviceImpl.DoGetServiceInstance(serviceName, bucket)

	if err != nil {
		common.Errorf("failed to fetch meta data: %v", err)
		return nil, err
	}
	return res, nil
}
