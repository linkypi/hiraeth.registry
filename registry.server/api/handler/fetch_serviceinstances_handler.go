package handler

import (
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type FetchServiceInstanceHandler struct {
	Log         *logrus.Logger
	ServiceImpl *ServiceImpl
}

func (r *FetchServiceInstanceHandler) Handle(req common.Request, con gnet.Conn) common.Response {
	request := pb.FetchServiceRequest{}
	err := proto.Unmarshal(req.Payload, &request)
	if err != nil {
		r.Log.Errorf("invalid fetch service request, failed to unmarshal request: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.FetchServiceInstance, "failed to unmarshal request", 0)
	}

	res, err := r.ServiceImpl.FetchServiceInstance(request.ServiceName)
	if err != nil {
		r.Log.Errorf("failed to fetch meta data: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.FetchServiceInstance, err.Error(), 0)
	}
	payload, err := common.EncodePb(res)
	if err != nil {
		r.Log.Errorf("failed to fetch meta data: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.FetchServiceInstance, err.Error(), 0)
	}
	return common.NewOkResponseWithPayload(req.RequestId, common.FetchServiceInstance, payload)
}
