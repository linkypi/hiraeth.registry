package handler

import (
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type SubscribeHandler struct {
	Log         *logrus.Logger
	ServiceImpl *ServiceImpl
}

func (r *SubscribeHandler) getRequestType() common.RequestType {
	return common.Register
}

func (r *SubscribeHandler) Handle(req common.Request, con gnet.Conn) common.Response {
	request := pb.SubscribeRequest{}
	err := proto.Unmarshal(req.Payload, &request)
	if err != nil {
		r.Log.Errorf("invalid subscribe request, failed to unmarshal request: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.Subscribe, "failed to unmarshal request", 0)
	}

	err = r.ServiceImpl.Subscribe(request.ServiceName, con.RemoteAddr().String())
	if err != nil {
		r.Log.Errorf("failed to subscribe service: %v", err)
		errType := 0
		if err == common.ErrorMetadataChanged {
			errType = int(pb.ErrorType_MetaDataChanged.Number())
		}
		return common.NewErrResponseWithMsg(req.RequestId, common.Subscribe, err.Error(), uint8(errType))
	}
	return common.NewOkResponse(req.RequestId, common.Subscribe)
}
