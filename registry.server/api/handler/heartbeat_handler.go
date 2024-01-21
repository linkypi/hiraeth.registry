package handler

import (
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type HeartbeatHandler struct {
	Log         *logrus.Logger
	ServiceImpl *ServiceImpl
}

func (r *HeartbeatHandler) Handle(req common.Request, con gnet.Conn) common.Response {
	request := pb.HeartbeatRequest{}
	err := proto.Unmarshal(req.Payload, &request)
	if err != nil {
		r.Log.Errorf("invalid service heartbeat request, failed to unmarshal request: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.Heartbeat, "failed to unmarshal request", 0)
	}

	err = r.ServiceImpl.Heartbeat(request.ServiceName, request.ServiceIp, int(request.ServicePort))
	if err != nil {
		r.Log.Errorf("failed to register service: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.Heartbeat, err.Error(), 0)
	}
	return common.NewOkResponse(req.RequestId, common.Heartbeat)
}
