package handler

import (
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type RegisterHandler struct {
	Log         *logrus.Logger
	ServiceImpl *ServiceImpl
}

func (r *RegisterHandler) Handle(req common.Request, con gnet.Conn) common.Response {
	request := pb.RegisterRequest{}
	err := proto.Unmarshal(req.Payload, &request)
	if err != nil {
		r.Log.Errorf("invalid register request, failed to unmarshal request: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.Register, "failed to unmarshal request", 0)
	}

	err = r.ServiceImpl.Register(request.ServiceName, request.ServiceIp, int(request.ServicePort))
	if err != nil {
		r.Log.Errorf("failed to register service: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.Register, err.Error(), 0)
	}
	return common.NewOkResponse(req.RequestId, common.Register)
}

func (r *RegisterHandler) Register(req *pb.RegisterRequest) (res *pb.RegisterResponse, err error) {
	return nil, nil
}
