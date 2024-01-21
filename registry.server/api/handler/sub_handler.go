package handler

import (
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"sync"
)

type SubHandler struct {
	sync.Map
	Log         *logrus.Logger
	ServiceImpl *ServiceImpl
}

const (
	AddrMapToServicePrefix = "addr_map_service_"
)

func (r *SubHandler) Handle(req common.Request, con gnet.Conn) common.Response {
	request := pb.SubRequest{}
	err := proto.Unmarshal(req.Payload, &request)
	if err != nil {
		r.Log.Errorf("invalid subscribe request, failed to unmarshal request: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.Subscribe, "failed to unmarshal request", 0)
	}

	addr := con.RemoteAddr().String()
	if request.SubType == pb.SubType_Subscribe {
		err = r.ServiceImpl.Subscribe(request.ServiceName, addr)
		if err != nil {
			r.Log.Errorf("failed to subscribe service: %v", err)
			errType := 0
			if err == common.ErrorMetadataChanged {
				errType = int(pb.ErrorType_MetaDataChanged.Number())
			}
			return common.NewErrResponseWithMsg(req.RequestId, common.Subscribe, err.Error(), uint8(errType))
		}

		r.Store(AddrMapToServicePrefix+addr, request.ServiceName)
		return common.NewOkResponse(req.RequestId, common.Subscribe)
	}

	if request.SubType == pb.SubType_UnSubscribe {
		key := AddrMapToServicePrefix + addr
		serviceName, ok := r.Load(key)
		if !ok {
			return common.NewOkResponse(req.RequestId, common.UnSubscribe)
		}

		_ = r.ServiceImpl.UnSubscribe(serviceName.(string), addr)
		r.Delete(key)
		return common.NewOkResponse(req.RequestId, common.UnSubscribe)
	}

	return common.NewErrResponse(req.RequestId, "", "invalid subscribe request")
}

func (r *SubHandler) UnSub(addr string) {
	key := AddrMapToServicePrefix + addr
	serviceName, ok := r.Load(key)
	if !ok {
		return
	}
	_ = r.ServiceImpl.UnSubscribe(serviceName.(string), addr)
	r.Delete(key)
	return
}
