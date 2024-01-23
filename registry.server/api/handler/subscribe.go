package handler

import (
	"context"
	"errors"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"google.golang.org/protobuf/proto"
	"sync"
)

type SubHandler struct {
	sync.Map
	Handler
}

const (
	AddrMapToServicePrefix = "addr_map_service_"
)

func (r *SubHandler) GetRouteKey(realReq proto.Message, ctx context.Context) string {
	request := realReq.(*pb.SubRequest)
	if request.SubType == pb.SubType_Subscribe {
		return request.ServiceName
	}
	addr := ctx.Value(common.RemoteAddr).(string)
	value, ok := r.Load(AddrMapToServicePrefix + addr)
	if ok {
		return value.(string)
	}
	return ""
}

func (r *SubHandler) Handle(req any, bucket *slot.Bucket, ctx context.Context) (proto.Message, error) {
	request := req.(*pb.SubRequest)
	addr := ctx.Value(common.RemoteAddr).(string)
	if request.SubType == pb.SubType_Subscribe {
		err := r.serviceImpl.Subscribe(bucket, request.ServiceName, addr)
		if err != nil {
			r.log.Errorf("failed to subscribe service: %v", err)
			//errType := 0
			//if err == common.ErrorMetadataChanged {
			//	errType = int(pb.ErrorType_MetaDataChanged.Number())
			//}
			return nil, errors.New(err.Error())
		}

		r.Store(AddrMapToServicePrefix+addr, request.ServiceName)
		return nil, nil
	}

	if request.SubType == pb.SubType_UnSubscribe {
		key := AddrMapToServicePrefix + addr
		serviceName, ok := r.Load(key)
		if !ok {
			return nil, nil
		}

		_ = r.serviceImpl.UnSubscribe(bucket, serviceName.(string), addr)
		r.Delete(key)
		return nil, nil
	}

	return nil, errors.New("unknown sub type")
}
