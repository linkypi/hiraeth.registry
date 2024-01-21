package handler

import (
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

type FetchMetadataHandler struct {
	Log         *logrus.Logger
	ServiceImpl *ServiceImpl
}

func (r *FetchMetadataHandler) Handle(req common.Request, con gnet.Conn) common.Response {

	res, err := r.ServiceImpl.FetchMetadata()
	if err != nil {
		r.Log.Errorf("failed to fetch meta data: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.FetchMetadata, err.Error(), 0)
	}
	if res.ErrorType != pb.ErrorType_None {
		return common.NewErrResponseWithMsg(req.RequestId, common.FetchMetadata, "", uint8(res.ErrorType))
	}
	payload, err := common.EncodePb(res)
	if err != nil {
		r.Log.Errorf("failed to fetch meta data: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.FetchMetadata, err.Error(), 0)
	}
	return common.NewOkResponseWithPayload(req.RequestId, common.FetchMetadata, payload)
}
