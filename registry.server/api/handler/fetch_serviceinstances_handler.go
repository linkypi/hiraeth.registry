package handler

import (
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

type FetchServiceInstanceHandler struct {
	Log         *logrus.Logger
	ServiceImpl *ServiceImpl
}

func (r *FetchServiceInstanceHandler) getRequestType() common.RequestType {
	return common.FetchServiceInstance
}

func (r *FetchServiceInstanceHandler) Handle(req common.Request, con gnet.Conn) common.Response {

	res, err := r.ServiceImpl.FetchMetadata()
	if err != nil {
		r.Log.Errorf("failed to fetch meta data: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.Subscribe, err.Error(), 0)
	}
	payload, err := common.EncodePb(res)
	if err != nil {
		r.Log.Errorf("failed to fetch meta data: %v", err)
		return common.NewErrResponseWithMsg(req.RequestId, common.Subscribe, err.Error(), 0)
	}
	return common.NewOkResponseWithPayload(req.RequestId, common.Subscribe, payload)
}
