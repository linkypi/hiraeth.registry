package handler

import (
	"context"
	"errors"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/cluster"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/linkypi/hiraeth.registry/server/service"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type IReqHandler interface {
	Handle(realReq any, bucket *slot.Bucket, ctx context.Context) (proto.Message, error)
	GetRouteKey(realReq proto.Message, ctx context.Context) string
}

type Handler struct {
	requestType common.RequestType
	handler     IReqHandler
	NeedRoute   bool
	NeedForward bool
	Request     proto.Message

	log         *logrus.Logger
	serviceImpl *service.RegistryImpl
}

type RequestHandlerFactory struct {
	*cluster.Cluster
	log      *logrus.Logger
	handlers map[common.RequestType]Handler
}

// Handle The bool type in the returned result is used to indicate whether the current request has been forwarded to another machine for processing
func (r *RequestHandlerFactory) Handle(req common.Request, conn gnet.Conn) (*common.Response, bool, error) {

	h := r.getHandler(req.RequestType)
	if h.requestType == common.Nop {
		r.log.Warnf("no handler found for request: %s", req.RequestType.String())
		return nil, false, errors.New("no handler found for request: " + req.RequestType.String())
	}
	if h.Request != nil {
		err := common.DecodeToPb(req.Payload, h.Request)
		if err != nil {
			r.log.Warnf("decode request failed: %s", err.Error())
			return nil, false, err
		}
	}
	realReq := h.Request
	ctx := context.WithValue(context.Background(), common.RemoteAddr, conn.RemoteAddr().String())
	routeKey := h.handler.GetRouteKey(h.Request, ctx)
	if !h.NeedRoute {
		bucketIndex := common.GetBucketIndex(routeKey)
		bucket := r.SlotManager.GetSlotByIndex(bucketIndex)
		r.log.Warnf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  request [%s], route key: %s, bucket index: %d -> %p, slot mgr: %p",
			req.RequestType.String(), routeKey, bucketIndex, bucket, r.SlotManager)
		res, err := r.handle(req, ctx, h.handler, realReq, bucket)
		return res, false, err
	}

	bucketIndex := common.GetBucketIndex(routeKey)
	bucket := r.SlotManager.GetSlotByIndex(bucketIndex)
	r.log.Warnf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  request [%s], route key: %s, bucket index: %d -> %p, slot mgr: %p",
		req.RequestType.String(), routeKey, bucketIndex, bucket, r.SlotManager)
	if bucketIndex == 0 {
		r.log.Warnf(">>>>>>>>>>>>>>>>>>>>> buket index: %d", bucketIndex)
	}
	// If it is a stand-alone mode, you can register directly
	if r.StartUpMode == config.StandAlone {
		res, err := r.handle(req, ctx, h.handler, realReq, bucket)
		return res, false, err
	}

	// If it is a cluster mode, you need to check the cluster status first
	if r.State != cluster.Active {
		r.Log.Errorf("failed to find the route: %s, cluster state not active: %v", routeKey, r.State.String())
		return nil, false, errors.New("cluster state not active: " + r.State.String())
	}

	nodeId, err := r.GetNodeIdByIndex(bucketIndex)
	if err != nil {
		r.Log.Errorf("failed to find node id for the target: %s, %v", routeKey, err.Error())
		return nil, false, errors.New("target node id not found")
	}
	if nodeId == r.SelfNode.Id {
		res, err := r.handle(req, ctx, h.handler, realReq, bucket)
		return res, false, err
	}

	// For some special requests, we cannot forward, such as service subscriptions
	// Subscription requests do not need to be forwarded because the server needs to be directly
	// connected to the client to send service change notifications
	if !h.NeedForward {
		return nil, false, nil
	}
	payload, err := common.EncodePb(realReq)
	if err != nil {
		return nil, false, err
	}
	reqType := common.ConvertReqType(req.RequestType)
	response, err := r.ForwardRequest(nodeId, reqType, false, payload)
	if err != nil {
		return nil, true, err
	}
	comRes := common.NewResponseWithReqType(req.RequestId, response.Payload, req.RequestType)
	comRes.ErrorType = uint8(response.ErrorType)
	return &comRes, true, nil
}

func (r *RequestHandlerFactory) handle(req common.Request, ctx context.Context, handler IReqHandler, realReq any, bucket *slot.Bucket) (*common.Response, error) {

	pbResult, err := handler.Handle(realReq, bucket, ctx)
	if err != nil {
		res := common.NewErrRes(err.Error())
		res.RequestId = req.RequestId
		res.RequestType = req.RequestType
		return &res, nil
	}
	payload, err := common.EncodePb(pbResult)
	if err != nil {
		r.Log.Errorf("failed to fetch meta data: %v", err)
		return nil, err
	}
	response := common.NewOkResponseWithPayload(req.RequestId, common.FetchServiceInstance, payload)
	response.RequestId = req.RequestId
	response.RequestType = req.RequestType
	return &response, err
}

func (r *RequestHandlerFactory) registerHandler(handler Handler) {
	if handler.requestType == common.Nop {
		r.log.Warnf("handler request type unknown: %s", handler.requestType.String())
		panic("handler request type unknown: " + handler.requestType.String())
	}
	if handler.handler == nil {
		r.log.Warnf("handler not implement IReqHandler: %s", handler.requestType.String())
		panic("handler not implement IReqHandler: " + handler.requestType.String())
	}
	r.handlers[handler.requestType] = handler
}

func (r *RequestHandlerFactory) getHandler(requestType common.RequestType) Handler {
	handler, ok := r.handlers[requestType]
	if ok {
		return handler
	}
	return Handler{}
}

func NewHandlerFactory(cl *cluster.Cluster, serviceImpl *service.RegistryImpl, log *logrus.Logger) *RequestHandlerFactory {

	handlerFactory := RequestHandlerFactory{
		log:      log,
		Cluster:  cl,
		handlers: make(map[common.RequestType]Handler),
	}
	regHandler := NewRegisterHandler(cl)
	h := Handler{requestType: common.Register, Request: &pb.RegisterRequest{}, log: log, serviceImpl: serviceImpl,
		NeedForward: true, NeedRoute: true, handler: regHandler}
	regHandler.Handler = h
	handlerFactory.registerHandler(h)

	subHandler := &SubHandler{}
	h = Handler{requestType: common.Subscribe, Request: &pb.SubRequest{},
		log: log, serviceImpl: serviceImpl, NeedForward: false, NeedRoute: false, handler: subHandler}
	subHandler.Handler = h
	handlerFactory.registerHandler(h)

	heartbeatHandler := &HeartbeatHandler{}
	h = Handler{requestType: common.Heartbeat, Request: &pb.HeartbeatRequest{},
		log: log, serviceImpl: serviceImpl, NeedForward: true, NeedRoute: true, handler: heartbeatHandler}
	heartbeatHandler.Handler = h
	handlerFactory.registerHandler(h)

	fetchHandler := &FetchMetadataHandler{}
	h = Handler{requestType: common.FetchMetadata, Request: nil,
		log: log, serviceImpl: serviceImpl, NeedForward: false, NeedRoute: false, handler: fetchHandler}
	fetchHandler.Handler = h
	handlerFactory.registerHandler(h)

	fetchInstanceHandler := &FetchServiceInstanceHandler{}
	h = Handler{requestType: common.FetchServiceInstance, Request: &pb.FetchServiceRequest{},
		log: log, serviceImpl: serviceImpl, NeedForward: false, NeedRoute: false, handler: fetchInstanceHandler}
	fetchInstanceHandler.Handler = h
	handlerFactory.registerHandler(h)

	return &handlerFactory
}
