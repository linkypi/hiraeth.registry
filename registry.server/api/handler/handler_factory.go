package handler

import (
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/linkypi/hiraeth.registry/server/log"
	"github.com/panjf2000/gnet"
)

type RequestHandler interface {
	Handle(req common.Request, con gnet.Conn) common.Response
	getRequestType() common.RequestType
}

type RequestHandlerFactory struct {
	handlers map[common.RequestType]RequestHandler
}

func (r *RequestHandlerFactory) GetHandlerByRequestType(requestType common.RequestType) RequestHandler {
	handler, ok := r.handlers[requestType]
	if ok {
		return handler
	}
	return nil
}

func InitHandlerFactory(serviceImpl *ServiceImpl) RequestHandlerFactory {

	requestHandlerFactory := RequestHandlerFactory{
		handlers: make(map[common.RequestType]RequestHandler),
	}

	requestHandlerFactory.handlers[common.Register] = &RegisterHandler{Log: log.Log, ServiceImpl: serviceImpl}
	requestHandlerFactory.handlers[common.Subscribe] = &SubscribeHandler{Log: log.Log, ServiceImpl: serviceImpl}
	requestHandlerFactory.handlers[common.Heartbeat] = &HeartbeatHandler{Log: log.Log, ServiceImpl: serviceImpl}
	requestHandlerFactory.handlers[common.FetchServiceInstance] = &FetchServiceInstanceHandler{Log: log.Log, ServiceImpl: serviceImpl}
	requestHandlerFactory.handlers[common.FetchMetadata] = &FetchMetadataHandler{Log: log.Log, ServiceImpl: serviceImpl}
	return requestHandlerFactory
}
