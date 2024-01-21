package handler

import (
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

type RequestHandler interface {
	Handle(req common.Request, con gnet.Conn) common.Response
}

type RequestHandlerFactory struct {
	log      *logrus.Logger
	handlers map[common.RequestType]RequestHandler
}

func (r *RequestHandlerFactory) GetHandlerByRequestType(requestType common.RequestType) RequestHandler {
	handler, ok := r.handlers[requestType]
	if ok {
		return handler
	}
	r.log.Warnf("no handler found for request type: %s", requestType.String())
	return nil
}

func NewHandlerFactory(serviceImpl *ServiceImpl, log *logrus.Logger) RequestHandlerFactory {

	requestHandlerFactory := RequestHandlerFactory{
		handlers: make(map[common.RequestType]RequestHandler),
		log:      log,
	}

	requestHandlerFactory.handlers[common.Register] = &RegisterHandler{Log: log, ServiceImpl: serviceImpl}
	requestHandlerFactory.handlers[common.Subscribe] = &SubHandler{Log: log, ServiceImpl: serviceImpl}
	requestHandlerFactory.handlers[common.Heartbeat] = &HeartbeatHandler{Log: log, ServiceImpl: serviceImpl}
	requestHandlerFactory.handlers[common.FetchServiceInstance] = &FetchServiceInstanceHandler{Log: log, ServiceImpl: serviceImpl}
	requestHandlerFactory.handlers[common.FetchMetadata] = &FetchMetadataHandler{Log: log, ServiceImpl: serviceImpl}
	return requestHandlerFactory
}
