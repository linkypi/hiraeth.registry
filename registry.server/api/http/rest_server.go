package http

import (
	"context"
	"github.com/emicklei/go-restful"
	"github.com/linkypi/hiraeth.registry/common"
	cpb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/api/handler"
	"github.com/linkypi/hiraeth.registry/server/log"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"github.com/sirupsen/logrus"
	"net/http"
	"time"
)

type RestServer struct {
	log            *logrus.Logger
	server         *http.Server
	slotManager    *slot.Manager
	addr           string
	handlerFactory *handler.RequestHandlerFactory
	shutDownCh     chan struct{}
}

func NewClientRestServer(addr string, slotManager *slot.Manager, handlerFactory *handler.RequestHandlerFactory, shutDownCh chan struct{}) *RestServer {

	server := RestServer{
		log:            log.Log,
		addr:           addr,
		shutDownCh:     shutDownCh,
		slotManager:    slotManager,
		handlerFactory: handlerFactory,
	}
	return &server
}

func (s *RestServer) Start() {
	wsContainer := restful.NewContainer()
	s.buildService(wsContainer)
	// Add container filter to enable CORS
	cors := restful.CrossOriginResourceSharing{
		ExposeHeaders:  []string{"Hiraeth-Registry"},
		AllowedHeaders: []string{"Content-Type", "Accept"},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTION"},
		CookiesAllowed: false,
		Container:      wsContainer}
	wsContainer.Filter(cors.Filter)

	// Add container filter to respond to OPTIONS
	wsContainer.Filter(wsContainer.OPTIONSFilter)
	server := &http.Server{Addr: s.addr, Handler: wsContainer}

	log.Log.Infof("start http server on: %s\n", s.addr)
	err := server.ListenAndServe()
	if err != nil {
		s.shutDown()
	}
}

func (s *RestServer) shutDown() {
	timeOutCtx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	err := s.server.Shutdown(timeOutCtx)
	if err != nil {
		s.log.Errorf("failed to shut down rest server: %v", err)
	} else {
		s.log.Infof("rest server is down")
	}
	close(s.shutDownCh)
}

func (s *RestServer) buildService(container *restful.Container) {
	webService := new(restful.WebService)
	webService.
		Path("/api").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	webService.Route(webService.POST("/register").
		Doc("register a service instance").To(s.handleRegister))
	//Param(service.PathParameter("user-id", "identifier of the user").DataType("string")).

	webService.Route(webService.POST("/subscribe").
		Doc("subscribe a service").To(s.handleSubscribe))
	//service.Route(service.PUT("/{key}").To(Update))

	container.Add(webService)
}

func (s *RestServer) handleRegister(request *restful.Request, response *restful.Response) {
	var req RegisterRequest
	err := request.ReadEntity(&req)
	if err != nil {
		s.replyMsg(response, err.Error())
		return
	}

	regRequest := cpb.RegisterRequest{ServiceName: req.ServiceName, ServiceIp: req.Ip, ServicePort: int32(req.Port)}
	bytes, err := common.EncodePb(&regRequest)
	if err != nil {
		s.log.Errorf("encode sub request failed, error: %v", err)
		s.replyMsg(response, err.Error())
	}
	_ = common.Message{RequestType: common.Register, Payload: bytes, RequestId: uint64(common.GenerateId())}

	//_, _, err = s.handlerFactory.Handle(common.Request{Message: msg})
	//if err != nil {
	//	s.replyMsg(response, err.Error())
	//	return
	//}
	s.replySuccess(response)
}

func (s *RestServer) replySuccess(response *restful.Response) {
	err := response.WriteEntity(&RestResult{
		Success: true,
	})
	if err != nil {
		s.log.Errorf("failed to write response: %v", err)
	}
}

func (s *RestServer) replyMsg(response *restful.Response, msg string) {
	err := response.WriteEntity(FiledResultWithMsg(msg))
	if err != nil {
		s.log.Errorf("failed to write response: %v", err)
	}
}

func (s *RestServer) handleSubscribe(request *restful.Request, response *restful.Response) {

}
