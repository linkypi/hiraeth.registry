package tcp

import (
	"encoding/json"
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/linkypi/hiraeth.registry/server/api/handler"
	"github.com/panjf2000/gnet"
	"time"
)

type RequestWrapper struct {
	Data    []byte
	Conn    gnet.Conn
	MsgType common.MessageType
}

func (s *Server) handleRequest(startCh chan struct{}) {
	var closed bool
	for {
		select {
		case <-s.shutDownCh:
			return
		case wrapper := <-s.Ch:
			s.handle(wrapper)
		default:
			if !closed {
				close(startCh)
				closed = true
			}
			time.Sleep(time.Millisecond * 50)
		}
	}
}

func (s *Server) handle(wrapper RequestWrapper) {
	s.log.Warnf("received request, msg type: %s", wrapper.MsgType.String())
	if wrapper.MsgType == common.RequestMsg {
		var req common.Request
		err := common.Decode(wrapper.Data, &req)
		if err != nil {
			s.log.Errorf("decode request error: %v", err)
			return
		}

		reqHandler := s.requestHandler.GetHandlerByRequestType(req.RequestType)
		if reqHandler != nil {
			_ = s.workerPool.Submit(func() {
				s.doHandle(reqHandler, req, wrapper.Conn)
			})
			return
		}
		return
	}

	if wrapper.MsgType == common.ResponseMsg {
		var res common.Response
		err := common.Decode(wrapper.Data, &res)
		if err != nil {
			s.log.Errorf("decode response error: %v", err)
			return
		}
		s.log.Debugf("received response, req id: %d, req type: %s", res.RequestId, res.RequestType.String())
		_ = s.workerPool.Submit(func() {
			s.handleResponse(res, wrapper.Conn)
		})
		return
	}
	s.log.Errorf("unknown message type: %d", wrapper.MsgType)
}

func (s *Server) handleResponse(res common.Response, c gnet.Conn) {

	s.log.Debugf("reply client response: %s, bytes: %d", res.RequestType.String(), 0)
}

func (s *Server) doHandle(handler handler.RequestHandler, request common.Request, conn gnet.Conn) {

	s.log.Debugf("received request, req type: %s, req id: %d", request.RequestType.String(), request.RequestId)

	response := handler.Handle(request, conn)
	bytes, err := response.ToBytes()

	jsonStr, _ := json.Marshal(request)
	if err != nil {
		s.log.Debugf("encode message error, msg: %s, %v", jsonStr, err)
	}

	err = conn.AsyncWrite(bytes)
	if err != nil {
		s.log.Errorf("async send message error, msg: %s, %v", jsonStr, err)
	}
	s.log.Warnf("reply client request: %s, bytes: %d", request.RequestType.String(), len(bytes))
}
