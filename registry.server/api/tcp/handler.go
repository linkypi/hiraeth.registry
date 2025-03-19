package tcp

import (
	"encoding/json"
	"github.com/linkypi/hiraeth.registry/common"
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
	common.Debugf("received request, msg type: %s", wrapper.MsgType.String())
	if wrapper.MsgType == common.RequestMsg {
		var req common.Request
		err := common.Decode(wrapper.Data, &req)
		if err != nil {
			common.Errorf("decode request error: %v", err)
			return
		}

		_ = s.workerPool.Submit(func() {
			s.doHandle(req, wrapper.Conn)
		})
		return
	}

	if wrapper.MsgType == common.ResponseMsg {
		var res common.Response
		err := common.Decode(wrapper.Data, &res)
		if err != nil {
			common.Errorf("decode response error: %v", err)
			return
		}
		common.Debugf("received response, req id: %d, req type: %s", res.RequestId, res.RequestType.String())
		_ = s.workerPool.Submit(func() {
			s.handleResponse(res, wrapper.Conn)
		})
		return
	}
	common.Errorf("unknown message type: %d", wrapper.MsgType)
}

func (s *Server) handleResponse(res common.Response, c gnet.Conn) {

	common.Debugf("reply client response: %s, bytes: %d", res.RequestType.String(), 0)
}

func (s *Server) doHandle(request common.Request, conn gnet.Conn) {

	common.Debugf("received request, req type: %s, req id: %d", request.RequestType.String(), request.RequestId)
	res, forwarded, err := s.handlerFactory.Handle(request, conn)
	if err != nil {
		common.Warnf("encode response error: %v", err)
		return
	}
	jsonStr, _ := json.Marshal(request)
	bytes, err := res.ToBytes()
	err = conn.AsyncWrite(bytes)
	if err != nil {
		common.Errorf("async send message error, msg: %s, %v", jsonStr, err)
	}
	common.Debugf("reply client request: %s, id: %d, bytes: %d", request.RequestType.String(), request.RequestId, len(bytes))

	// Check whether the processing results need to be replicated to replicas
	// After the primary shard data is updated, it needs to be synchronized asynchronously to its replica shard
	if !forwarded {
		s.syner.MaybeForwardToReplicas(request, *res)
	}
}
