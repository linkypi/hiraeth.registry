package tcp

import (
	"encoding/json"
	common "github.com/linkypi/hiraeth.registry/common"
	"github.com/linkypi/hiraeth.registry/server/api/handler"
	"github.com/linkypi/hiraeth.registry/server/cluster"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/linkypi/hiraeth.registry/server/log"
	cpb "github.com/linkypi/hiraeth.registry/server/proto"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"github.com/panjf2000/gnet"
	"github.com/panjf2000/gnet/pkg/logging"
	"github.com/panjf2000/gnet/pkg/pool/goroutine"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"strconv"
	"time"
)

type Server struct {
	*gnet.EventServer
	log         *logrus.Logger
	addr        string
	cluster     *cluster.Cluster
	slotManager *slot.Manager
	startUpMode config.StartUpMode
	netManager  *NetManager
	shutDownCh  chan struct{}

	heartbeatSec          int
	workerPool            *goroutine.Pool
	requestHandlerFactory handler.RequestHandlerFactory
}

func NewClientTcpServer(addr string, cluster *cluster.Cluster, startupMode config.StartUpMode,
	slotManager *slot.Manager, shutDownCh chan struct{}) *Server {

	netManager := NewNetManager()
	serviceImpl := handler.ServiceImpl{
		SlotManager: slotManager,
		Cluster:     cluster, Log: log.Log,
		StartUpMode: startupMode}
	handlerFactory := handler.InitHandlerFactory(&serviceImpl)
	tcpServer := Server{
		log:                   log.Log,
		addr:                  addr,
		cluster:               cluster,
		slotManager:           slotManager,
		startUpMode:           startupMode,
		netManager:            netManager,
		heartbeatSec:          3,
		workerPool:            goroutine.Default(),
		shutDownCh:            shutDownCh,
		requestHandlerFactory: handlerFactory,
	}
	serviceImpl.OnSubEvent = tcpServer.publishServiceChanged

	return &tcpServer
}

func (t *Server) OnInitComplete(srv gnet.Server) (action gnet.Action) {
	log.Log.Infof("client tcp server is listening on %s (multi-cores: %t, loops: %d)\n",
		srv.Addr.String(), srv.Multicore, srv.NumEventLoop)
	return
}
func (t *Server) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	t.log.Infof("connected to client: %s", c.RemoteAddr().String())
	t.netManager.AddConn(c)
	return
}

func (t *Server) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	t.log.Infof("client connection is closed, remote addr: %s, error: %s", c.RemoteAddr().String(), err.Error())
	t.netManager.RemoveConn(c.RemoteAddr().String())
	return
}

func (t *Server) Tick() (delay time.Duration, action gnet.Action) {
	t.netManager.SendHeartbeat()
	delay = time.Duration(t.heartbeatSec) * time.Millisecond
	return
}

func (t *Server) React(buffer []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	data := append([]byte{}, buffer...)
	msgType := data[0]
	data = data[1:]

	if msgType == uint8(common.RequestMsg) {
		var req common.Request
		err := common.Decode(data, &req)
		if err != nil {
			t.log.Errorf("decode request error: %v", err)
			return
		}
		t.log.Debugf("received request, req id: %d, req type: %s", req.RequestId, req.RequestType.String())
		_ = t.workerPool.Submit(func() {
			t.handleRequest(req, c)
		})
		return
	} else if msgType == uint8(common.ResponseMsg) {
		var res common.Response
		err := common.Decode(data, &res)
		if err != nil {
			t.log.Errorf("decode response error: %v", err)
			return
		}
		t.log.Debugf("received response, req id: %d, req type: %s", res.RequestId, res.RequestType.String())
		_ = t.workerPool.Submit(func() {
			t.handleResponse(res, c)
		})
		return
	}

	t.log.Errorf("unknown message type: %d", msgType)
	return
}

func (t *Server) handleResponse(res common.Response, c gnet.Conn) {

}

func (t *Server) handleRequest(req common.Request, c gnet.Conn) {

	reqHandler := t.requestHandlerFactory.GetHandlerByRequestType(req.RequestType)
	response := reqHandler.Handle(req, c)

	bytes, err := response.ToBytes()
	jsonStr, _ := json.Marshal(req)
	if err != nil {
		t.log.Errorf("encode message error, msg: %s, %v", jsonStr, err)
		return
	}

	err = c.AsyncWrite(bytes)
	if err != nil {
		t.log.Errorf("async send message error, msg: %s, %v", jsonStr, err)
	}
}

func (t *Server) publishServiceChanged(connIds []string, serviceName string, instances []common.ServiceInstance) {
	if t.netManager == nil {
		return
	}

	// check cluster state
	if t.startUpMode == config.Cluster && t.cluster.State != cluster.Active {
		t.log.Warnf("failed to publish service changed: %s, cluster %d is not active: %s",
			serviceName, t.cluster.ClusterId, t.cluster.State.String())
		return
	}

	request := cpb.ServiceChangedRequest{ServiceName: serviceName}

	for _, connId := range connIds {
		conn := t.netManager.GetConn(connId)
		if conn == nil {
			t.log.Warnf("failed to publish service changed: %s, conn [%s] not exist", serviceName, connId)
			continue
		}
		payload, err := proto.Marshal(&request)
		if err != nil {
			t.log.Errorf("failed to publish service changed: %s, %s", serviceName, err.Error())
			return
		}
		newRequest := common.NewRequest(common.PublishServiceChanged, payload)
		bytes, err := newRequest.ToBytes()
		if err != nil {
			t.log.Errorf("failed to publish service changed: %s, encode occur error: %s", serviceName, err.Error())
			return
		}
		err = conn.AsyncWrite(bytes)
		if err != nil {
			t.log.Errorf("failed to publish service changed: %s, async reply occur error: %s", serviceName, err.Error())
		}
	}
}

func (t *Server) OnShutdown(server gnet.Server) {
	t.log.Errorf("client server shutting down")
	t.Shutdown()
}

func (t *Server) Start(nodeId string) {

	machineId, err := strconv.Atoi(nodeId)
	if err != nil {
		machineId = 1
		t.log.Warnf("invalid nodeId, use default nodeId: %d", machineId)
	}

	err = common.InitSnowFlake("", int64(machineId))
	if err != nil {
		t.log.Errorf("init snowflake failed, err: %v", err)
		close(t.shutDownCh)
		return
	}

	go t.startTcpServer(t.addr)

	select {
	case <-t.shutDownCh:
		t.Shutdown()
		return
	}
}

func (t *Server) startTcpServer(addr string) {

	defer func() {
		if err := recover(); err != nil {
			t.log.Errorf("start tcp server panic: %v", err)
		}
	}()

	codec := gnet.NewLengthFieldBasedFrameCodec(common.EncoderConfig, common.DecoderConfig)
	err := gnet.Serve(t, addr,
		gnet.WithLogger(log.Log),
		gnet.WithLogLevel(logging.DebugLevel),
		gnet.WithCodec(codec),
		gnet.WithMulticore(true),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithReusePort(true),
		gnet.WithReuseAddr(true),
		gnet.WithNumEventLoop(4),
		gnet.WithTCPKeepAlive(time.Minute*5),
	)

	if err != nil {
		t.log.Errorf("start client server failed, err: %v", err)
		t.Shutdown()
	}
}

func (t *Server) Shutdown() {
	t.netManager.CloseAllConn()
	close(t.shutDownCh)
}
