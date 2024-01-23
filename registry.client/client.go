package client

import (
	"encoding/json"
	"errors"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/panjf2000/gnet/pkg/pool/goroutine"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"strconv"
	"sync"
	"time"
)

const (
	ConnKey = "CONN_KEY_"
	ConnMtx = "CON_MTX_"
)

type Client struct {
	sync.Map
	log        *logrus.Logger
	pool       *goroutine.Pool
	shutdownCh chan struct{}
	codec      common.ICodec

	//  only used for pulling metadata when the client is initialized
	serverAddrs []string
	readBufSize int

	turnOnServiceSubs bool

	metaData         MetaData
	serviceInstances []common.ServiceInstance

	serviceName string
	ip          string
	port        int

	havePulledMetadata bool
}

type MetaData struct {
	shards       map[string]common.Shard
	clusterNodes map[string]string
}

type Conn interface {
	Read([]byte) (int, error)
	SetReadDeadline(time time.Time) error
	Write(data []byte) (int, error)
	AsyncWrite(data []byte) error
	Close() error
}

// CreateClient addrs can be like: 127.0.0.1:8080,127.0.0.1:8081,127.0.0.1:8082 or 127.0.0.1:8080
func CreateClient(addrs string, shutdownCh chan struct{}, logger *logrus.Logger) (*Client, error) {
	return NewClient(addrs, nil, 4096, shutdownCh, logger)
}

// NewClient addrs can be like: 127.0.0.1:8080,127.0.0.1:8081,127.0.0.1:8082 or 127.0.0.1:8080
func NewClient(addrs string, codec common.ICodec, readBufSize int, shutdownCh chan struct{}, log *logrus.Logger) (*Client, error) {

	if addrs == "" {
		return nil, errors.New("addr can not be empty")
	}
	servers, ok := common.ParseIpPort(addrs)
	if !ok {
		return nil, errors.New("invalid cluster server address: " + addrs)
	}
	if codec == nil {
		codec = &common.BuildInFixedLengthCodec{Version: common.DefaultProtocolVersion}
	}

	if readBufSize <= 0 {
		readBufSize = 1024 * 4
	}

	client := &Client{
		log:         log,
		serverAddrs: servers,
		codec:       codec,
		readBufSize: readBufSize,
		pool:        goroutine.Default(),
		shutdownCh:  shutdownCh,
	}

	return client, nil
}

func init() {
	common.InitLogger("./logs", logrus.DebugLevel)
	_ = common.InitSnowFlake("", 1)
}

func (c *Client) SetCodec(codec common.ICodec) {
	c.codec = codec
}

func (c *Client) RegisterServiceAsync(serviceName, ip string, port int, callback func(response common.Response, err error)) error {
	requestKey, err := c.registerService(serviceName, ip, port)
	if err != nil {
		c.log.Errorf("failed to async register: %v", err)
		return err
	}
	c.serviceName = serviceName
	c.ip = ip
	c.port = port

	c.asyncWait(requestKey, func(a any, err2 error) {
		if err2 == nil {
			callback(a.(common.Response), nil)
			return
		}
		callback(common.Response{}, err2)
	})
	return nil
}

func (c *Client) sendHeartbeatsInPeriod() {

	for {
		select {
		case <-c.shutdownCh:
			return
		default:
			time.Sleep(time.Second * 5)
			requestKey, err := c.sendHeartbeat(c.serviceName, c.ip, c.port)
			if err != nil {
				c.log.Errorf("failed to send heartbeat: %v", err)
			}

			c.asyncWait(requestKey, func(a any, err error) {
				if err != nil {
					c.log.Warnf("failed to send heartbeat: %s, %v", requestKey, err)
				}
				c.log.Debug("send heartbeat success")
			})
		}
	}
}

func (c *Client) RegisterService(serviceName, ip string, port int, timeout time.Duration) (common.Response, error) {

	requestKey, err := c.registerService(serviceName, ip, port)
	if err != nil {
		return common.Response{}, err
	}

	res, err := c.syncWait(requestKey, timeout)
	if err != nil {
		return common.Response{}, err
	}
	response := res.(common.Response)
	if response.Success {
		c.serviceName = serviceName
		c.ip = ip
		c.port = port
		go c.sendHeartbeatsInPeriod()
		go func() {
			_ = c.fetchMetadata()
		}()
	}
	return response, nil
}

func (c *Client) RegisterServiceWithoutRoute(serviceName, addr, ip string, port int, timeout time.Duration) (common.Response, error) {

	requestKey, err := c.registerServiceWithoutRoute(serviceName, addr, ip, port)
	if err != nil {
		return common.Response{}, err
	}

	res, err := c.syncWait(requestKey, timeout)
	if err != nil {
		return common.Response{}, err
	}
	response := res.(common.Response)
	if response.Success {
		c.serviceName = serviceName
		c.ip = ip
		c.port = port
		go c.sendHeartbeatsInPeriod()
		go func() {
			_ = c.fetchMetadata()
		}()
	}
	return response, nil
}

func (c *Client) findRouteAndCheckConn(serviceName string) (string, error) {
	if c.metaData.shards == nil || len(c.metaData.shards) == 0 {
		err := c.fetchMetadata()
		if err != nil {
			return "", err
		}
	}
	index := common.GetBucketIndex(serviceName)
	serverId, err := common.GetNodeIdByIndex(index, c.metaData.shards)
	if err != nil {
		return "", err
	}
	addr, ok := c.metaData.clusterNodes[serverId]
	if !ok {
		return "", errors.New("node not found")
	}
	if _, ok = c.Load(ConnKey + addr); ok {
		return addr, nil
	}
	return c.checkConnection(addr)
}

func (c *Client) updateServiceInstances() error {
	response, err := c.FetchServiceInstances()
	if err != nil {
		return err
	}
	c.serviceInstances = make([]common.ServiceInstance, 0, len(response.ServiceInstances))
	for _, instance := range response.ServiceInstances {
		item := common.ServiceInstance{InstanceIp: instance.ServiceIp,
			InstancePort: int(instance.ServicePort),
			ServiceName:  instance.ServiceName}
		c.serviceInstances = append(c.serviceInstances, item)
	}
	c.log.Debugf("fetched service instances: %d", len(c.serviceInstances))
	return nil
}

func (c *Client) FetchServiceInstances() (*pb.FetchServiceResponse, error) {

	addr, err := c.findRouteAndCheckConn(c.serviceName)
	if err != nil {
		return nil, err
	}
	request := pb.FetchServiceRequest{
		ServiceName: c.serviceName,
	}
	requestKey, err := c.sendRequest(addr, common.FetchServiceInstance, &request)
	if err != nil {
		c.log.Warnf("fetch service instance request failed, err: %v", err)
		return nil, err
	}

	res, err := c.syncWait(requestKey, time.Second*10)
	if err != nil {
		return nil, err
	}
	response := res.(common.Response)
	sresp := &pb.FetchServiceResponse{}
	err = common.DecodeToPb(response.Payload, sresp)
	if err != nil {
		c.log.Warnf("decode fetch service instance response failed, err: %v", err)
		return nil, err
	}
	return sresp, nil
}

func (c *Client) waitForFetchedMetadata() {
	for {
		select {
		case <-c.shutdownCh:
			return
		default:
		}
		// If the metadata is pulled again, the error may be that the cluster is starting,
		// and you need to wait for the cluster to be in a normal state and pull it again
		err := c.fetchMetadata()
		if err != nil {
			time.Sleep(time.Second * 5)
			c.log.Errorf("fetch metadata error: %s", err)
			continue
		}
		c.log.Info("Re-pull the metadata success")
		break
	}
}

func (c *Client) fetchMetadata() error {

	addr := ""
	for {
		if c.havePulledMetadata {
			for _, serverAddr := range c.metaData.clusterNodes {
				_, err := c.checkConnection(serverAddr)
				if err != nil {
					c.log.Debugf("check connection failed, err: %v", err)
					continue
				}
				addr = serverAddr
				break
			}
		} else {
			for _, serverAddr := range c.serverAddrs {
				_, err := c.checkConnection(serverAddr)
				if err != nil {
					c.log.Debugf("check connection failed, err: %v", err)
					continue
				}
				addr = serverAddr
				break
			}
		}
		if addr != "" {
			break
		}
		time.Sleep(time.Second * 5)
	}

	response, err := c.FetchMetadata(addr)
	if err != nil {
		c.log.Errorf("failed to fetch metadata: %v", err)
		return err
	}

	if !response.Success {
		if response.ErrorType == uint8(pb.ErrorType_ClusterDown.Number()) {
			c.log.Errorf("failed to fetch metadata, cluster down, try to connect other nodes")
			err = c.connectOtherServers(addr)
			if err != nil {
				return err
			}
			return c.fetchMetadata()
		}
		c.log.Errorf("failed to fetch metadata: %v", response.Msg)
		return err
	}
	var metadata pb.FetchMetadataResponse
	err = proto.Unmarshal(response.Payload, &metadata)
	if err != nil {
		c.log.Errorf("failed to decode metadata: %v", err)
		return err
	}
	if metadata.ErrorType != pb.ErrorType_None {
		c.log.Errorf("failed to fetch metadata: %v", metadata.ErrorType.String())
		return err
	}
	var shards map[string]common.Shard
	err = json.Unmarshal([]byte(metadata.Shards), &shards)
	if err != nil {
		c.log.Errorf("failed to unmarshal shard json: %v", err)
		return err
	}
	c.metaData = MetaData{
		shards:       shards,
		clusterNodes: metadata.Nodes,
	}
	c.log.Debugf("fetch metadata success: %s", metadata.Shards)
	c.havePulledMetadata = true

	go c.ensureConnectedToQuorumServers()
	return nil
}

func (c *Client) ensureConnectedToQuorumServers() {
	allConnected := true
	time.NewTimer(time.Second * 10)
	for {
		select {
		case <-c.shutdownCh:
			return
		default:
		}

		allConnected = true
		for _, addr := range c.metaData.clusterNodes {
			_, err := c.checkConnection(addr)
			if err != nil {
				allConnected = false
				time.Sleep(time.Second * 5)
				continue
			}
		}
		if allConnected {
			break
		}
	}
}

func (c *Client) connectOtherServers(addr string) error {
	c.log.Infof("try to connect other cluster server")
	if len(c.serverAddrs) == 1 {
		c.log.Debugf("only one cluster server, no need to connect other servers")
		return errors.New("only one cluster server")
	}

	for _, sAddr := range c.serverAddrs {
		if sAddr == addr {
			continue
		}
		c.log.Infof("connect to server: %s", sAddr)
		_, err := c.createConn(sAddr)
		if err != nil {
			time.Sleep(time.Second * 5)
			continue
		}
		c.log.Infof("connected to other cluster server: %s", sAddr)
		return nil
	}
	return errors.New("failed to connect other cluster server")
}

func (c *Client) Subscribe(serviceName string, timeout time.Duration) (common.Response, error) {
	c.serviceName = serviceName
	requestKey, err := c.subscribe(serviceName)
	if err != nil {
		return common.Response{}, err
	}
	res, err := c.syncWait(requestKey, timeout)
	if err != nil {
		return common.Response{}, err
	}
	var response = res.(common.Response)
	// fetch metadata and try again
	if response.ErrorType == uint8(pb.ErrorType_MetaDataChanged.Number()) {
		err := c.fetchMetadata()
		if err != nil {
			return response, err
		}
		return c.Subscribe(serviceName, timeout)
	}
	c.turnOnServiceSubs = true
	return response, nil
}

func (c *Client) FetchMetadata(addr string) (common.Response, error) {
	requestKey, err := c.sendRequest(addr, common.FetchMetadata, nil)
	if err != nil {
		return common.Response{}, err
	}

	res, err := c.syncWait(requestKey, 30*time.Second)
	if err != nil {
		return common.Response{}, err
	}
	return res.(common.Response), nil
}

func (c *Client) syncWait(requestKey string, timeout time.Duration) (any, error) {
	after := time.After(timeout)
	for {
		select {
		case <-after:
			c.Delete(requestKey)
			return nil, errors.New("request timeout")
		case <-c.shutdownCh:
			return nil, errors.New("client shutdown")
		default:
			value, ok := c.Load(requestKey)
			if ok && value != nil {
				c.Delete(requestKey)
				return value, nil
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *Client) asyncWait(requestKey string, callback func(any, error)) {
	timer := time.NewTimer(time.Second * 60) // To prevent syncing.Map memory leak, set a timeout
	_ = c.pool.Submit(func() {
		for {
			select {
			case <-timer.C:
				timer.Stop()
				c.Delete(requestKey)
				callback(nil, errors.New("request timeout"))
				return
			case <-c.shutdownCh:
				c.Delete(requestKey)
				callback(nil, errors.New("client shutdown"))
				return
			default:
				value, ok := c.Load(requestKey)
				if ok && value != nil {
					c.Delete(requestKey)
					callback(value, nil)
					return
				}
				time.Sleep(time.Millisecond * 100)
			}
		}
	})
}

func (c *Client) sendHeartbeat(serviceName string, ip string, port int) (string, error) {
	heartbeatRequest := pb.HeartbeatRequest{
		ServiceName: serviceName,
		ServiceIp:   ip,
		ServicePort: int32(port),
	}
	connKey, err := c.findRouteAndCheckConn(serviceName)
	if err != nil {
		return "", err
	}
	reqKey, err := c.sendRequest(connKey, common.Heartbeat, &heartbeatRequest)
	return reqKey, err
}

func (c *Client) registerService(serviceName string, ip string, port int) (string, error) {
	regRequest := pb.RegisterRequest{
		ServiceName: serviceName,
		ServiceIp:   ip,
		ServicePort: int32(port),
	}
	serverAddr, err := c.findRouteAndCheckConn(serviceName)
	if err != nil {
		return "", err
	}
	return c.sendRequest(serverAddr, common.Register, &regRequest)
}

func (c *Client) registerServiceWithoutRoute(serviceName, serverAddr string, ip string, port int) (string, error) {
	regRequest := pb.RegisterRequest{
		ServiceName: serviceName,
		ServiceIp:   ip,
		ServicePort: int32(port),
	}
	return c.sendRequest(serverAddr, common.Register, &regRequest)
}

func (c *Client) sendResponse(serviceName string, requestId uint64, res proto.Message) (uint64, error) {
	serverId, err := c.findRouteAndCheckConn(serviceName)
	if err != nil {
		return 0, err
	}
	toBytes, err := common.BuildResponseToBytes(requestId, res)
	if err != nil {
		c.log.Errorf("Error encoding pb: %v", err)
		return 0, err
	}
	addr, err := c.getAddr(serverId)
	if err != nil {
		return 0, err
	}
	con, ok := c.Load(ConnKey + addr)
	if !ok {
		return 0, errors.New("failed to send request, connection not found: " + addr)
	}
	conn := con.(*Conn)
	_, err = (*conn).Write(toBytes)
	if err != nil {
		return 0, err
	}
	return requestId, nil
}

func (c *Client) getAddr(serverId string) (string, error) {
	addr, ok := c.metaData.clusterNodes[serverId]
	if !ok {
		c.log.Errorf("failed to send response, server addr not found, serverId: %s", serverId)
		return "", errors.New("failed to send response, server addr not found")
	}
	return addr, nil
}

func (c *Client) sendRequest(addr string, requestType common.RequestType, request proto.Message) (string, error) {
	requestId, toBytes, err := common.BuildRequestToBytes(requestType, request)
	if err != nil {
		c.log.Errorf("Error encoding pb: %v", err)
		return "", err
	}
	con, ok := c.Load(ConnKey + addr)
	if !ok {
		return "", errors.New("failed to send request, connection not found: " + addr)
	}
	if con == nil {
		return "", errors.New("failed to send request, connection not found: " + addr)
	}
	conn := con.(*Conn)
	_, err = (*conn).Write(toBytes)
	if err != nil {
		return "", err
	}
	requestKey := requestType.String() + strconv.Itoa(int(requestId))
	c.Store(requestKey, nil)
	c.log.Debugf("send request: %s to %s", requestType.String(), addr)
	return requestKey, nil
}

func (c *Client) subscribe(serviceName string) (string, error) {
	subRequest := pb.SubRequest{
		ServiceName: serviceName,
		SubType:     pb.SubType_Subscribe,
	}
	connKey, err := c.findRouteAndCheckConn(serviceName)
	if err != nil {
		return "", err
	}
	return c.sendRequest(connKey, common.Subscribe, &subRequest)
}

func (c *Client) Close() {
	c.Range(func(key, cn interface{}) bool {
		conn := cn.(*Conn)
		err := (*conn).Close()
		if err != nil {
			c.log.Errorf("failed to close server connection: %s, %v", key, err)
		}
		c.Delete(key)
		return true
	})

}

func (c *Client) checkConnection(addr string) (string, error) {
	if _, ok := c.Load(ConnKey + addr); !ok {
		conn, err := c.createConn(addr)
		if err != nil {
			return "", err
		}
		c.Store(ConnKey+addr, &conn)
	}
	return addr, nil
}

func (c *Client) onReceive(bytes []byte, err error) {
	if err != nil {
		c.log.Errorf("failed to receive data: %v", err)
		return
	}
	data := append([]byte{}, bytes...)
	msgType := bytes[0]
	data = data[1:]
	if (msgType) == uint8(common.RequestMsg) {
		var req common.Request
		err := common.Decode(data, &req)
		if err != nil {
			c.log.Errorf("failed decode msg to request: %v", err)
			return
		}
		if req.RequestType == common.PublishServiceChanged {
			go func() {
				_ = c.updateServiceInstances()
			}()
		}
		//requestKey := req.RequestType.String() + strconv.Itoa(int(req.RequestId))
		//c.Store(requestKey, req)
	} else if (msgType) == uint8(common.ResponseMsg) {
		var res common.Response
		err := common.Decode(data, &res)
		if err != nil {
			c.log.Errorf("failed decode msg to response: %v", err)
			return
		}
		requestKey := res.RequestType.String() + strconv.Itoa(int(res.RequestId))
		c.log.Debugf("receive response : %s", requestKey)
		c.Store(requestKey, res)
	} else {
		c.log.Errorf("invalid msg: %v", err)
		return
	}
}

func (c *Client) createConn(addr string) (Conn, error) {

	exCon, err, creating := c.checkConnCreatingOrNot(addr)
	if creating {
		return exCon, err
	}

	lockKey := ConnMtx + addr
	// add conn lock
	c.Store(lockKey, addr)
	defer func() {
		c.Delete(lockKey)
	}()
	c.log.Debugf("create conn %s", addr)
	con, err := c.createConnInternal(addr)
	if err != nil {
		return nil, err
	}
	c.Store(ConnKey+addr, &con)
	return con, nil
}

func (c *Client) checkConnCreatingOrNot(addr string) (Conn, error, bool) {
	lockKey := ConnMtx + addr
	_, ok := c.Load(lockKey)

	// If there is already a goroutine creating the connection
	// timeout waits for the previous connection to be created, and the timeout period is 10 seconds
	timer := time.NewTimer(time.Second * 10)
	for ok {
		// Check if the lock is released
		if _, ok = c.Load(lockKey); !ok {
			value, exist := c.Load(ConnKey + addr)
			if exist {
				conn := value.(*Conn)
				return *conn, nil, true
			}
		}
		select {
		case <-c.shutdownCh:
			return nil, errors.New("shutdown"), true
		case <-timer.C:
			return nil, errors.New("create conn timeout"), true
		default:
		}
	}
	return nil, nil, false
}
