package client

import (
	"encoding/json"
	"errors"
	pb "github.com/linkypi/hiraeth.registry/client/proto"
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/panjf2000/gnet"
	"github.com/panjf2000/gnet/pkg/pool/goroutine"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Client struct {
	sync.Map
	log          *logrus.Logger
	pool         *goroutine.Pool
	shutdownCh   chan struct{}
	connMap      map[string]*Conn
	addr         string
	readBufSize  int
	readCallback ReadCallBack
	eventHandler gnet.EventHandler

	shards       map[string]common.Shard
	clusterNodes map[string]string

	turnOnServiceSubs bool
	serviceInstances  []common.ServiceInstance

	serviceName string
	ip          string
	port        int
}
type Conn interface {
	Read([]byte) (int, error)
	SetReadDeadline(time time.Time) error
	Write(data []byte) (int, error)
	AsyncWrite(data []byte) error
	Close() error
}

func NewClient(readBufSize int, shutdownCh chan struct{}, log *logrus.Logger) *Client {
	client := &Client{
		log:         log,
		connMap:     make(map[string]*Conn),
		readBufSize: readBufSize,
		pool:        goroutine.Default(),
		shutdownCh:  shutdownCh,
	}

	return client
}

// SetReadCallBack Only for Windows, as for Linux, the darwin system recommends using SetEventHandler
func (c *Client) SetReadCallBack(readCallback ReadCallBack) {
	c.readCallback = readCallback
}

// SetEventHandler For Linux, Darwin only, as for Windows system recommends using SetReadCallBack
func (c *Client) SetEventHandler(handler gnet.EventHandler) {
	c.eventHandler = handler
}

func (c *Client) RegisterAsync(serviceName, ip string, port int, callback func(response common.Response, err error)) error {
	requestKey, err := c.register(serviceName, ip, port)
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
				if strings.Contains(err.Error(), "connection refused") {

				}
			}

			c.asyncWait(requestKey, func(a any, err error) {
				if err != nil {
					c.log.Warnf("failed to send heartbeat: %v", err)
				}
				c.log.Debug("send heartbeat success")
			})
		}
	}
}

func (c *Client) Register(serviceName, ip string, port int, timeout time.Duration) (common.Response, error) {

	requestKey, err := c.register(serviceName, ip, port)
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

func (c *Client) checkMetadataAndConnection(serviceName string) (string, error) {
	if c.shards == nil || len(c.shards) == 0 {
		err := c.fetchMetadata()
		if err != nil {
			return "", err
		}
	}
	index := common.GetBucketIndex(serviceName)
	serverId, err := common.GetNodeIdByIndex(index, c.shards)
	if err != nil {
		return "", err
	}
	addr, ok := c.clusterNodes[serverId]
	if !ok {
		return "", errors.New("node not found")
	}
	return c.checkConnection(serverId, addr)
}

func (c *Client) updateServiceInstances() error {
	response, err := c.FetchServiceInstances()
	if err != nil {
		return err
	}
	c.serviceInstances = make([]common.ServiceInstance, 0, len(response.ServiceInstances))
	for i, instance := range response.ServiceInstances {
		item := common.ServiceInstance{InstanceIp: instance.ServiceIp,
			InstancePort: int(instance.ServicePort),
			ServiceName:  instance.ServiceName}
		c.serviceInstances[i] = item
	}
	return nil
}

func (c *Client) FetchServiceInstances() (*pb.FetchServiceResponse, error) {
	request := pb.FetchServiceRequest{
		ServiceName: c.serviceName,
	}
	requestKey, err := c.sendRequest(c.addr, common.FetchServiceInstance, &request)
	if err != nil {
		c.log.Warnf("fetch service instance request failed, err: %v", err)
		return nil, err
	}

	res, err := c.syncWait(requestKey, time.Second*10)
	response := res.(common.Response)
	var sresp *pb.FetchServiceResponse
	err = proto.Unmarshal(response.Payload, sresp)
	if err != nil {
		c.log.Warnf("decode fetch service instance response failed, err: %v", err)
		return nil, err
	}
	return sresp, nil
}

func (c *Client) fetchMetadata() error {
	response, err := c.FetchMetadata()
	if err != nil {
		c.log.Errorf("failed to fetch metadata: %v", err)
		return err
	}
	var metadata pb.FetchMetadataResponse
	err = proto.Unmarshal(response.Payload, &metadata)
	if err != nil {
		c.log.Errorf("failed to decode metadata: %v", err)
		return err
	}

	var shards map[string]common.Shard
	err = json.Unmarshal([]byte(metadata.Shards), &shards)
	if err != nil {
		c.log.Errorf("failed to unmarshal shard json: %v", err)
		return err
	}
	c.shards = shards
	c.clusterNodes = metadata.Nodes
	c.log.Debugf("fetch metadata success: %s", metadata.Shards)
	return nil
}

func (c *Client) Subscribe(serviceName string, timeout time.Duration) (common.Response, error) {
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

func (c *Client) FetchMetadata() (common.Response, error) {
	requestKey, err := c.sendRequest(c.addr, common.FetchMetadata, nil)
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
	connKey, err := c.checkMetadataAndConnection(serviceName)
	if err != nil {
		return "", err
	}
	reqKey, err := c.sendRequest(connKey, common.Heartbeat, &heartbeatRequest)
	return reqKey, err
}

func (c *Client) register(serviceName string, ip string, port int) (string, error) {
	regRequest := pb.RegisterRequest{
		ServiceName: serviceName,
		ServiceIp:   ip,
		ServicePort: int32(port),
	}
	connKey, err := c.checkMetadataAndConnection(serviceName)
	if err != nil {
		return "", err
	}
	return c.sendRequest(connKey, common.Register, &regRequest)
}

func (c *Client) sendResponse(serviceName string, requestId uint64, res proto.Message) (uint64, error) {
	serverId, err := c.checkMetadataAndConnection(serviceName)
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
	_, err = (*c.connMap[addr]).Write(toBytes)
	if err != nil {
		return 0, err
	}
	return requestId, nil
}

func (c *Client) getAddr(serverId string) (string, error) {
	addr, ok := c.clusterNodes[serverId]
	if !ok {
		c.log.Errorf("failed to send response, server addr not found, serverId: %s", serverId)
		return "", errors.New("failed to send response, server addr not found")
	}
	return addr, nil
}

func (c *Client) sendRequest(connKey string, requestType common.RequestType, request proto.Message) (string, error) {
	requestId, toBytes, err := common.BuildRequestToBytes(requestType, request)
	if err != nil {
		c.log.Errorf("Error encoding pb: %v", err)
		return "", err
	}
	_, err = (*c.connMap[connKey]).Write(toBytes)
	if err != nil {
		return "", err
	}
	requestKey := requestType.String() + strconv.Itoa(int(requestId))
	c.Store(requestKey, nil)
	c.log.Debugf("send request: %s", requestKey)
	return requestKey, nil
}

func (c *Client) subscribe(serviceName string) (string, error) {
	subRequest := pb.SubscribeRequest{
		ServiceName: serviceName,
	}
	connKey, err := c.checkMetadataAndConnection(serviceName)
	if err != nil {
		return "", err
	}
	return c.sendRequest(connKey, common.Subscribe, &subRequest)
}

func (c *Client) onReceive(bytes []byte, conn net.Conn, err error) {
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

func (c *Client) Close() {
	if c.connMap != nil {
		for id, cn := range c.connMap {
			err := (*cn).Close()
			if err != nil {
				c.log.Errorf("failed to close server connection: %s, %v", id, err)
			}
		}
	}
}

func (c *Client) checkConnection(serverId, addr string) (string, error) {
	addr = strings.Replace(addr, "localhost", "127.0.0.1", -1)
	if _, ok := c.connMap[addr]; !ok {
		conn, err := CreatConn(addr, c.readBufSize, c.shutdownCh, c.readCallback)
		if err != nil {
			return "", err
		}
		c.connMap[addr] = &conn
	}
	return addr, nil
}
