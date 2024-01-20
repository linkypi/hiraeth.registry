//go:build windows

package client

import (
	"errors"
	"github.com/sirupsen/logrus"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/linkypi/hiraeth.registry/common"
)

type WinConn struct {
	Conn
	codec common.ICodec
	ctx   interface{}
	conn  *net.TCPConn
}

func (w *WinConn) SetCodec(codec common.ICodec) {
	w.codec = codec
}
func (w *WinConn) GetCodec() common.ICodec {
	return w.codec
}

func (w *WinConn) SetCtx(ctx interface{}) {
	w.ctx = ctx
}
func (w *WinConn) GetCtx() interface{} {
	return w.ctx
}
func (w *WinConn) AsyncWrite(data []byte) error {
	common.Log.Error("windows not support")
	return errors.New("windows not support")
}
func (w *WinConn) Write(data []byte) (int, error) {
	codec := w.GetCodec()
	if codec == nil {
		return 0, errors.New("codec could not be nil")
	}
	buf, err := codec.EncodeFor(codec, data)
	if err != nil {
		common.Log.Errorf("encode error: %s", err.Error())
		return 0, err
	}
	return w.conn.Write(buf)
}

func (w *WinConn) Close() error {
	return w.conn.Close()
}

func (w *WinConn) Read(buf []byte) (int, error) {
	return w.conn.Read(buf)
}
func (w *WinConn) SetReadDeadline(time time.Time) error {
	return w.conn.SetReadDeadline(time)
}

func CreateClient(addr string, shutdownCh chan struct{}, logger *logrus.Logger) (*Client, error) {
	return CreateClientWithCodec(addr, nil, shutdownCh, logger)
}

func CreateClientWithCodec(addr string, codec common.ICodec, shutdownCh chan struct{}, logger *logrus.Logger) (*Client, error) {
	client := NewClient(4096, shutdownCh, logger)
	if codec == nil {
		codec = &common.BuildInFixedLengthCodec{Version: common.DefaultProtocolVersion}
	}
	client.SetCodec(codec)
	err := client.Start(addr)
	if err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}

func (c *Client) Start(addr string) error {
	if _, ok := c.connMap[addr]; ok {
		c.log.Debugf("connection already exists: %s", addr)
		return nil
	}

	addr = strings.Replace(addr, "localhost", "127.0.0.1", -1)
	con, err := c.createConn(addr)
	if err != nil {
		return err
	}

	c.addr = addr
	c.connMap[addr] = &con

	winConn := con.(*WinConn)
	winConn.SetCodec(c.codec)
	winConn.SetCtx(c.codec)

	return nil
}

func (c *Client) onReconnectSuccess(con *Conn) {

	_ = c.fetchMetadata()
	// If the client has enabled a service subscription, the subscription
	// will be initiated again after the reconnection is successful
	if c.turnOnServiceSubs {
		// Resubscribing to the service
		_, err := c.Subscribe(c.serviceName, time.Second*10)
		if err == nil {
			c.log.Infof("resubscribe to %s success", c.serviceName)
		} else {
			c.log.Warnf("resubscribe to %s failed: %v", c.serviceName, err)
		}
	}
}

func (c *Client) onConnClosedEvent(err error, addr string) {

	delete(c.connMap, addr)

	// reconnect until success
	for {
		select {
		case <-c.shutdownCh:
			return
		default:
		}

		newCon, err := c.createConn(addr)
		if err != nil {
			time.Sleep(time.Second * 3)
			c.log.Warnf("reconnect to [%s] failed: %v", addr, err)
			continue
		}
		// There won't be too many server nodes, so there's no sync.Map for concurrency control here.
		c.connMap[addr] = &newCon
		c.log.Infof("reconnect to %s success", addr)

		go c.onReconnectSuccess(&newCon)
		break
	}
}

func (c *Client) handleReceiveEvent(bytes []byte, conn net.Conn, err error) {
	if err != nil {
		c.onReceive(nil, err)
		return
	}
	c.onReceive(bytes, nil)
}

var conLock sync.Map

func (c *Client) createConn(addr string) (Conn, error) {

	if c.readBufSize <= 0 {
		c.readBufSize = 1024 * 4
	}

	lockKey := "CON_MTX_" + addr
	_, ok := conLock.Load(lockKey)
	if ok {
		common.Log.Debugf("conn is already exist, %s, wait for the conn to complete", addr)
		return nil, errors.New("conn is already exist")
	}
	conLock.Store(lockKey, addr)
	defer conLock.Delete(lockKey)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		common.Log.Errorf("dial error: %v", err)
		return nil, err
	}

	tcpConn := conn.(*net.TCPConn)
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(time.Second * 5)
	_ = tcpConn.SetNoDelay(true)
	_ = tcpConn.SetReadBuffer(c.readBufSize)
	//_ = tcpConn.SetDeadline(time.Now().Add(2 * time.Minute))

	reader := common.NewReader(conn, c.readBufSize, c.codec)
	winConn := WinConn{conn: tcpConn, ctx: c.codec, codec: c.codec}

	go reader.Receive(c.handleReceiveEvent, c.onConnClosedEvent, c.shutdownCh)

	return &winConn, nil
}
