//go:build linux || freebsd || dragonfly || darwin

package client

import (
	"errors"
	"strings"
	"time"

	"github.com/panjf2000/gnet"
	"github.com/panjf2000/gnet/pkg/logging"
	"github.com/sirupsen/logrus"

	"github.com/linkypi/hiraeth.registry/common"
)

func (c *Client) Start(addr string) error {
	if _, ok := c.connMap[addr]; ok {
		c.log.Debugf("connection already exists: %s", addr)
		return nil
	}

	addr = strings.Replace(addr, "localhost", "127.0.0.1", -1)
	con, err := CreateConn(addr, c.readBufSize, nil, c.codec, c.eventHandler)
	if err != nil {
		return err
	}

	c.addr = addr
	c.connMap[addr] = &con
	return nil
}

func (h *Client) OnOpened(c gnet.Conn) ([]byte, gnet.Action) {
	c.SetContext(h.client.codec)
	return nil, gnet.None
}

func (h *Client) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	h.client.onReceive(frame, nil)
	return
}
func (h *Client) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	return
}

func CreateClient(addr string, shutdownCh chan struct{}, logger *logrus.Logger) (*Client, error) {
	return CreateClientWithCodec(addr, nil, shutdownCh, logger)
}

func CreateClientWithCodec(addr string, codec gnet.ICodec, shutdownCh chan struct{}, logger *logrus.Logger) (*Client, error) {
	client := NewClient(4096, shutdownCh, logger)
	client.codec = codec
	if c.codec == nil {
		c.codec = &common.BuildInFixedLengthCodec{Version: common.DefaultProtocolVersion}
	}

	c.eventHandler = &client

	err := client.Start(addr)
	if err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}

type GConn struct {
	Conn
	conn *gnet.Conn
}

func (g *GConn) AsyncWrite(data []byte) error {
	return (*g.conn).AsyncWrite(data)
}

func (g *GConn) Write(data []byte) (int, error) {
	err := (*g.conn).AsyncWrite(data)
	return len(data), err
}
func (g *GConn) Close() error {
	return (*g.conn).Close()
}

func (g *GConn) Read([]byte) (int, error) {
	return 0, errors.New("not support")
}
func (g *GConn) SetReadDeadline(time time.Time) error {
	return errors.New("not support")
}

func CreateConn(addr string, readBufSize int, _ chan struct{}, codec gnet.ICodec, handler any) (Conn, error) {
	eventHandler, ok := handler.(gnet.EventHandler)
	if !ok {
		return nil, errors.New("handler must implement all the interface of gnet.EventHandler")
	}

	client, err := gnet.NewClient(
		eventHandler,
		gnet.WithLogger(common.Log),
		gnet.WithLogLevel(logging.InfoLevel),
		gnet.WithCodec(codec),
		gnet.WithReusePort(true),
		gnet.WithReadBufferCap(readBufSize),
		gnet.WithReuseAddr(true),
		gnet.WithTCPKeepAlive(time.Second*5),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
	)
	if err != nil {
		return nil, err
	}

	err = client.Start()
	if err != nil {
		return nil, err
	}

	conn, err := client.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	gConn := GConn{conn: &conn}
	return &gConn, nil
}
