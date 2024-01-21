//go:build linux || freebsd || dragonfly || darwin

package client

import (
	"errors"
	"time"

	"github.com/panjf2000/gnet"
	"github.com/panjf2000/gnet/pkg/logging"
	"github.com/sirupsen/logrus"

	"github.com/linkypi/hiraeth.registry/common"
)

func (c *Client) OnOpened(con gnet.Conn) ([]byte, gnet.Action) {
	con.SetContext(c.codec)
	return nil, gnet.None
}

func (c *Client) React(frame []byte, con gnet.Conn) (out []byte, action gnet.Action) {
	c.onReceive(frame, nil)
	return
}
func (c *Client) OnClosed(con gnet.Conn, err error) (action gnet.Action) {
	return
}

func CreateClientWithCodec(addr string, codec common.ICodec, shutdownCh chan struct{}, logger *logrus.Logger) (*Client, error) {
	return NewClient(addr, codec, 4096, shutdownCh, logger)
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

func (c *Client) createConnInternal(addr string) (Conn, error) {

	client, err := gnet.NewClient(
		&c,
		gnet.WithLogger(common.Log),
		gnet.WithLogLevel(logging.InfoLevel),
		gnet.WithCodec(c.codec),
		gnet.WithReusePort(true),
		gnet.WithReadBufferCap(c.readBufSize),
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
