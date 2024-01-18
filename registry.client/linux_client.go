//go:build linux || freebsd || dragonfly || darwin
// +build linux freebsd dragonfly darwin

package client

import (
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/panjf2000/gnet"
	"time"
)

func (c *Client) Start() error {
	winCon, err := CreateConn(c.addr, c.readBufSize, c.shutdownCh, c.eventHandler)
	if err != nil {
		return err
	}
	c.connMap = winCon
	return nil
}

type GConn struct {
	Conn
	conn *gnet.Conn
}

func (g *GConn) AsyncWrite(data []byte) error {
	g.conn.AsyncWrite(data)
}
func (g *GConn) Write(data []byte) (int, error) {
	return g.conn.Write(data)
}
func (g *GConn) Close() error {
	return g.conn.Close()
}

func CreateConn(addr string, readBufSize int, handler *gnet.EventHandler) (Conn, error) {
	codec := gnet.NewLengthFieldBasedFrameCodec(common.EncoderConfig, common.DecoderConfig)

	client, err := gnet.NewClient(
		handler,
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
		return conn, err
	}
	gConn := GConn{conn: conn}
	return &gConn, nil
}
