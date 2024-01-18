//go:build windows

package client

import (
	"errors"
	"github.com/linkypi/hiraeth.registry/common"
	"net"
	"time"
)

func (c *Client) Start(addr string) error {
	winCon, err := CreatConn(addr, c.readBufSize, c.shutdownCh, c.readCallback)
	if err != nil {
		return err
	}

	c.addr = addr
	c.connMap[addr] = &winCon
	go c.checkConn(addr, &winCon)
	return nil
}

// To determine whether the connection is closed by reading data
// this will block the wait until a std err is generated,
// which may be a disconnection from the server, or an IO timeout
// this implementation refers to the implementation of gnet framework
// and the relevant code is in gnet@1.6.7/acceptor_windows.go -> line number 56
// https://github.com/panjf2000/gnet
func (c *Client) checkConn(addr string, conn *Conn) {
	var buffer [0x10000]byte
	for {
		_, stdErr := (*conn).Read(buffer[:])
		if stdErr != nil {
			_ = (*conn).SetReadDeadline(time.Time{})
			c.log.Warnf("remote connection is closed, %v", stdErr)

			// reconnect
			newCon, err := CreatConn(addr, c.readBufSize, c.shutdownCh, c.readCallback)
			if err != nil {
				c.log.Warnf("reconnect to [%s] failed: %v", addr, err)
				continue
			}
			// close the old connection
			err = (*conn).Close()
			if err != nil {
				c.log.Warnf("close old connection failed: %s, %v", addr, err)
			}
			// There won't be too many server nodes, so there's no sync.Map for concurrency control here.
			c.connMap[addr] = &newCon
			c.log.Infof("reconnect to %s success", addr)
			go c.checkConn(addr, &newCon)
			break
		}
	}
}

func CreatConn(addr string, readBufSize int, shutdownCh chan struct{}, readCallback ReadCallBack) (Conn, error) {

	if readBufSize <= 0 {
		readBufSize = 1024 * 4
	}

	if readCallback == nil {
		return nil, errors.New("readCallback is nil")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		common.Log.Errorf("dial error: %v", err)
		return nil, err
	}

	tcpConn := conn.(*net.TCPConn)
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(time.Second * 5)
	_ = tcpConn.SetNoDelay(true)
	_ = tcpConn.SetReadBuffer(readBufSize)
	_ = tcpConn.SetDeadline(time.Now().Add(2 * time.Minute))

	reader := NewReader(conn, readBufSize)
	winConn := WinConn{conn: tcpConn}

	go reader.Receive(shutdownCh, readCallback)

	return &winConn, nil
}

type WinConn struct {
	Conn
	conn *net.TCPConn
}

func (w *WinConn) AsyncWrite(data []byte) error {
	common.Log.Error("windows not support")
	return errors.New("windows not support")
}
func (w *WinConn) Write(data []byte) (int, error) {
	return w.conn.Write(data)
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
