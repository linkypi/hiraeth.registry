//go:build windows

package common

import (
	"github.com/panjf2000/gnet/pkg/pool/goroutine"
	"net"
	"strings"
	"time"
)

type HandleReceiveEvent func(bytes []byte, conn net.Conn, err error)
type ConnClosedEvent func(err error, addr string)

type WinReader struct {
	buffer       []byte
	dataReceived []byte

	readBufSize int

	conn     net.Conn
	workPool *goroutine.Pool
	codec    ICodec
}

func NewReader(conn net.Conn, readBufSize int, codec ICodec) *WinReader {
	reader := WinReader{
		conn:        conn,
		codec:       codec,
		workPool:    goroutine.Default(),
		readBufSize: readBufSize,
	}
	reader.init()
	return &reader
}

func (r *WinReader) init() {
	r.buffer = make([]byte, r.readBufSize)
	r.dataReceived = make([]byte, 0, r.readBufSize)
}

func (r *WinReader) Receive(handleEvent HandleReceiveEvent, onConnClosed ConnClosedEvent, shutdownCh chan struct{}) {

	for {
		select {
		case <-shutdownCh:
			return
		default:
		}

		bytesRead, stdErr := r.conn.Read(r.buffer)

		// connection is closed
		// To determine whether the connection is closed by reading data
		// this will block the wait until a std err is generated,
		// which may be a disconnection from the server, or an IO timeout
		// this implementation refers to the implementation of gnet framework
		// and the relevant code is in gnet@1.6.7/acceptor_windows.go -> line number 56
		// https://github.com/panjf2000/gnet
		if stdErr != nil {
			if strings.Contains(stdErr.Error(), "use of closed network connection") {
				continue
			}
			_ = r.conn.SetReadDeadline(time.Time{})
			addr := r.conn.RemoteAddr().String()
			logx.Warnf("remote connection is closed: %s", addr)

			// close the old connection
			err := r.conn.Close()
			if err != nil {
				logx.Warnf("close old connection failed: %s, %v", addr, err)
			}
			err = r.workPool.Submit(func() {
				onConnClosed(stdErr, addr)
			})
			if err != nil {
				logx.Errorf("failed to submit conn closed event task to pool: %v", err)
			}
			break
		}

		_ = r.conn.SetReadDeadline(time.Time{})

		if bytesRead == 0 {
			logx.Warnf("connection closed by server")
			continue
		}

		if stdErr != nil {
			r.onCallback(handleEvent, stdErr)
			continue
		}

		if bytesRead < r.readBufSize {
			r.dataReceived = append(r.dataReceived, r.buffer[:bytesRead]...)
			r.onCallback(handleEvent, nil)
			r.init()
			continue
		}
		r.dataReceived = append(r.dataReceived, r.buffer...)
	}
}

func (r *WinReader) onCallback(callback HandleReceiveEvent, readErr error) {
	data, con := r.dataReceived, r.conn
	err := r.workPool.Submit(func() {
		buf, err := r.codec.DecodeFor(data)
		if err != nil {
			logx.Errorf("Decode error:%v", err)
		}
		callback(buf, con, err)
	})
	if err != nil {
		logx.Errorf("failed to submit read callback task to pool: %v", err)
	}
}
