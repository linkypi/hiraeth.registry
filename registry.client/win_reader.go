//go:build windows

package client

import (
	"context"
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/panjf2000/gnet/pkg/pool/goroutine"
	"google.golang.org/appengine/log"
	"net"
)

type WinReader struct {
	totalLen  int
	readLen   int
	firstRead bool

	buffer       []byte
	dataReceived []byte

	readBufSize       int
	lengthFieldLength int

	conn     net.Conn
	workPool *goroutine.Pool
}

func NewReader(conn net.Conn, readBufSize int) *WinReader {
	reader := WinReader{
		conn:        conn,
		workPool:    goroutine.Default(),
		readBufSize: readBufSize,
	}
	reader.init()
	return &reader
}

func (r *WinReader) init() {
	r.totalLen = 0
	r.readLen = 0
	r.firstRead = true
	r.buffer = make([]byte, r.readBufSize)
	r.dataReceived = make([]byte, 0, r.readBufSize)
	r.lengthFieldLength = common.DecoderConfig.LengthFieldLength
}

type ReadCallBack func(bytes []byte, conn net.Conn, err error)

func (r *WinReader) Receive(shutdownCh chan struct{}, callback ReadCallBack) {

	for {
		select {
		case <-shutdownCh:
			return
		default:
		}
		// All data has been read, reset the read state
		if r.totalLen > 0 && r.readLen == r.totalLen {
			// Here you must remove the bytes of data with the header indicating the length of the data
			r.dataReceived = r.dataReceived[common.DecoderConfig.LengthFieldLength:]
			r.runCallback(callback, nil)
			r.init()
			continue
		}

		//_ = r.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		bytesRead, err := r.conn.Read(r.buffer)
		if err != nil {
			r.runCallback(callback, err)
			return
		}

		if r.firstRead {
			// First, read the length of data
			validBytes := r.buffer[r.lengthFieldLength:bytesRead]
			// Extract the portion of valid data that has already been read
			r.dataReceived = append(r.dataReceived, validBytes...)
			r.totalLen = common.ReadMessageLengthFromHeader(r.buffer, r.lengthFieldLength)
			r.readLen = r.readLen + bytesRead - r.lengthFieldLength
			r.firstRead = false
		} else {
			validBytes := r.buffer[:bytesRead]
			r.dataReceived = append(r.dataReceived, validBytes...)
			r.readLen = r.readLen + bytesRead
		}
	}
}

func (r *WinReader) runCallback(callback ReadCallBack, readErr error) {
	// 这里需要移除
	data, con := r.dataReceived, r.conn
	err := r.workPool.Submit(func() {
		callback(data, con, readErr)
	})
	if err != nil {
		log.Errorf(context.Background(), "Failed to submit task to pool: %v", err)
	}
}
