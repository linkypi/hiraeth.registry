//go:build linux || freebsd || dragonfly || darwin
// +build linux freebsd dragonfly darwin

package client

import (
	"context"
	"github.com/panjf2000/gnet"
	"testing"
)

func TestLinuxClient(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	client := NewClient(4096, ctx)

	linuxClient := MyEventHandler{}
	client.SetEventHandler(&linuxClient)
	err := client.Start()
	if err != nil {
		cancelFunc()
		client.Close()
		t.Error(err)
	}
	linuxClient.client = client
	select {
	case <-ctx.Done():
		cancelFunc()
		client.Close()
		t.Log("client done")
	}
}

type MyEventHandler struct {
	gnet.EventServer
	client *Client
}

func (w *MyEventHandler) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	w.client.onReceive(frame)
	return
}
