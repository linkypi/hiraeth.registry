package tcp

import (
	"fmt"
	"github.com/linkypi/hiraeth.registry/server/log"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"sync"
)

type NetManager struct {
	log         *logrus.Logger
	connections sync.Map
}

func NewNetManager() *NetManager {
	return &NetManager{
		log: log.Log,
	}
}

func (net *NetManager) SendHeartbeat() {
	net.connections.Range(func(key, value interface{}) bool {
		addr := key.(string)
		c := value.(gnet.Conn)
		bytes := []byte(fmt.Sprintf("heart beating to %s\n", addr))
		_ = c.AsyncWrite(bytes)
		return true
	})
}

func (net *NetManager) GetConn(addr string) gnet.Conn {
	c, ok := net.connections.Load(addr)
	if !ok {
		net.log.Errorf("connection not found %s", addr)
		return nil
	}
	return c.(gnet.Conn)
}

func (net *NetManager) AddConn(c gnet.Conn) {
	addr := c.RemoteAddr().String()
	net.connections.Store(addr, c)
	net.log.Infof("add client connection %s.", addr)
}

func (net *NetManager) RemoveConn(addr string) {
	conn, ok := net.connections.Load(addr)
	if ok {
		err := (conn.(gnet.Conn)).Close()
		if err != nil {
			net.log.Warnf("failed to close conn: %s, %v", addr, err)
		}
		net.connections.Delete(addr)
	}
	net.log.Infof("remove client connection %s.", addr)
}

func (net *NetManager) CloseAllConn() {
	net.connections.Range(func(key, value interface{}) bool {
		addr := key.(string)
		c := value.(gnet.Conn)
		err := c.Close()
		if err != nil {
			net.log.Errorf("close connection %s failed: %s", addr, err.Error())
		}
		return true
	})
}
