package test

import (
	"encoding/json"
	"fmt"
	ji "github.com/json-iterator/go"
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/linkypi/hiraeth.registry/server/cluster"
	"github.com/linkypi/hiraeth.registry/server/config"
	"testing"
	"time"
)

type User struct {
	ID int `json:"id"` // 用户id
	// 其它field
	FansCount  int `json:"fansCount,omitempty"` // 粉丝数
	LastTime   time.Time
	CreateTime string `json:"createTime"`
}

type Person struct {
	Birthday time.Time
	Name     string
}

type State int

const (
	NoneState State = iota
	Initializing
	// Active The cluster is active and can be read and written to
	Active
)

type MetaData struct {
	ClusterId       uint64                     `json:"clusterId"`
	LeaderId        string                     `json:"leaderId"`
	Term            uint64                     `json:"term"`
	ExpectedNodeMap map[string]config.NodeInfo `json:"expectedNodeMap"`
	ActualNodeMap   map[string]config.NodeInfo `json:"actualNodeMap"`
	ActualNodes     []config.NodeInfo          `json:"actualNodes"`

	State         State                `json:"state"`
	NodeConfig    config.NodeConfig    `json:"nodeConfig"`
	ClusterConfig config.ClusterConfig `json:"clusterConfig"`

	// Data sharding info, record the shard info stored on each machine
	Shards map[string]common.Shard `json:"shards"`
	// Replica sharding info, record the shard info replicated on each machine
	Replicas   map[string][]common.Shard `json:"replicas"`
	CreateTime string                    `json:"createTime"`
}

func TestJson(t *testing.T) {
	formattedTime := time.Now().Format("2006-01-02 15:04:05")
	metaData := MetaData{ClusterId: 123, State: Active, CreateTime: formattedTime}
	marshal, _ := ji.Marshal(metaData)
	mdata, _ := json.Marshal(&metaData)
	fmt.Println(">>>>>>>>>>: " + string(marshal))
	fmt.Println(">>>>>>>>>>: " + string(mdata))

	cmeta := cluster.MetaData{ClusterId: 123, State: cluster.Active.String(), CreateTime: formattedTime}
	marshal, _ = ji.Marshal(cmeta)
	mdata, _ = json.Marshal(&cmeta)
	fmt.Println(">>>>>>>>>>: " + string(mdata))
	fmt.Println(">>>>>>>>>>: " + string(marshal))
	//
	//person := Person{
	//	Birthday: time.Now(),
	//	Name:     "leon",
	//}
	//data, err := json.Marshal(&person)
	//
	//if err != nil {
	//	fmt.Println("Marshal Error:", err)
	//	return
	//}
	//fmt.Println(string(data))
	//
	//formattedTime = time.Now().Format("2006-01-02 15:04:05")
	//user1 := User{ID: 123, FansCount: 0, LastTime: time.Now(), CreateTime: formattedTime}
	//user2 := User{ID: 123, FansCount: 123, LastTime: time.Now()}
	//
	//str, _ := json.Marshal(user1)
	//str2, _ := json.Marshal(&user2)
	//
	//fmt.Println(string(str))
	//fmt.Println(string(str2))
	//t.Log(string(str))
}
