先前出于好奇自我实现了一个基于Java实现的分布式微服务注册中心 [my-registry](https://github.com/linkypi/my-registry). 最近迷上了go, 又刚好在B站看到了小徐先生对 raft 算法及 etcd raft实现的讲解, 碰巧就想到 my-registry 正是需要该算法来实现leader选举以及数据一致性保证. 于是就想着使用go基于etcd raft来实现一遍, 虽然etcd已经将raft算法独立封装成lib但是想基于它实现自己的功能还是存在不少难度:

1. 传输层 transport 仅支持 http, 虽然提供了 transport 相关接口, 但想改用grpc则需调整不少代码, 文档中可用信息很少.
2. 再一个是etcd raft 并没有提供一个通知机制来告知开发者 leader 已发生变更, 如集群异常导致重新选出leader后需要对数据重新分配或者调整数据分片

相反, hashicorp/raft 实现则提供了更好的API使用, 内部代码实现也更加清晰明了:
1. transport 提供 mem, TCP 以及 grpc 等多种方式, 
2. leader 的变更通知也可通过 LeaderCh 以及 raft Config 提供的 NotifyCh 来实现
3. 提供了操作集群的工具 [raftadmin](https://github.com/Jille/raftadmin) , 但仍缺少部分功能, 如向集群添加或移除节点 AddPeer/RemovePeer
4. 提供了状态机接口 FSM 用于处理写请求完成后的相关操作
当然还有部分功能需要自行实现, 如集群启动时并没有提供各个节点发现功能, 而需要raftadmin来处理. 同时 LeaderCh 是 bool 类型的 channel，不带缓存，当本节点的 leader 状态有变化的时候，会往这个 channel 里面写数据，但是由于不带缓冲且写数据的协程不会阻塞在这里，有可能会写入失败，没有及时变更状态，所以使用 leaderCh 的可靠性不能保证。好在 raft Config 里面提供了另一个 channel NotifyCh，它是带缓存的，当 leader 状态变化时会往这个 chan 写数据，写入的变更消息能够缓存在 channel 里面，应用程序能够通过它获取到最新的状态变化。


### 集群启动流程
每个节点启动后都会尝试从给定的集群节点中建立长连接，待连接都建立完成后开始互换节点信息。互换节点信息完成便可以开始启动 Raft 来选举集群 Leader，在开始的实现中必须等到集群所有节点都互连完成后才进入Leader选举阶段，但此实现过于死板，实际并不需要所有节点都连接完成才开始选举。可根据系统适用场景区分，如用作分布式配置中心则需要较高的一致性，必须保证大多数节点在线。故系统提供了 cluster.quorum.count 来指定集群选举所需要的最小节点数。

### Leader 变更 (Leadership Transfer)

Leader变更有几个问题需要注意:

#### 1. 集群 Follower 实际是哪些节点

虽然可以通过相关channel得知Raft已经重新选举出 Leader, 但是其内部存储的节点并不能确定每个都是正常的, 如为Raft配置三个节点, 而实际仅仅启动了两个节点, 此时Raft同样可以选举Leader. 所以实际在应用层面还需自行判断当前集群实际都有哪些 Follower 以及这些 Follower 是否都正常, 以及整个集群节点数是否已经达到配置中的 quorum 数, 以防止脑裂时重新选出的集群不满足指定要求. 

#### 2. 移除无效节点

我们必须移除无效的节点，因为raft在启动时已经将配置中的所有节点加入到集群中
// raft 会不断尝试自动连接所有节点, 然后将他们加入集群
// 然而，在某些情况下，可能无法连接到某些节点，或者某些节点已经过时
// 因此，我们必须从集群中移除这些无效的节点
// 以免将集群元数据分配到这些无效的节点上


很有可能是新节点加入集群,此时就需要读取 metadata.json 中的数据来判断前后的follower数量是否一致
若follower数量不一致则说明是新节点加入,需要重新调整数据分片






### 集群新增节点

向集群新增节点时，首先判断auto.join.cluster.enable是否已开启，默认关闭。

- 若未开启，则考虑实现一个控制台命令来将新节点加入集群
- 若已开启，则新节点启动后会主动连接集群节点，然后启动raft。 集群节点收到新节点元数据信息后，判断新节点是否是集群新加入的节点，同时是否已开启 auto.join.cluster.enable ，若是新节点并且已开启该开关则将该节点加入到Raft集群。注意，只有leader才可以执行该操作，若是非leader节点接收到该请求则需要转发到leader再处理

测试节点加入集群

```sh
# 首先启动三个节点组成的集群
hiraeth.registry -config=./config/registry1.conf
hiraeth.registry -config=./config/registry2.conf
hiraeth.registry -config=./config/registry3.conf

# 待集群启动完成后，启动新节点，并带上 -join 参数（-join 或者 --join 亦可）
hiraeth.registry -config=./config/registry4.conf -join
```



raft启动需注意区分当前系统是否已有数据，有数据则恢复集群，否则新建集群：

```golang
   	r, err := raft.NewRaft(conf, fsm, ldb, sdb, fss, rn.transport())
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	cfg := raft.Configuration{Servers: peers}

	// use boltDb to read the value of the key [CurrentTerm] to determine whether a cluster exists
	// If the cluster state not exists, use r.BootstrapCluster() method to create a new one
	existState, err := raft.HasExistingState(ldb, sdb, fss)
	if err != nil {
		return nil, fmt.Errorf("raft check existing state failed: %v", err)
	}
	if !existState {
		fur := r.BootstrapCluster(cfg)
		if err := fur.Error(); err != nil {
			return nil, fmt.Errorf("raft bootstrap cluster failed: %v", err)
		}
	}
```







``` sh
 protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=require_unimplemented_servers=false InternalService.proto 
```


> Uber 静态分析工具 NilAway 分析空指针



### 1. QA

- 


```shell

go get go.etcd.io/etcd/raft/v3
go get go.etcd.io/etcd/wal

```

