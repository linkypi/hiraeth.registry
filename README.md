先前出于好奇自我实现了一个基于Java实现的分布式微服务注册中心 [my-registry](https://github.com/linkypi/my-registry). 最近迷上了go, 又刚好在B站看到了小徐先生对 raft 算法及 etcd raft实现的讲解, 碰巧就想到 my-registry 正是需要该算法来实现leader选举以及数据一致性保证. 于是就想着使用go基于etcd raft来实现一遍, 虽然etcd已经将raft算法独立封装成lib但是想基于它实现自己的功能还是存在不少难度:

1. 传输层 transport 仅支持 http, 虽然提供了 transport 相关接口, 但想改用grpc则需调整不少代码, 文档中可用信息很少.
2. 再一个是etcd raft 并没有提供一个通知机制来告知开发者 leader 已发生变更, 如集群异常导致重新选出leader后需要对数据重新分配或者调整数据分片

相反, hashicorp/raft 实现则提供了更好的API使用, 内部代码实现也更加清晰明了:
1. transport 提供 mem, TCP 以及 grpc 等多种方式, 
2. leader 的变更通知也可通过 LeaderCh 以及 raft Config 提供的 NotifyCh 来实现
3. 提供了操作集群的工具 [raftadmin](https://github.com/Jille/raftadmin) , 但仍缺少部分功能, 如向集群添加或移除节点 AddPeer/RemovePeer
4. 提供了状态机接口 FSM 用于处理写请求完成后的相关操作
当然还有部分功能需要自行实现, 如集群启动时并没有提供各个节点发现功能, 而需要raftadmin来处理. 同时 LeaderCh 是 bool 类型的 channel，不带缓存，当本节点的 leader 状态有变化的时候，会往这个 channel 里面写数据，但是由于不带缓冲且写数据的协程不会阻塞在这里，有可能会写入失败，没有及时变更状态，所以使用 leaderCh 的可靠性不能保证。好在 raft Config 里面提供了另一个 channel NotifyCh，它是带缓存的，当 leader 状态变化时会往这个 chan 写数据，写入的变更消息能够缓存在 channel 里面，应用程序能够通过它获取到最新的状态变化。



SOFARegistry  在数据存储层面采用了类似 Eureka 的最终一致性的过程，但是存储内容上和 Eureka 在每个节点存储相同内容特性不同，采用每个节点上的内容按照一致性 Hash 数据分片来达到数据容量无限水平扩展能力. 支持多副本备份，保证数据高可用. 此外, 它还提供会话层, 即客户端接入能力，接受客户端的服务发布及服务订阅请求，并作为一个中间层将发布数据转发 DataServer 存储。SessionServer 可无限扩展以支持海量客户端连接



### 注册中心功能

程序员非常容易犯的一个错误就是一旦陷入到代码中非常容易迷失方向。所以通过绘图来理清注册中心的功能点就显得尤为重要，在工作中也习惯通过该方式来处理。注册中心包含服务注册与发现(订阅)，心跳维持以及客户端及服务端的容灾处理. 一图胜千言, 通过绘图更容易理清思路, 绘图思路简单明了，原本以外花不了太长时间来开发, 很快可以进入到处理分布式一致性写入的环节. 但实际编码需考虑的问题可不少, 哪里会存在内存泄漏, 哪里会出现死循环, 是否考虑超时处理, 服务端及客户端每一步流程出现问题后对端该如何处理



#### 集群待处理问题

- follower节点宕机后如何快速恢复


### 集群启动流程

每个节点启动后都会尝试从给定的集群节点中建立长连接，待连接都建立完成后开始互换节点信息。互换节点信息完成便可以开始启动 Raft 来选举集群 Leader，在开始的实现中必须等到集群所有节点都互连完成后才进入Leader选举阶段，但此实现过于死板，实际并不需要所有节点都连接完成才开始选举。可根据系统适用场景区分，如用作分布式配置中心则需要较高的一致性，必须保证大多数节点在线。故系统提供了 cluster.quorum.count 来指定集群选举所需要的最小节点数。

### Leader 变更 (Leadership Transfer)

Leader变更有几个问题需要注意:

#### 1. 集群 Follower 实际是哪些节点

虽然可以通过相关channel得知Raft已经重新选举出 Leader, 但是其内部存储的节点并不能确定每个都是正常的, 如为Raft配置三个节点, 而实际仅仅启动了两个节点, 此时Raft同样可以选举Leader. 所以实际在应用层面还需自行判断当前集群实际都有哪些 Follower 以及这些 Follower 是否都正常, 以及整个集群节点数是否已经达到配置中的 quorum 数, 以防止脑裂时重新选出的集群不满足指定要求. 

#### 2. 移除无效节点

我们必须移除无效的节点，因为raft在启动时会将配置文件中的所有节点都加入到集群中，此后 raft 会不断尝试自动连接所有节点, 然后将他们加入集群。然而，在某些情况下，可能无法连接到某些节点。 因此，我们必须从集群中移除这些无效的节点，以免将集群元数据分配到这些无效的节点上

### 如何检测集群节点宕机

宕机检测有几种方式

- 通过 leaderCh 及 notifyCh 获知，但是通常只有 leader 节点才会收到该部分通知，并且是所有 follower 都已宕机仅剩leader的情况下，leader才会收到这些channel的通知
- 通过连接断开检查获知，集群每个节点会维护与其他节点的长连接。实际raft算法内部也实现了心跳机制
- 定期实时检测当前Raft节点状态，raft 提供了Stats()方法用户获取节点内部统计数据, 该方式更加直接、高效. 另外节点状态检测不能仅仅通过一个状态变化就得出结论, 很有可能集群能很快选出leader, 所以在该系统实现中检测集群是否宕机是通过检测节点Candidate状态持续时间是否已超出一个 Raft 选举周期, 一共检测三次来判断

#### 1. leader 宕机

若是 leader 宕机，那么会有两种场景:

##### 1.1 剩余可用节点满足 quorum 数量

此时可重新选举出 leader， 然后通过 LeaderCh 获取leader变更通知。随后在应用层面还需重新调整数据分片, 若数据较多, 那么数据迁移将会是一个漫长的过程

##### 1.2 剩余可用节点无法满足quorum数量

此时就需要有一个自动检查机制去实时检查并更新集群状态，以免集群状态未能及时更新从而导致数据不一致。如由两个节点组成的集群（hashicrop/raft 两个点亦可选主），在主节点宕机后Follower 节点的状态会进入到Candidate, 但因为目前集群仅剩一个节点，所以也就无法选主, 而 hashicorp/raft 的实现中并没有给出相关的通知. 

#### 2. follower 宕机

若是 follower 宕机， 那么在可用节点满足quorum数量的情况下, 集群仍然可正常提供服务. 此时需将宕机节点的所在的副本分片提升为主分片, 同时在其他可用机器中增加缺失的副本分片即可. 待宕机节点恢复重新加入集群后, 说先会判断宕机集群与当前运行的集群是否同属一个集群(通过集群id判断), 若是同一个集群则对比彼此数据分片情况, 若仅是当前节点的元数据做了调整那么此时只需要增量同步从宕机节点开始到恢复完成时新增的数据. 否则, 集群就需要调整数据分片, 从其他各个节点中分摊部分分片给到新加入的节点, 而非重新打乱来调整分片, 以减少数据迁移数据.

考虑到这样一种情况：一个集群有N个节点，已经服务一段时间，此时由于某种极端原因导致所有节点宕机或无法联通。待修复后重新启动所有节点，raft 开始重新选举产生leader， 此时是否需要考虑

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




一. 集群新增节点调整:
1. 重新对数据分片, 分片期间数据只读
2. 数据逐步迁移, 直到迁移完成

二. 宕机恢复实现
1. 检查集群 id 是否一致, 若一致说明

三. 定期存储 raft 相关统计数据, 以便宕机恢复排查问题
四. 限流: 如果 Raft 模块已提交的日志索引（committed index）比已应用到状态机的日志索引（applied index）超过了 5000，那么它就返回一个”etcdserver: too many requests” 错误给 client

etcdserver 从 Raft 模块获取到以上消息和日志条目后，作为 Leader，它会将 put 提案消息广播给集群各个节点，同时需要把集群 Leader 任期号、投票信息、已提交索引、提案内容持久化到一个 WAL（Write Ahead Log）日志文件中，用于保证集群的一致性、可恢复性

五. 幂等问题处理, 全局递增提案 ID

六. CP 模式下, 新的 Follower 加入集群后, 增加同步数据的时间限制  initlimit

七. 热点 Key 处理
当有请求进来读写普通节点时，节点内会同时做请求 Key 的统计。如果某个 Key 达到了一定的访问量或者带宽的占用量，会自动触发流控以限制热点 Key 访问，防止节点被热点请求打满。同时，监控服务会周期性的去所有 Redis 实例上查询统计到的热点 Key。如果有热点，监控服务会把热点 Key 所在 Slot 上报到我们的迁移服务。迁移服务这时会把热点主从节点加入到这个集群中，然后把热点 Slot 迁移到这个热点主从上。因为热点主从上只有热点 Slot 的请求，所以热点 Key 的处理能力得到了大幅提升。通过这样的设计，我们可以做到实时的热点监控，并及时通过流控去止损；通过热点迁移，我们能做到自动的热点隔离和快速的容量扩充。

八. 集群宕机处理, 数据复制问题
全量与增量同步

九. WAL 考虑使用 mmap 实现, 提升 IO 性能
https://www.cnblogs.com/oxspirt/p/14633182.html


Cockroach 对 MultiRaft 的定义
> In CockroachDB, we use the Raft consensus algorithm to ensure that your data remains consistent even when machines fail. In most systems that use Raft, such as etcd and Consul, the entire system is one Raft consensus group. In CockroachDB, however, the data is divided into ranges, each with its own consensus group. This means that each node may be participating in hundreds of thousands of consensus groups. This presents some unique challenges, which we have addressed by introducing a layer on top of Raft that we call MultiRaft.

hMultiRaft 参考:

https://open.oceanbase.com/blog/10900387

https://zhuanlan.zhihu.com/p/33047950

https://cn.pingcap.com/blog/the-design-and-implementation-of-multi-raft/

https://blog.csdn.net/JSWANGCHANG/article/details/122534755

https://ggaaooppeenngg.github.io/zh-CN/2017/04/07/MultiRaft-%E8%A7%A3%E6%9E%90/



分布式 KV 系统参考 : https://tech.meituan.com/2020/07/01/kv-squirrel-cellar.html

