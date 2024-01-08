先前出于好奇自我实现了一个基于Java实现的分布式微服务注册中心 [my-registry](https://github.com/linkypi/my-registry). 最近迷上了go, 又刚好在B站看到了小徐先生对 raft 算法及 etcd raft实现的讲解, 碰巧就想到 my-registry 正是需要该算法来实现leader选举以及数据一致性保证. 于是就想着使用go基于etcd raft来实现一遍, 虽然etcd已经将raft算法独立封装成lib但是想基于它实现自己的功能还是存在不少难度:

1. 传输层 transport 仅支持 http, 虽然提供了 transport 相关接口, 但想改用grpc则需调整不少代码, 文档中可用信息很少.
2. 再一个是etcd raft 并没有提供一个通知机制来告知开发者 leader 已发生变更, 如集群异常导致重新选出leader后需要对数据重新分配或者调整数据分片

相反, hashicorp/raft 实现则提供了更好的API使用, 内部代码实现也更加清晰明了:
1. transport 提供 mem, TCP 以及 grpc 等多种方式, 
2. leader 的变更通知也可通过 LeaderCh 以及 raft Config 提供的 NotifyCh 来实现
3. 提供了操作集群的工具 [raftadmin](https://github.com/Jille/raftadmin) , 但仍缺少部分功能, 如向集群添加或移除节点 AddPeer/RemovePeer
4. 提供了状态机接口 FSM 用于处理写请求完成后的相关操作
当然还有部分功能需要自行实现, 如集群启动时并没有提供各个节点发现功能, 而需要raftadmin来处理. 同时 LeaderCh 是 bool 类型的 channel，不带缓存，当本节点的 leader 状态有变化的时候，会往这个 channel 里面写数据，但是由于不带缓冲且写数据的协程不会阻塞在这里，有可能会写入失败，没有及时变更状态，所以使用 leaderCh 的可靠性不能保证。好在 raft Config 里面提供了另一个 channel NotifyCh，它是带缓存的，当 leader 状态变化时会往这个 chan 写数据，写入的变更消息能够缓存在 channel 里面，应用程序能够通过它获取到最新的状态变化。



``` sh
 protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=require_unimplemented_servers=false InternalService.proto 
```
### 1. QA
- 


```shell

go get go.etcd.io/etcd/raft/v3
go get go.etcd.io/etcd/wal

```

