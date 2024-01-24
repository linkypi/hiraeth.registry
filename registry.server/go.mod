module github.com/linkypi/hiraeth.registry/server

go 1.20

require (
	github.com/emicklei/go-restful v2.16.0+incompatible
	github.com/fatih/set v0.2.1
	github.com/golang/protobuf v1.5.3
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/raft v1.6.0
	github.com/hashicorp/raft-boltdb v0.0.0-20231211162105-6c830fa4535e
	github.com/json-iterator/go v1.1.12
	github.com/lestrrat-go/file-rotatelogs v2.4.0+incompatible
	github.com/linkypi/hiraeth.registry/common v1.0.0
	github.com/panjf2000/gnet v1.6.7
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.9.3
	github.com/sourcegraph/conc v0.3.0
	google.golang.org/grpc v1.61.0
	google.golang.org/protobuf v1.32.0
)

replace github.com/linkypi/hiraeth.registry/common => ../registry.common

require (
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-hclog v1.6.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.1 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/lestrrat-go/strftime v1.0.6 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/panjf2000/ants/v2 v2.9.0 // indirect
	github.com/prometheus/client_golang v1.18.0 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common v0.46.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/shaj13/raft v0.0.0-20230207130729-18878c2751ed // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.11 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.11 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.11 // indirect
	go.etcd.io/etcd/server/v3 v3.5.11 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.26.0 // indirect
	golang.org/x/net v0.20.0 // indirect
	golang.org/x/sync v0.6.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240123012728-ef4313101c80 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)
