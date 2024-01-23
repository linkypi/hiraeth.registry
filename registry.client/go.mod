module github.com/linkypi/hiraeth.registry/client

go 1.20

require (
	github.com/panjf2000/gnet v1.6.7
	github.com/sirupsen/logrus v1.9.3
	google.golang.org/protobuf v1.31.0
)

require (
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/lestrrat-go/file-rotatelogs v2.4.0+incompatible // indirect
	github.com/lestrrat-go/strftime v1.0.6 // indirect
	github.com/panjf2000/ants/v2 v2.4.7 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	golang.org/x/net v0.16.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231002182017-d307bd883b97 // indirect
	google.golang.org/grpc v1.60.1 // indirect
)

replace github.com/linkypi/hiraeth.registry/common => ../registry.common

require (
	github.com/linkypi/hiraeth.registry/common v1.0.0
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	go.uber.org/zap v1.19.1 // indirect
	golang.org/x/sys v0.16.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
)
