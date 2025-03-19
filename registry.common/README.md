proto文件生成命令：

```bash
 protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:. cluster_s
ervice.proto
```

