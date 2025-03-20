#!/bin/bash

# 安装指定版本的 protoc
PB_VER=25.0
if ! command -v protoc &> /dev/null; then
  curl -LO "https://github.com/protocolbuffers/protobuf/releases/download/v${PB_VER}/protoc-${PB_VER}-linux-x86_64.zip"
  unzip protoc-${PB_VER}-linux-x86_64.zip -d $HOME/.local
  export PATH="$PATH:$HOME/.local/bin"
fi
# 安装指定版本的工具
PROTOC_GEN_GO_VERSION="v1.31.0"
PROTOC_GEN_GO_GRPC_VERSION="v1.3.0"
go install google.golang.org/protobuf/cmd/protoc-gen-go@$PROTOC_GEN_GO_VERSION
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$PROTOC_GEN_GO_GRPC_VERSION
export PATH="$PATH:$(go env GOPATH)/bin"

# 生成代码
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative,require_unimplemented_servers=false \
       cluster_service.proto
