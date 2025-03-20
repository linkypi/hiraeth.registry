@echo off
setlocal

REM 安装指定版本的 protoc
set PB_VER=25.0
where protoc >nul 2>&1
if errorlevel 1 (
    curl -LO "https://github.com/protocolbuffers/protobuf/releases/download/v%PB_VER%/protoc-%PB_VER%-win64.zip"
    powershell -Command "Expand-Archive -Path protoc-%PB_VER%-win64.zip -DestinationPath %USERPROFILE%\.local"
    set PATH="%PATH%;%USERPROFILE%\.local\bin"
)

REM 安装指定版本的工具
set PROTOC_GEN_GO_VERSION=v1.31.0
set PROTOC_GEN_GO_GRPC_VERSION=v1.3.0
go install google.golang.org/protobuf/cmd/protoc-gen-go@%PROTOC_GEN_GO_VERSION%
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@%PROTOC_GEN_GO_GRPC_VERSION%
set PATH="%PATH%;%GOPATH%\bin"

rem 生成代码
protoc --go_out=. --go_opt=paths=source_relative ^
       --go-grpc_out=. --go-grpc_opt=paths=source_relative,require_unimplemented_servers=false ^
       cluster_service.proto

endlocal