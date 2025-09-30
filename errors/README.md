# 错误处理包使用指南

## 概述

本错误处理包提供了一套统一的错误处理机制，用于在 Bedrock MetaServer 中标准化错误的创建、包装、传递和检查。它包含错误码定义、错误类型、创建和包装错误的函数，以及检查错误类型的辅助函数。

## 特性

- **统一的错误类型**：提供了包含错误码、错误信息和原始错误的自定义错误类型
- **堆栈跟踪**：自动捕获和保存错误发生时的堆栈信息，方便调试
- **错误包装**：支持在不丢失原始错误信息的情况下添加更多上下文信息
- **错误类型检查**：提供辅助函数检查错误是否为特定类型
- **预定义错误**：包含常用的预定义错误，减少重复代码

## 错误码

错误码分为系统级错误和业务级错误：

### 系统级错误
- `ErrCodeInternal`：内部错误
- `ErrCodeTimeout`：操作超时
- `ErrCodeDatabase`：数据库错误
- `ErrCodeNetwork`：网络错误
- `ErrCodeResourceExhausted`：资源耗尽
- `ErrCodeNotLeader`：不是领导者节点
- `ErrCodeNoAvailableResource`：没有可用资源
- `ErrCodeUnsupported`：不支持的操作

### 业务级错误
- `ErrCodeNotFound`：资源不存在
- `ErrCodeAlreadyExists`：资源已存在
- `ErrCodeInvalidArgument`：参数无效
- `ErrCodeUnauthorized`：未授权
- `ErrCodeForbidden`：禁止访问
- `ErrCodeFailedPrecondition`：前提条件失败
- `ErrCodePermissionDenied`：权限拒绝

## 使用方法

### 创建新错误

```go
import "sr.ht/moyanhao/bedrock-metaserver/errors"

// 创建简单错误
err := errors.New(errors.ErrCodeInvalidArgument, "invalid parameter")

// 创建带格式化消息的错误
err := errors.Newf(errors.ErrCodeNotFound, "shard %d not found", shardID)
```

### 包装错误

```go
// 包装简单错误
err := someFunction()
if err != nil {
	return errors.Wrap(err, errors.ErrCodeInternal, "failed to process request")
}

// 包装带格式化消息的错误
err := someFunction()
if err != nil {
	return errors.Wrapf(err, errors.ErrCodeDatabase, "failed to query database: %s", query)
}
```

### 检查错误类型

```go
err := someFunction()

if errors.IsNotFound(err) {
	// 处理资源不存在的情况
} else if errors.IsInvalidArgument(err) {
	// 处理参数无效的情况
} else if errors.IsInternal(err) {
	// 处理内部错误
}

// 也可以使用标准库的errors包
err := someFunction()
if errors.Is(err, errors.ErrNoSuchShard) {
	// 处理分片不存在的情况
}
```

### 预定义错误

包中提供了常用的预定义错误，可以直接使用：

```go
// 使用预定义错误
if shard == nil {
	return errors.ErrNoSuchShard
}

// 或包装预定义错误
err := someFunction()
if err != nil {
	return errors.Wrap(err, errors.ErrCodeInternal, "operation failed")
}
```

## 错误处理最佳实践

1. **错误创建**：使用 `errors.New` 或 `errors.Newf` 创建新错误，提供明确的错误码和描述性消息
2. **错误包装**：在函数调用链中，使用 `errors.Wrap` 或 `errors.Wrapf` 添加上下文信息
3. **错误检查**：使用提供的辅助函数（如 `IsNotFound`, `IsInvalidArgument` 等）检查错误类型
4. **错误日志**：记录错误时，优先使用包含堆栈信息的完整错误内容
5. **错误传播**：避免在中间层吞掉错误，应向上传播或包装后再传播
6. **错误转换**：在 API 边界处，将内部错误转换为适合外部使用的错误格式

## 与 gRPC 集成

在 gRPC 服务中使用错误处理包：

```go
import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sr.ht/moyanhao/bedrock-metaserver/errors"
)

func (s *server) SomeRPC(ctx context.Context, req *pb.SomeRequest) (*pb.SomeResponse, error) {
	// 业务逻辑
	if err := someFunction(); err != nil {
		// 记录错误日志
		log.Printf("Error in SomeRPC: %v", err)
		
		// 将内部错误转换为 gRPC 错误
		var code codes.Code
		if errors.IsNotFound(err) {
			code = codes.NotFound
		} else if errors.IsInvalidArgument(err) {
			code = codes.InvalidArgument
		} else if errors.IsUnauthorized(err) {
			code = codes.Unauthenticated
		} else {
			code = codes.Internal
		}
		return nil, status.Errorf(code, "%v", err)
	}
	// 返回成功响应
	return &pb.SomeResponse{...}, nil
}
```