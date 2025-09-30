# Bedrock MetaServer 测试集群快速启动指南

这个文档介绍如何使用提供的 `docker-compose.yml` 文件快速启动一个 Bedrock MetaServer 测试集群。

## 环境准备

在开始之前，请确保您的系统已经安装了以下软件：
- Docker：https://docs.docker.com/get-docker/
- Docker Compose：https://docs.docker.com/compose/install/

## 集群组件

这个测试集群包含以下组件：

1. **MetaServer 集群**：由3个节点组成，每个节点内置嵌入式 Etcd 实例，用于存储元数据

> **注意**：Bedrock MetaServer 使用嵌入式 Etcd（embed模式），不需要单独部署 Etcd 集群。每个 MetaServer 节点会启动自己的嵌入式 Etcd 实例，这些实例会自动组成集群。

## 快速启动步骤

### 1. 构建 MetaServer 镜像

首先，需要构建 MetaServer 镜像。在项目根目录执行以下命令：

```bash
# 编译 MetaServer 二进制文件
go build -o metaserver sr.ht/moyanhao/bedrock-metaserver

# 编译客户端工具（可选）
go build -o mscli sr.ht/moyanhao/bedrock-metaserver/cmd/client/mscli.go
```

### 2. 启动测试集群

使用 Docker Compose 启动整个测试集群：

```bash
docker-compose up -d
```

这个命令会：
- 构建并启动3个 MetaServer 节点（每个包含嵌入式 Etcd 实例）

### 3. 查看集群状态

检查所有容器是否正常启动：

```bash
docker-compose ps
```

查看 MetaServer 日志：

```bash
docker-compose logs metaserver-1
```

### 4. 访问服务

服务启动后，可以通过以下端口访问：

- MetaServer 1: http://localhost:1080
- MetaServer 2: http://localhost:1081
- MetaServer 3: http://localhost:1082

嵌入式 Etcd 客户端端口：
- MetaServer 1 Etcd: http://localhost:2379
- MetaServer 2 Etcd: http://localhost:2479
- MetaServer 3 Etcd: http://localhost:2579

### 5. 停止集群

要停止整个测试集群，可以运行：

```bash
docker-compose down
```

要停止集群并删除所有数据卷（会删除所有测试数据）：

```bash
docker-compose down -v
```

## 注意事项

1. **持久化数据**：MetaServer 和嵌入式 Etcd 数据存储在 Docker 数据卷中，使用 `docker-compose down` 不会删除这些数据，使用 `docker-compose down -v` 会删除所有数据。

2. **配置文件**：`config.toml` 文件包含了 MetaServer 和嵌入式 Etcd 的配置参数，您可以根据需要修改这些参数。配置文件中的 `name = "${METASERVER_NAME}"` 会从 Docker Compose 环境变量中获取每个节点的唯一名称。

3. **自定义网络**：集群使用了名为 `bedrock-network` 的自定义网络，所有服务都在这个网络中运行。

4. **嵌入式 Etcd**：嵌入式 Etcd 模式简化了部署，但每个 MetaServer 实例需要额外的资源来运行 Etcd。

## 高级使用

### 扩展集群

要扩展集群，可以修改 `docker-compose.yml` 文件，增加更多的 MetaServer 节点。

### 查看嵌入式 Etcd 集群状态

使用 etcdctl 工具查看嵌入式 Etcd 集群状态：

```bash
docker exec -it metaserver-1 /usr/local/bin/etcdctl --endpoints=http://localhost:2379 endpoint status --write-out=table
```

## 故障排除

如果遇到问题，可以查看容器日志来排查：

```bash
docker-compose logs <service-name>
```

常见问题：

1. **嵌入式 Etcd 集群启动失败**：检查网络配置是否正确，查看 MetaServer 日志获取详细错误信息，确认 `cluster_peers` 配置中的节点名称和地址是否正确
2. **MetaServer 之间无法组成集群**：确认网络连接是否正常，检查容器间的名称解析是否正常
3. **集群启动缓慢**：首次启动时可能需要较长时间，特别是嵌入式 Etcd 集群初始化
4. **端口冲突**：如果本地已有服务占用了相应端口，需要修改 `docker-compose.yml` 中的端口映射