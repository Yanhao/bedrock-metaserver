FROM golang:alpine AS builder

RUN sed -i 's#https\?://dl-cdn.alpinelinux.org/alpine#https://mirrors.tuna.tsinghua.edu.cn/alpine#g' /etc/apk/repositories
RUN apk add --no-cache git gcc musl-dev make protoc protobuf-dev
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.0
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN make
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bedrock-metaserver
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o mscli cmd/client/mscli.go

FROM debian:trixie-slim

RUN mkdir -p /opt/bedrock/metaserver/
RUN mkdir -p /app/data/etcd/wal

WORKDIR /opt/bedrock/metaserver/

COPY --from=builder /app/bedrock-metaserver metaserver
COPY --from=builder /app/mscli mscli

RUN ln -s /opt/bedrock/metaserver/config.toml /app/config.toml || true

EXPOSE 2379
EXPOSE 2380
EXPOSE 1080
EXPOSE 6063

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
