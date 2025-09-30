# syntax=docker/dockerfile:1

# Stage 1: Build stage
FROM golang:1.24-alpine AS builder

# Install required build tools
RUN apk add --no-cache git gcc musl-dev make protoc protobuf-dev

# Install protoc plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.0
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the main application with static linking for cross-platform compatibility
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bedrock-metaserver

# Build the command line tool with static linking
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o mscli cmd/client/mscli.go

# Stage 2: Runtime stage
FROM debian:bookworm-slim

# Create application directories
RUN mkdir -p /opt/bedrock/metaserver/
RUN mkdir -p /app/data/etcd/wal

# Set working directory
WORKDIR /opt/bedrock/metaserver/

# Copy binary files from build stage
COPY --from=builder /app/bedrock-metaserver metaserver
COPY --from=builder /app/mscli mscli

# Create symlink for config directory to match expected path in config.toml
RUN ln -s /opt/bedrock/metaserver/config.toml /app/config.toml || true

# Expose ports
EXPOSE 2379
EXPOSE 2380
EXPOSE 1080
EXPOSE 6063

# Copy and set up entrypoint script
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Set entrypoint to dynamically replace environment variables
ENTRYPOINT ["./entrypoint.sh"]
