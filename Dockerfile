# syntax=docker/dockerfile:1

FROM debian:bookworm
RUN mkdir -p /opt/bedrock/metaserver/
RUN mkdir -p /opt/bedrock/metaserver/data/wal/
WORKDIR /opt/bedrock/metaserver/
COPY metaserver metaserver
COPY mscli mscli
COPY config.sample.toml config.toml
EXPOSE 2379
EXPOSE 2380
EXPOSE 1080
CMD ["./metaserver", "-c", "config.toml"]
