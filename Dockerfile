# syntax=docker/dockerfile:1

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    libstdc++6 \
    librdkafka1 \
    libgflags2.2 \
    libgoogle-glog0v5 \
    libsnappy1v5 \
    zlib1g \
    libbz2-1.0 \
    liblz4-1 \
    libzstd1 \
    libjemalloc2 \
    libunwind8 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/pikiwidb-port

COPY ./bin/pika_port_go /opt/pikiwidb-port/pika_port_go
RUN chmod +x /opt/pikiwidb-port/pika_port_go

ENTRYPOINT ["/opt/pikiwidb-port/pika_port_go"]
