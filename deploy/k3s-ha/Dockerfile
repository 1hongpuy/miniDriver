FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       libleveldb1d \
       libssl3 \
       libspdlog1 \
       libfmt8 \
       libhiredis0.14 \
       libsnappy1v5 \
    && rm -rf /var/lib/apt/lists/*

COPY build-ha/bin/minikv_metadata_raft_service /usr/local/bin/
COPY build-ha/bin/minikv_v2_gateway /usr/local/bin/
COPY build-ha/bin/minikv_v2_datanode /usr/local/bin/
COPY build-ha/bin/minidriver_edge_cache /usr/local/bin/
COPY build-ha/bin/minikv_v2_bench /usr/local/bin/
COPY build-ha/bin/minikv_storage_sync_probe /usr/local/bin/

RUN groupadd --system --gid 10001 minikv \
    && useradd --system --uid 10001 --gid 10001 --create-home minikv \
    && mkdir -p /var/lib/minidriver \
    && chown -R 10001:10001 /var/lib/minidriver

USER 10001:10001
WORKDIR /var/lib/minidriver
ENTRYPOINT []
