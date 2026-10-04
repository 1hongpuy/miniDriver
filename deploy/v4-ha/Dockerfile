# Local 4.0-HA validation image.
#
# The binaries are built with -DMINIKV_ENABLE_NURAFT=ON in build-ha before
# this image is created.  Keeping the image assembly separate from the
# source build makes the Compose contract usable in an offline development
# environment while still failing clearly if the HA artifacts are missing.
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl libleveldb1d libssl3 libspdlog1 libhiredis0.14 \
      libsnappy1v5 libyaml-cpp0.7 libraw20 \
    && rm -rf /var/lib/apt/lists/*

COPY build-ha/bin/minikv_metadata_raft_service /usr/local/bin/
COPY build-ha/bin/minikv_v2_gateway /usr/local/bin/
COPY build-ha/bin/minikv_v2_datanode /usr/local/bin/

RUN useradd --system --uid 10001 --create-home minikv \
    && mkdir -p /var/lib/minidriver \
    && chown -R minikv:minikv /var/lib/minidriver

USER minikv
WORKDIR /var/lib/minidriver
ENTRYPOINT []
