FROM ubuntu:24.04

# Docker image for building dx-toolkit components,
# tested so far for upload-agent, dx-verify-file. May need
# additional work to support other components.

# Same Dockerfile used in
# 1. dx-toolkit/build/docker/Dockerfile
# 2. ci-scripts/docker_images/upload-agent-tester/Dockerfile

# Set environment variables to avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    cmake \
    curl \
    g++ \
    gcc \
    git \
    libboost-all-dev \
    libbz2-dev \
    libcurl4-openssl-dev \
    libicu-dev \
    libidn11-dev \
    libssl-dev \
    make \
    python3 \
    python3-pip \
    python3-venv \
    virtualenv \
    zlib1g-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

CMD ["/bin/bash"]
