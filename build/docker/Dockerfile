FROM ubuntu:24.04

# Docker image for building Upload Agent.
# This could be extended to support building other dx-toolkit components.
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
    libboost-all-dev \
    libbz2-dev \
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
