FROM ubuntu:24.04

# Set environment variables to avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# TODO check for unnecessary packages and remove them

# Update the package list and install development essentials
RUN apt-get update && apt-get install -y \
    # build-essential \
    # g++ \
    # gcc \
    libbz2-dev \
    libicu-dev \
    # zlib1g-dev \
    # cmake \
    # git \
    wget \
    curl \
    libpsl-dev \
    libidn11-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    python3 \
    python3-pip \
    python3-venv \
    vim \
    libopenmpi-dev \
    openmpi-bin \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

CMD ["/bin/bash"]
