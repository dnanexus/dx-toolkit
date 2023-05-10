FROM --platform=amd64 debian:11

SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV DXPY_TEST_PYTHON_VERSION=3

RUN \
    apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    python3 -m venv /pytest-env

RUN \
    source /pytest-env/bin/activate && \
    python3 -m pip install --quiet pytest pexpect

RUN \
    apt-get install -y \
        python3-argcomplete \
        python3-cryptography \
        python3-dateutil \
        python3-psutil \
        python3-requests \
        python3-websocket

COPY run_tests.sh /

ENTRYPOINT [ "/run_tests.sh" ]
