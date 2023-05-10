FROM --platform=amd64 ubuntu:18.04

SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV DXPY_TEST_PYTHON_VERSION=2

RUN \
    apt-get update && \
    apt-get install -y python python-pip python3-pip python3-venv && \
    python2 -m pip install --quiet --upgrade 'pip<21.0' && \
    python3 -m venv /pytest-env

RUN \
    source /pytest-env/bin/activate && \
    python3 -m pip install --quiet pytest pexpect

RUN \
    apt-get install -y \
        python-argcomplete \
        python-cryptography \
        python-dateutil \
        python-psutil \
        python-requests \
        python-websocket

COPY run_tests.sh /

ENTRYPOINT [ "/run_tests.sh" ]
