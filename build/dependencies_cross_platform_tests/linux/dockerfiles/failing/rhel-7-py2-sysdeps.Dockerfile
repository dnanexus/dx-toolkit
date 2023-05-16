FROM --platform=amd64 centos:7

SHELL ["/bin/bash", "-c"]
ENV DXPY_TEST_PYTHON_VERSION=2

RUN \
    yum install -y epel-release && \
    yum install -y which diffutils python python-pip python3 python3-pip && \
    python2 -m pip install --quiet --upgrade 'pip<21.0' 'setuptools<45' && \
    python3 -m pip install --quiet --upgrade pip==21.3.1 && \
    python3 -m venv /pytest-env

RUN \
    source /pytest-env/bin/activate && \
    python3 -m pip install --quiet pytest pexpect

RUN \
    yum install -y \
        python2-argparse \
        python-cryptography \
        python-dateutil \
        python-requests \
        python-websocket-client

COPY run_tests.sh /

ENTRYPOINT [ "/run_tests.sh" ]
