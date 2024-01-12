FROM --platform=amd64 almalinux:9

SHELL ["/bin/bash", "-c"]
ENV DXPY_TEST_PYTHON_VERSION=3

RUN \
    dnf install -y epel-release && \
    dnf install -y which diffutils python3 python3-pip && \
    python3 -m venv /pytest-env

RUN \
    source /pytest-env/bin/activate && \
    python3 -m pip install --quiet pytest pexpect

COPY run_tests.sh /

RUN \
    dnf install -y \
        python3-argcomplete \
        python3-dateutil \
        python3-psutil \
        python3-urllib3 \
        python3-websocket-client

ENTRYPOINT [ "/run_tests.sh" ]
