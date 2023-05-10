FROM --platform=amd64 fedora:38

SHELL ["/bin/bash", "-c"]
ENV DXPY_TEST_PYTHON_VERSION=3

RUN \
    dnf install -y which diffutils python3 python3-pip && \
    python3 -m venv /pytest-env

RUN \
    source /pytest-env/bin/activate && \
    python3 -m pip install --quiet pytest pexpect

RUN \
    dnf install -y \
        python3-argcomplete \
        python3-cryptography \
        python3-dateutil \
        python3-psutil \
        python3-requests \
        python3-websocket-client

COPY run_tests.sh /

ENTRYPOINT [ "/run_tests.sh" ]
