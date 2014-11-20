#!/bin/bash

set -e

if [[ $1 == "" ]]; then
    PORT=5000
else
    PORT=$((5000+$1))
fi

if [[ $2 == "" ]]; then
    SCRATCH_DIR=.
else
    SCRATCH_DIR=$2
fi

./api.py --port $PORT > /dev/null 2>&1 &
MOCK_SERVER_PID=$!

cleanup() {
    kill $MOCK_SERVER_PID
}

trap cleanup EXIT

export DX_APISERVER_HOST=localhost
#export DX_APISERVER_HOST=10.0.3.1
export DX_APISERVER_PORT=$PORT
export DX_APISERVER_PROTOCOL=http
export DX_JOB_ID=job-0123456789ABCDEF01234567
export DX_PROJECT_CONTEXT_ID=project-0123456789ABCDEF01234567
export DX_WORKSPACE_ID=container-0123456789ABCDEF01234567
export DX_CLI_WD=/
#export _DX_DEBUG=1

for i in {1..8}; do
    dx api system setPayload >/dev/null
    dx download test --output ${SCRATCH_DIR}/$PORT -f 2>/dev/null
    wire_md5=$(md5sum ${SCRATCH_DIR}/$PORT | cut -f 1 -d " ")
    desc_md5=$(dx api file-test describe | jq --raw-output .md5)
    echo $wire_md5 $desc_md5
    if [[ $wire_md5 != $desc_md5 ]]; then
        echo $(date) $i $wire_md5 $desc_md5 >> ERR_LOG
        mv -f ${SCRATCH_DIR}/$PORT ${SCRATCH_DIR}/dl_check.${PORT}.$i
        dx download test --output ${SCRATCH_DIR}/dl_check.${PORT}.${i}.retry -f
        #cmp dl_check.${PORT}.$i dl_check.${PORT}.${i}.retry
    fi
done
