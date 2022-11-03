#!/usr/bin/env bash

set -f

dx-registry-login() {
CREDENTIALS=${HOME}/credentials
dx download "${docker_creds}" -o $CREDENTIALS

command -v docker >/dev/null 2>&1 || (echo "ERROR: docker is required when running with the Docker credentials."; exit 1)

export REGISTRY=$(jq '.docker_registry.registry' "$CREDENTIALS" | tr -d '"')
export REGISTRY_USERNAME=$(jq '.docker_registry.username' "$CREDENTIALS" | tr -d '"')
export REGISTRY_ORGANIZATION=$(jq '.docker_registry.organization' "$CREDENTIALS" | tr -d '"')
if [[  -z $REGISTRY_ORGANIZATION || $REGISTRY_ORGANIZATION == "null" ]]; then
    export REGISTRY_ORGANIZATION=$REGISTRY_USERNAME
fi

if [[ -z $REGISTRY || $REGISTRY == "null" \
      || -z $REGISTRY_USERNAME  || $REGISTRY_USERNAME == "null" ]]; then
    echo "Error parsing the credentials file. The expected format to specify a Docker registry is: "
    echo "{"
    echo "    docker_registry: {"
    echo "        registry": "<Docker registry name, e.g. quay.io or docker.io>",
    echo "        username": "<registry login name>",
    echo "        organization": "<(optional, default value equals username) organization as defined by DockerHub or Quay.io>",
    echo "        token": "<API token>"
    echo "    }"
    echo "}"
    exit 1
fi

jq '.docker_registry.token' "$CREDENTIALS" -r | docker login $REGISTRY --username $REGISTRY_USERNAME --password-stdin 2> >(grep -v -E "WARNING! Your password will be stored unencrypted in |Configure a credential helper to remove this warning. See|https://docs.docker.com/engine/reference/commandline/login/#credentials-store")
}

generate_runtime_config() {
  touch nxf_runtime.config
  # make a runtime config file to override optional inputs
  # whose defaults are defined in the default pipeline config such as RESOURCES_SUBPATH/nextflow.config
  @@GENERATE_RUNTIME_CONFIG@@

  RUNTIME_CONFIG=''
  if [[ -s nxf_runtime.config ]]; then
    if [[ $debug == true ]]; then
      cat nxf_runtime.config
    fi

    RUNTIME_CONFIG='-c nxf_runtime.config'
  fi
}

on_exit() {
  ret=$?

  set +x
  if [[ $debug == true ]]; then
    # DEVEX-1943 Wait up to 30 seconds for log forwarders to terminate
    set +e
    i=0
    while [[ $i -lt 30 ]];
    do
        if kill -0 "$LOG_MONITOR_PID" 2>/dev/null; then
            sleep 1
        else
            break
        fi
        ((i++))
    done
    kill $LOG_MONITOR_PID 2>/dev/null || true
    set -xe
  fi


  # remove .nextflow from the current folder /home/dnanexus/output_files
  rm -rf .nextflow

  # try uploading the log file if it is not empty
  if [[ -s $LOG_NAME ]]; then
    mkdir ../nextflow_log
    mv $LOG_NAME ../nextflow_log/$LOG_NAME || true
  else
    echo "No nextflow log file available."
  fi
  
  # upload the log file and published files if any
  cd ..
  if [[ -d ./nextflow_log || -n "$(ls -A ./output_files)" ]]; then
    dx-upload-all-outputs --parallel || true
  else
    echo "No log file or output files has been generated."
  fi
  # done
  exit $ret
}

dx_path() {
  local str=${1#"dx://"}
  local tmp=$(mktemp -t nf-XXXXXXXXXX)
  case $str in
    project-*)
      dx download $str -o $tmp --no-progress --recursive -f
      echo file://$tmp
      ;;
    container-*)
      dx download $str -o $tmp --no-progress --recursive -f
      echo file://$tmp
      ;;
    *)
      echo "Invalid $2 path: $1"
      return 1
      ;;
  esac
}
    
main() {
    if [[ $debug == true ]]; then
      export NXF_DEBUG=2
      TRACE_CMD="-trace nextflow.plugin"
      set -x && env | grep -v DX_SECURITY_CONTEXT | sort
    fi
    
    if [ -n "$docker_creds" ]; then
        dx-registry-login
    fi

    DX_WORK=${work_dir:-$DX_WORKSPACE_ID:/scratch/}

    LOG_NAME="nextflow-$(date +"%y%m%d-%H%M%S").log"
    # parse dnanexus-job.json to get job output destination
    OUT_PROJECT=$(jq -r .project /home/dnanexus/dnanexus-job.json)
    OUT_FOLDER=$(jq -r .folder /home/dnanexus/dnanexus-job.json)
    OUTDIR="$OUT_PROJECT:${OUT_FOLDER#/}"
    DX_LOG=${log_file:-"$OUTDIR/$LOG_NAME"}

    export NXF_WORK=dx://$DX_WORK
    export NXF_HOME=/opt/nextflow
    export NXF_UUID=$(uuidgen)
    export NXF_ANSI_LOG=false
    export NXF_EXECUTOR=dnanexus
    export NXF_PLUGINS_DEFAULT=nextaur@1.1.0
    export NXF_DOCKER_LEGACY=true
    #export NXF_DOCKER_CREDS_FILE=$docker_creds_file
    #[[ $scm_file ]] && export NXF_SCM_FILE=$(dx_path $scm_file 'Nextflow CSM file')

    mkdir -p /home/dnanexus/out/output_files
    cd /home/dnanexus/out/output_files
    required_inputs=""
    @@REQUIRED_RUNTIME_PARAMS@@

    generate_runtime_config
    
    NEXTFLOW_CMD="nextflow \
      ${TRACE_CMD} \
      $nextflow_top_level_opts \
      ${RUNTIME_CONFIG} \
      -log ${LOG_NAME} \
      run @@RESOURCES_SUBPATH@@ \
      @@PROFILE_ARG@@ \
      -name run-${NXF_UUID} \
      $nextflow_run_opts \
      $nextflow_pipeline_params \
      $required_inputs & NXF_EXEC_PID=$!
      "

    trap on_exit EXIT
    echo "============================================================="
    echo "=== NF work-dir : ${DX_WORK}"
    echo "=== NF log file : ${DX_LOG}"
    echo "=== NF cache    : $DX_PROJECT_CONTEXT_ID:/.nextflow/cache/$NXF_UUID"
    echo "=== NF command  :" $NEXTFLOW_CMD
    echo "============================================================="

    $NEXTFLOW_CMD & NXF_EXEC_PID=$!
    
    # forwarding nextflow log file to job monitor
    set +x
    if [[ $debug == true ]] ; then
      touch $LOG_NAME
      tail --follow -n 0 $LOG_NAME -s 60 >&2 & LOG_MONITOR_PID=$!
      disown $LOG_MONITOR_PID
      set -x
    fi
    
    wait $NXF_EXEC_PID
    ret=$?
    exit $ret
}


nf_task_exit() {
  ret=$?
  if [ -f .command.log ]; then
    dx upload .command.log --path "${cmd_log_file}" --brief --wait --no-progress || true
  else
    >&2 echo "Missing Nextflow .command.log file"
  fi
  # mark the job as successful in any case, real task
  # error code is managed by nextflow via .exitcode file
  dx-jobutil-add-output exit_code "0" --class=int
}

nf_task_entry() {
  # enable debugging mode
  [[ $NXF_DEBUG ]] && set -x
  if [ -n "$docker_creds" ]; then
    dx-registry-login
  fi
  # capture the exit code
  trap nf_task_exit EXIT
  # run the task
  dx cat "${cmd_launcher_file}" > .command.run
  bash .command.run > >(tee .command.log) 2>&1 || true
}
