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

  RUNTIME_CONFIG_CMD=''
  if [[ -s nxf_runtime.config ]]; then
    if [[ $debug == true ]]; then
      cat nxf_runtime.config
    fi

    RUNTIME_CONFIG_CMD='-c nxf_runtime.config'
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

  # backup cache
  if [[ $no_future_resume == false ]]; then
    echo "=== Execution complete — uploading Nextflow cache and history file"
    # TBD: overwritten previous cache?
    # dx rm -r "$DX_PROJECT_CONTEXT_ID:/.nextflow/$NXF_UUID/*" 2>&1 >/dev/null || true
    # files in workdir: $DX_PROJECT_CONTEXT_ID:/.nextflow/$NXF_UUID/scratch/
    # should be uploaded to $DX_PROJECT_CONTEXT_ID:/.nextflow/$NXF_UUID/work/ by the plugin after each subjob
    # so we shall only upload cache and history file
    tar -cvf .nextflow/cache.tar .nextflow/cache
    dx upload ".nextflow/cache.tar" --path "$DX_PROJECT_CONTEXT_ID:/.nextflow/$NXF_UUID/" --no-progress --brief --wait -p -r || true

    cat "$HISTORY_FILE"
    download_prev_history
    if [[ -s prev_history ]]; then
      sort -mu prev_history "$HISTORY_FILE" -o "$HISTORY_FILE"
      cat "$HISTORY_FILE"
      dx rm "$DX_PROJECT_CONTEXT_ID:/.nextflow/history"
      rm prev_history
    fi
    dx upload "$HISTORY_FILE" --path "$DX_PROJECT_CONTEXT_ID:/.nextflow/history" --no-progress --brief --wait -p -r || true
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

restore_cache_and_history() {
  # get session id if specified
  if [[ -n "$resume_session" ]]; then
    NXF_UUID=$resume_session
  else
    # find the latest job of this applet
    EXECUTABLE_ID=$(jq -r .executable /home/dnanexus/dnanexus-job.json)
    PREV_JOB_ID=$(dx find executions --executable "$EXECUTABLE_ID" --origin-jobs --brief --project $DX_PROJECT_CONTEXT_ID | sed -n 2p)
    if [[ -z $PREV_JOB_ID ]]; then
      dx-jobutil-report-error "Cannot find a previous session ran by $EXECUTABLE_ID."
    fi
    # get session id of this latest job
    NXF_UUID=$(dx describe "$PREV_JOB_ID" --json | jq .properties.session_id)
    if [[ -z $NXF_UUID ]]; then
      dx-jobutil-report-error "Cannot retrieve the session ID of previous job $PREV_JOB_ID."
    fi
  fi

  if [[ $debug == true ]]; then
    echo "Will resume from previous session: $NXF_UUID"
  fi

  # download $DX_PROJECT_CONTEXT_ID:/.nextflow/$NXF_UUID/cache.tar --> .nextflow/cache.tar
  local ret
  ret=$(dx download "$DX_PROJECT_CONTEXT_ID:/.nextflow/$NXF_UUID/cache.tar" --no-progress -f -o .nextflow/cache.tar 2>&1) ||
    {
      if [[ $ret == *"FileNotFoundError"* ]]; then
        dx-jobutil-report-error "No previous execution cache of session $NXF_UUID was found."
      else
        dx-jobutil-report-error "$ret"
      fi
    }

  # untar cache.tar, which contains
  # 1. cache folder .nextflow/cache/$NXF_UUID
  # 2. history of previous session .nextflow/cache/latest_history
  tar -xvf .nextflow/cache.tar
  if [[ -z "$(ls -A .nextflow/cache/$NXF_UUID)" ]]; then
    dx-jobutil-report-error "Previous execution cache of session $NXF_UUID is empty."
  fi

  if [[ -s ".nextflow/cache/latest_history" ]]; then
    mv ".nextflow/cache/latest_history" ".nextflow/history"
  else
    dx-jobutil-report-error "Missing history file in restored cache of previous session $NXF_UUID."
  fi
}

download_prev_history() {
  local ret
  ret=$(dx download "$DX_PROJECT_CONTEXT_ID:/.nextflow/history" --no-progress -f -o prev_history 2>&1) ||
    {
      if [[ $ret == *"FileNotFoundError"* || $ret == *"ResolutionError"* ]]; then
        echo "No history file found as $DX_PROJECT_CONTEXT_ID:/.nextflow/history"
      else
        dx-jobutil-report-error "$ret"
      fi
    }
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
  export NXF_DOCKER_LEGACY=true
  #export NXF_DOCKER_CREDS_FILE=$docker_creds_file
  #[[ $scm_file ]] && export NXF_SCM_FILE=$(dx_path $scm_file 'Nextflow CSM file')

  # parse dnanexus-job.json to get job output destination
  OUT_PROJECT=$(jq -r .project /home/dnanexus/dnanexus-job.json)
  OUT_FOLDER=$(jq -r .folder /home/dnanexus/dnanexus-job.json)
  OUTDIR="$OUT_PROJECT:${OUT_FOLDER#/}"

  # initiate log file
  LOG_NAME="nextflow-$(date +"%y%m%d-%H%M%S").log"
  DX_LOG=${log_file:-"$OUTDIR/$LOG_NAME"}

  # set NXF env constants
  export NXF_HOME=/opt/nextflow
  export NXF_ANSI_LOG=false
  export NXF_EXECUTOR=dnanexus
  export NXF_PLUGINS_DEFAULT=nextaur@1.1.0

  # use /home/dnanexus/out/output_files as the temporary nextflow execution folder
  mkdir -p /home/dnanexus/out/output_files 
  cd /home/dnanexus/out/output_files
  mkdir -p .nextflow/cache

  # restore cache and set/create current session id
  RESUME_CMD=""
  if [[ $resume == true ]]; then
    restore_cache_and_history
    RESUME_CMD="-resume $NXF_UUID"
  elif [[ -n "$resume_session" ]]; then
    dx-jobutil-report-error "Session was provided, but resume functionality was not allowed. Please set input 'resume' as true and try again."
  else
    NXF_UUID=$(uuidgen)
  fi
  export NXF_UUID
  dx set_properties "$DX_JOB_ID" "session_id=$NXF_UUID"

  # set workdir
  DX_WORK="$DX_PROJECT_CONTEXT_ID:/.nextflow/$NXF_UUID/work/"
  export NXF_WORK=dx://$DX_WORK

  generate_runtime_config

  trap on_exit EXIT
  echo "============================================================="
  echo "=== NF work-dir : ${DX_WORK}"
  echo "=== NF log file : ${DX_LOG}"
  echo "=== NF cache    : $DX_PROJECT_CONTEXT_ID:/.nextflow/$NXF_UUID/cache.tar"
  echo "============================================================="

  set -x
  nextflow \
    ${TRACE_CMD} \
    $nextflow_top_level_opts \
    ${RUNTIME_CONFIG_CMD} \
    -log ${LOG_NAME} \
    run @@RESOURCES_SUBPATH@@ \
    @@PROFILE_ARG@@ \
    -name $DX_JOB_ID \
    # TODO: resume command
    $nextflow_run_opts \
    $nextflow_pipeline_params \
    @@REQUIRED_RUNTIME_PARAMS@@ &
  NXF_EXEC_PID=$!
  set +x

  # forwarding nextflow log file to job monitor
  if [[ $debug == true ]]; then
    touch $LOG_NAME
    tail --follow -n 0 $LOG_NAME -s 60 >&2 &
    LOG_MONITOR_PID=$!
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
