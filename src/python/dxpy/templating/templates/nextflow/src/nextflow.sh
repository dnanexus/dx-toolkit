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

  # update project nextflow history
  update_project_history

  # backup cache
  dx set_properties "$DX_JOB_ID" "preserve_cache=$preserve_cache"
  if [[ $preserve_cache == true ]]; then
    echo "=== Execution complete — caching current session to $DX_CACHEDIR/$NXF_UUID"
    # upload local workdir (only when executor is overriden to 'local')
    # otherwise files in workdir are uploaded by the plugin after each subjob
    if [[ $NXF_EXECUTOR == 'local' ]]; then
      if [[ -d $NXF_WORK && -n "$(ls -A $NXF_WORK)" ]]; then
        ABSOLUTE_LOCAL_WORKDIR=$(realpath $NXF_WORK)
        echo $ABSOLUTE_LOCAL_WORKDIR >>local_working_files.txt
      fi
      if [[ -e local_working_files.txt ]]; then
        tar -cf local_working_env.tar -T local_working_files.txt || true
        dx rm "$DX_CACHEDIR/$NXF_UUID/local_working_env.tar" 2>&1 >/dev/null || true
        LOCAL_WORK_ENV_ID=$(dx upload local_working_env.tar --path "$DX_CACHEDIR/$NXF_UUID/local_working_env.tar" --no-progress --brief --wait -p -r) &&
          echo "Upload local work directory of current session as $LOCAL_WORK_ENV_ID" ||
          echo "Failed to upload local work directory of current session $NXF_UUID"
      fi
      rm -rf local_working_env.tar local_working_files.txt $NXF_WORK || true
    fi

    # wrap cache folder and history and upload cache.tar
    if [[ -n "$(ls -A .nextflow)" ]]; then
      tar -cf cache.tar .nextflow
      # remove any existing cache.tar with the same session id
      dx rm "$DX_CACHEDIR/$NXF_UUID/cache.tar" 2>&1 >/dev/null || true

      CACHE_ID=$(dx upload "cache.tar" --path "$DX_CACHEDIR/$NXF_UUID/cache.tar" --no-progress --brief --wait -p -r) &&
        echo "Upload cache of current session as file: $CACHE_ID" &&
        rm -f cache.tar ||
        echo "Failed to upload cache of current session $NXF_UUID"
    else
      echo "No cache is generated from this execution. Skip uploading cache."
    fi

  # preserve_cache is false
  # clean up files of this session
  else
    echo "=== Execution complete — cache and working files will not be resumable"
    nextflow clean -f $NXF_UUID || true
  fi

  # remove .nextflow from the current folder /home/dnanexus/nextflow_playground
  rm -rf .nextflow
  rm nxf_runtime.config

  # try uploading the log file if it is not empty
  if [[ -s $LOG_NAME ]]; then
    mkdir -p /home/dnanexus/out/nextflow_log
    mv "$LOG_NAME" "/home/dnanexus/out/nextflow_log/$LOG_NAME" || true
  else
    echo "No nextflow log file available."
  fi

  # upload the log file and published files if any
  mkdir -p /home/dnanexus/out/published_files
  find . -type f -newerBt "$BEGIN_TIME" -exec mv {} /home/dnanexus/out/published_files/ \;

  dx-upload-all-outputs --parallel --wait-on-close || echo "No log file or published files has been generated."
  # done
  exit $ret
}

restore_cache_and_history() {
  valid_id_pattern='^\{?[A-Z0-9a-z]{8}-[A-Z0-9a-z]{4}-[A-Z0-9a-z]{4}-[A-Z0-9a-z]{4}-[A-Z0-9a-z]{12}\}?$'
  # if [[ -n "$resume_session" ]]; then
  #   # get session id if specified
  #   PREV_JOB_SESSION_ID=$resume_session
  #   PREV_JOB_DESC=$(dx api system findExecutions \
  #     '{"state":["done","failed"],
  #   "created": {"after": 1.5552e10},
  #   "project":"'$DX_PROJECT_CONTEXT_ID'",
  #   "limit":1,
  #   "includeSubjobs":false,
  #   "describe":{"fields":{"properties":true}},
  #   "properties":{"session_id":"'$PREV_JOB_SESSION_ID'",
  #   "preserve_cache":"true",
  #   "nextflow_executable":"'$EXECUTABLE_NAME'"}}')
  # else
  #   # find the latest job run by applet with the same name
  #   echo "Will try to find the session ID of the latest session run by $EXECUTABLE_NAME."
  #   PREV_JOB_DESC=$(dx api system findExecutions \
  #     '{"state":["done","failed"],
  #   "created": {"after": 1.5552e10},
  #   "project":"'$DX_PROJECT_CONTEXT_ID'",
  #   "limit":1,
  #   "includeSubjobs":false,
  #   "describe":{"fields":{"properties":true}},
  #   "properties":{"session_id":true,
  #   "preserve_cache":"true",
  #   "nextflow_executable":"'$EXECUTABLE_NAME'"}}')

  #   PREV_JOB_SESSION_ID=$(echo "$PREV_JOB_DESC" | jq -r '.results[].describe.properties.session_id')
  # fi

  if [[ $resume == job-* ]]; then
    PREV_JOB_SESSION_ID=$(dx describe "$resume" --json | jq -r '.results[].describe.properties.session_id')
  elif [[ $resume == 'true' || $resume == 'last' ]]; then
    # find the latest job run by applet with the same name
    echo "Will try to find the session ID of the latest session run by $EXECUTABLE_NAME."
    PREV_JOB_SESSION_ID=$(dx api system findExecutions \
      '{"state":["done","failed"],
    "created": {"after": 1.5552e10},
    "project":"'$DX_PROJECT_CONTEXT_ID'",
    "limit":1,
    "includeSubjobs":false,
    "describe":{"fields":{"properties":true}},
    "properties":{"session_id":true,
    "preserve_cache":"true",
    "nextflow_executable":"'$EXECUTABLE_NAME'"}}' | jq -r '.results[].describe.properties.session_id')
  else
    PREV_JOB_SESSION_ID=$resume
  fi

  [[ "$PREV_JOB_SESSION_ID" =~ $valid_id_pattern ]] ||
    dx-jobutil-report-error "The session ID $PREV_JOB_SESSION_ID is not a valid UUID. Please set input 'resume' with a valid session ID and try again."
  # [[ -n $PREV_JOB_DESC ]] ||
  #   dx-jobutil-report-error "Cannot find any matching session ID run by $EXECUTABLE_NAME in $DX_PROJECT_CONTEXT_ID in the past 6 months. Please provides exact resume_session for resume."

  # download cached files from $DX_CACHEDIR/$PREV_JOB_SESSION_ID/
  set +f
  local ret
  ret=$(dx download "$DX_CACHEDIR/$PREV_JOB_SESSION_ID/*" --no-progress -f 2>&1) ||
    {
      if [[ $ret == *"FileNotFoundError"* || $ret == *"ResolutionError"* ]]; then
        dx-jobutil-report-error "No previous execution cache of session $PREV_JOB_SESSION_ID was found as $DX_CACHEDIR/$PREV_JOB_SESSION_ID/."
      else
        dx-jobutil-report-error "$ret"
      fi
    }

  # untar cache.tar, which needs to contain
  # 1. cache folder .nextflow/cache/$PREV_JOB_SESSION_ID
  # 2. history of previous session .nextflow/history
  tar -xf cache.tar
  [[ -n "$(ls -A .nextflow/cache/$PREV_JOB_SESSION_ID)" ]] ||
    dx-jobutil-report-error "Previous execution cache of session $PREV_JOB_SESSION_ID is empty."
  [[ -s ".nextflow/history" ]] ||
    dx-jobutil-report-error "Missing history file in restored cache of previous session $PREV_JOB_SESSION_ID."
  rm cache.tar

  # PREV_JOB_WORKDIR=$(echo "$PREV_JOB_DESC" | jq -r '.results[].describe.properties.workdir')
  # if [[ $PREV_JOB_WORKDIR != dx* ]]; then
  #   # download $DX_CACHEDIR/$PREV_JOB_SESSION_ID/local_working_env and restore ${PREV_JOB_WORKDIR}
  #   # https://jira.internal.dnanexus.com/browse/APPS-1403
  #   ret=$(dx download "$DX_CACHEDIR/$PREV_JOB_SESSION_ID/local_working_env/" --no-progress -rf 2>&1) ||
  #     {
  #       if [[ $ret == *"FileNotFoundError"* || $ret == *"ResolutionError"* ]]; then
  #         dx-jobutil-report-error "No previous local work directory of session $PREV_JOB_SESSION_ID was found at $DX_PROJECT_CONTEXT_ID:/${PREV_JOB_WORKDIR#*/}"
  #       else
  #         dx-jobutil-report-error "$ret"
  #       fi
  #     }
  # # restore previous workdir
  # set +f
  # cp -r local_workdir/* / && rm -r local_workdir
  # set -f
  # fi

  # if previous job is run by local executor, resume the previous workdir
  [[ -e local_working_env.tar ]] &&
    tar -tf local_working_env.tar | xargs -I {} echo /{} >local_working_files.txt &&
    cat local_working_files.txt &&
    tar -xf local_working_env.tar -C / &&
    rm local_working_env.tar &&
    NXF_EXECUTOR='local' ||
    NXF_EXECUTOR='dnanexus'
  # resume succeeded, set session id and add it to job properties
  echo "Will resume from previous session: $PREV_JOB_SESSION_ID"
  NXF_UUID=$PREV_JOB_SESSION_ID
  RESUME_CMD="-resume $NXF_UUID"
  dx tag "$DX_JOB_ID" "resumed"
}

get_runtime_workdir() {
  IFS=" " read -r -a arr <<<"$nextflow_run_opts"
  for i in "${!arr[@]}"; do
    case ${arr[i]} in
    -w=* | -work-dir=*)
      NXF_WORK="${i#*=}"
      break
      ;;
    -w | -work-dir)
      NXF_WORK=${arr[i + 1]}
      break
      ;;
    *) ;;
    esac
  done

  # no user specified workdir, set default
  if [[ -z $NXF_WORK ]]; then
    if [[ $NXF_EXECUTOR == 'local' ]]; then
      NXF_WORK='/work'
    else
      if [[ $preserve_cache == true ]]; then
        NXF_WORK="dx://$DX_CACHEDIR/$NXF_UUID/work/"
      else
        NXF_WORK="dx://$DX_WORKSPACE_ID:/work/"
      fi
    fi
  elif [[ -n $NXF_EXECUTOR ]]; then
    # validate workdir and previous workdir are in the same filesystem
    [[ $NXF_EXECUTOR == 'local' && $NXF_WORK != dx* ]] ||
      [[ $NXF_EXECUTOR == 'dnanexus' && $NXF_WORK == dx* ]] ||
      dx-jobutil-report-error "Resuming from a previous session requires the both resumed and current workdir to be in the same $NXF_EXECUTOR file system. Please provide a compatible workdir with '-w' in nextflow_run_opts."
  else
    [[ $NXF_WORK != dx* ]] && NXF_EXECUTOR='local' || NXF_EXECUTOR='dnanexus'
  fi
}

update_project_history() {
  local ret
  ret=$(dx download "$DX_CACHEDIR/history" --no-progress -f -o .nextflow/prev_history 2>&1 >/dev/null) ||
    {
      if [[ $ret == *"FileNotFoundError"* || $ret == *"ResolutionError"* ]]; then
        echo "No history file found as $DX_CACHEDIR/history"
      else
        dx-jobutil-report-error "$ret"
      fi
    }

  if [[ -s ".nextflow/prev_history" ]]; then
    # merge the nonempty project nextflow history with the current history
    sort -mu ".nextflow/prev_history" ".nextflow/history" -o ".nextflow/latest_history"
    # remove previous project history
    dx rm "$DX_CACHEDIR/history" 2>&1 >/dev/null || true
    rm .nextflow/prev_history
  else
    # there is no project nextflow history
    cp ".nextflow/history" ".nextflow/latest_history"
  fi
  # upload the new project history
  dx upload ".nextflow/latest_history" --path "$DX_CACHEDIR/history" --no-progress --brief --wait -p -r 2>&1 >/dev/null ||
    echo "Failed to update nextflow history in $DX_PROJECT_CONTEXT_ID"
  rm .nextflow/latest_history
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
    env | grep -v DX_SECURITY_CONTEXT | sort
    set -x
  fi

  if [ -n "$docker_creds" ]; then
    dx-registry-login
  fi
  export NXF_DOCKER_LEGACY=true
  #export NXF_DOCKER_CREDS_FILE=$docker_creds_file
  #[[ $scm_file ]] && export NXF_SCM_FILE=$(dx_path $scm_file 'Nextflow CSM file')

  # set default NXF env constants
  export NXF_HOME=/opt/nextflow
  export NXF_ANSI_LOG=false
  export NXF_PLUGINS_DEFAULT=nextaur@1.1.0

  # use /home/dnanexus/nextflow_playground as the temporary nextflow execution folder
  mkdir -p /home/dnanexus/nextflow_playground
  cd /home/dnanexus/nextflow_playground
  mkdir -p .nextflow/cache

  # parse dnanexus-job.json to get job output destination
  DX_JOB_OUTDIR=$(jq -r '[.project, .folder] | join(":")' /home/dnanexus/dnanexus-job.json)
  # initiate log file
  LOG_NAME="nextflow-$DX_JOB_ID.log"

  # add current executable name to job properties:
  EXECUTABLE_NAME=$(jq -r .executableName /home/dnanexus/dnanexus-job.json)
  dx set_properties "$DX_JOB_ID" "nextflow_executable=$EXECUTABLE_NAME"

  DX_CACHEDIR=$DX_PROJECT_CONTEXT_ID:/nextflow_cache_db
  # restore cache and set/create current session id
  RESUME_CMD=""
  if [[ -n $resume ]]; then
    restore_cache_and_history
  else
    NXF_UUID=$(uuidgen)
  fi
  export NXF_UUID
  export NXF_CACHE_MODE=LENIENT
  dx set_properties "$DX_JOB_ID" "session_id=$NXF_UUID"

  get_runtime_workdir
  export NXF_EXECUTOR
  export NXF_WORK
  dx set_properties "$DX_JOB_ID" "workdir=$NXF_WORK"

  # for optional inputs, pass to the run command by using a runtime config
  # TODO: better handling inputs defined in nextflow_schema.json
  RUNTIME_CONFIG_CMD=""
  generate_runtime_config

  # set beginning timestamp
  BEGIN_TIME="$(date +"%Y-%m-%d %H:%M:%S")"

  # execution starts
  NEXTFLOW_CMD="nextflow \
    ${TRACE_CMD} \
    $nextflow_top_level_opts \
    ${RUNTIME_CONFIG_CMD} \
    -log ${LOG_NAME} \
    run @@RESOURCES_SUBPATH@@ \
    @@PROFILE_ARG@@ \
    -name $DX_JOB_ID \
    $RESUME_CMD \
    $nextflow_run_opts \
    $nextflow_pipeline_params \
    @@REQUIRED_RUNTIME_PARAMS@@
      "

  trap on_exit EXIT
  echo "============================================================="
  echo "=== NF projectDir   : @@RESOURCES_SUBPATH@@"
  echo "=== NF session ID   : ${NXF_UUID}"
  echo "=== NF log file     : dx://${DX_JOB_OUTDIR%/}/${LOG_NAME}"
  if [[ $preserve_cache == true ]]; then
    echo "=== NF workDir      : ${NXF_WORK}"
    echo "=== NF cache folder : dx://${DX_CACHEDIR}/${NXF_UUID}/"
  fi
  echo "=== NF command      :" $NEXTFLOW_CMD
  echo "============================================================="

  $NEXTFLOW_CMD &
  NXF_EXEC_PID=$!

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
    echo >&2 "Missing Nextflow .command.log file"
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
  dx cat "${cmd_launcher_file}" >.command.run
  bash .command.run > >(tee .command.log) 2>&1 || true
}
