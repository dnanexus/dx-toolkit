#!/usr/bin/env bash

# =========================================================
# Script containing
# 1. Nextflow head (orchestrator) job entry, exit functions
# 2. Nextflow task subjob entry, exit functions
# 3. Helper functions
# They are currently in one file because supporting use
# of the resources folder for Nextflow Pipeline Applets
# would require more changes; currently the pipeline
# source folder completely replaces the resources folder.
# =========================================================

set -f

AWS_ENV="$HOME/.dx-aws.env"
USING_S3_WORKDIR=false
LOGS_DIR="$HOME/.log/"

# How long to let a subjob with error keep running for Nextflow to handle it
# before we end the DX job, in seconds
MAX_WAIT_AFTER_JOB_ERROR=240
# How often to check when waiting for a subjob with error, in seconds
WAIT_INTERVAL=15

# =========================================================
# 1. Nextflow head (orchestrator) job
# =========================================================

main() {
  if [[ $debug == true ]]; then
    export NXF_DEBUG=2
    TRACE_CMD="-trace nextflow.plugin"
    env | grep -v DX_SECURITY_CONTEXT | sort
    set -x
  fi

  detect_nextaur_plugin_version

  # unset properties
  cloned_job_properties=$(dx describe "$DX_JOB_ID" --json | jq -r '.properties | to_entries[] | select(.key | startswith("nextflow")) | .key')
  [[ -z $cloned_job_properties ]] || dx unset_properties "$DX_JOB_ID" $cloned_job_properties

  # check if all run opts provided by user are supported
  validate_run_opts

  # set default NXF env constants
  export NXF_HOME=/opt/nextflow
  export NXF_ANSI_LOG=false
  export NXF_PLUGINS_DEFAULT=nextaur@$NXF_PLUGINS_VERSION
  export NXF_EXECUTOR='dnanexus'

  # use /home/dnanexus/nextflow_execution as the temporary nextflow execution folder
  mkdir -p /home/dnanexus/nextflow_execution
  cd /home/dnanexus/nextflow_execution

  # make runtime parameter arguments from applet inputs
  set +x
  applet_runtime_inputs=()
  @@APPLET_RUNTIME_PARAMS@@
  if [[ $debug == true ]]; then
    if [[ "${#applet_runtime_inputs}" -gt 0 ]]; then
      echo "Will specify the following runtime parameters:"
      printf "[%s] " "${applet_runtime_inputs[@]}"
      echo
    else
      echo "No runtime parameter is specified. Will use the default values."
    fi
    set -x
  fi

  # get job output destination
  DX_JOB_OUTDIR=$(jq -r '[.project, .folder] | join(":")' /home/dnanexus/dnanexus-job.json)
  # initiate log file
  LOG_NAME="nextflow-$DX_JOB_ID.log"

  # get current executable name
  EXECUTABLE_NAME=$(jq -r .executableName /home/dnanexus/dnanexus-job.json)

  # download default applet file type inputs
  dx-download-all-inputs --parallel @@EXCLUDE_INPUT_DOWNLOAD@@ 2>/dev/null 1>&2
  RUNTIME_CONFIG_CMD=''
  RUNTIME_PARAMS_FILE=''
  [[ -d "$HOME/in/nextflow_soft_confs/" ]] && RUNTIME_CONFIG_CMD=$(find "$HOME"/in/nextflow_soft_confs -name "*.config" -type f -printf "-c %p ")
  [[ -d "$HOME/in/nextflow_params_file/" ]] && RUNTIME_PARAMS_FILE=$(find "$HOME"/in/nextflow_params_file -type f -printf "-params-file %p ")
  if [[ -d "$HOME/in/docker_creds" ]]; then
    CREDENTIALS=$(find "$HOME/in/docker_creds" -type f | head -1)
    [[ -s $CREDENTIALS ]] && docker_registry_login || echo "no docker credential available"
    dx upload "$CREDENTIALS" --path "$DX_WORKSPACE_ID:/dx_docker_creds" --brief --wait --no-progress || true
  fi

  # First Nextflow run, only to parse & save config required for AWS login
  local env_job_suffix='-GET-ENV'
  declare -a NEXTFLOW_CMD_ENV="(nextflow \
    ${TRACE_CMD} \
    $nextflow_top_level_opts \
    ${RUNTIME_CONFIG_CMD} \
    -log ${LOGS_DIR}${LOG_NAME}${env_job_suffix} \
    run @@RESOURCES_SUBPATH@@ \
    $profile_arg \
    -name ${DX_JOB_ID}${env_job_suffix} \
    $nextflow_run_opts \
    $RUNTIME_PARAMS_FILE \
    $nextflow_pipeline_params)"

  NEXTFLOW_CMD_ENV+=("${applet_runtime_inputs[@]}")
  
  AWS_ENV="$HOME/.dx-aws.env"

  get_nextflow_environment "${NEXTFLOW_CMD_ENV[@]}"
  dx download "$DX_WORKSPACE_ID:/.dx-aws.env" -o $AWS_ENV -f --no-progress 2>/dev/null || true

  # Login to AWS, if configured
  aws_login
  aws_relogin_loop & AWS_RELOGIN_PID=$!

  set_vars_session_and_cache
  
  if [[ $preserve_cache == true ]]; then
    set_job_properties_cache
    check_cache_db_storage_limit
    if [[ -n $resume ]]; then
      check_no_concurrent_job_same_cache
    fi
  fi

  RESUME_CMD=""
  if [[ -n $resume ]]; then
    restore_cache_and_set_resume_cmd
  fi

  # Set Nextflow workdir based on S3 workdir / preserve_cache options
  setup_workdir

  # set beginning timestamp
  BEGIN_TIME="$(date +"%Y-%m-%d %H:%M:%S")"

  # Start Nextflow run
  declare -a NEXTFLOW_CMD="(nextflow \
    ${TRACE_CMD} \
    $nextflow_top_level_opts \
    ${RUNTIME_CONFIG_CMD} \
    -log ${LOGS_DIR}${LOG_NAME} \
    run @@RESOURCES_SUBPATH@@ \
    $profile_arg \
    -name ${DX_JOB_ID} \
    $RESUME_CMD \
    $nextflow_run_opts \
    $RUNTIME_PARAMS_FILE \
    $nextflow_pipeline_params)"

  NEXTFLOW_CMD+=("${applet_runtime_inputs[@]}")

  trap on_exit EXIT
  log_context_info

  "${NEXTFLOW_CMD[@]}" & NXF_EXEC_PID=$!
  set +x
  if [[ $debug == true ]] ; then
    # Forward Nextflow log to job log
    touch "${LOGS_DIR}${LOG_NAME}"
    tail --follow -n 0 "${LOGS_DIR}${LOG_NAME}" -s 60 >&2 & LOG_MONITOR_PID=$!
    disown $LOG_MONITOR_PID
    set -x
  fi

  # After Nextflow run
  wait $NXF_EXEC_PID
  ret=$?

  kill "$AWS_RELOGIN_PID"
  exit $ret
}

on_exit() {
  ret=$?

  properties=$(dx describe ${DX_JOB_ID} --json 2>/dev/null | jq -r ".properties")
  if [[ $properties != "null" ]]; then
    if [[ $(jq .nextflow_errorStrategy <<<${properties} -r) == "ignore" ]]; then
      ignored_subjobs=$(jq .nextflow_errored_subjobs <<<${properties} -r)
      if [[ ${ignored_subjobs} != "null" ]]; then
        echo "Subjob(s) ${ignored_subjobs} ran into Nextflow process errors. \"ignore\" errorStrategy was applied."
      fi
    fi
  fi
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

  if [[ $preserve_cache == true ]]; then
    echo "=== Execution completed — caching current session to $DX_CACHEDIR/$NXF_UUID"
    upload_session_cache_file
  else
    echo "=== Execution completed — cache and working files will not be resumable"
  fi

  # remove .nextflow from the current folder /home/dnanexus/nextflow_execution
  rm -rf .nextflow

  if [[ -s "${LOGS_DIR}${LOG_NAME}" ]]; then
    echo "=== Execution completed — upload nextflow log to job output destination ${DX_JOB_OUTDIR%/}/"
    NEXFLOW_LOG_ID=$(dx upload "${LOGS_DIR}${LOG_NAME}" --path "${DX_JOB_OUTDIR%/}/${LOG_NAME}" --wait --brief --no-progress --parents) &&
      echo "Upload nextflow log as file: $NEXFLOW_LOG_ID" ||
      echo "Failed to upload log file of current session $NXF_UUID"
  else
    echo "=== Execution completed — no nextflow log file available."
  fi

  if [[ $ret -ne 0 ]]; then
    echo "=== Execution failed — skip uploading published files to job output destination ${DX_JOB_OUTDIR%/}/"

  else
    echo "=== Execution succeeded — upload published files to job output destination ${DX_JOB_OUTDIR%/}/"
    mkdir -p /home/dnanexus/out/published_files
    find . -type f -newermt "$BEGIN_TIME" -exec cp --parents {} /home/dnanexus/out/published_files/ \; -delete
    dx-upload-all-outputs --parallel --wait-on-close || echo "No published files has been generated."
  fi
  exit $ret
}

# =========================================================
# 2. Nextflow task subjobs
# =========================================================

nf_task_entry() {
  CREDENTIALS="$HOME/docker_creds"
  dx download "$DX_WORKSPACE_ID:/dx_docker_creds" -o $CREDENTIALS --recursive --no-progress -f 2>/dev/null || true
  [[ -f $CREDENTIALS ]] && docker_registry_login  || echo "no docker credential available"
  dx download "$DX_WORKSPACE_ID:/.dx-aws.env" -o $AWS_ENV -f --no-progress 2>/dev/null || true
  aws_login
  aws_relogin_loop & AWS_RELOGIN_PID=$!
  # capture the exit code
  trap nf_task_exit EXIT

  download_cmd_launcher_file

  # enable debugging mode
  [[ $NXF_DEBUG ]] && set -x

  # run the task
  set +e
  bash .command.run > >(tee .command.log) 2>&1
  export exit_code=$?
  kill "$AWS_RELOGIN_PID"
  dx set_properties ${DX_JOB_ID} nextflow_exit_code=$exit_code
  set -e
}

nf_task_exit() {
  if [ -f .command.log ]; then
    if [[ $USING_S3_WORKDIR == true ]]; then
      aws s3 cp .command.log "${cmd_log_file}"
    else
      dx upload .command.log --path "${cmd_log_file}" --brief --wait --no-progress || true
    fi
  else
    >&2 echo "Missing Nextflow .command.log file"
  fi

  # exit_code should already be set in nf_task_entry(); default just in case
  # This is just for including as DX output; Nextflow internally uses .exitcode file
  if [ -z ${exit_code} ]; then export exit_code=0; fi

  if [ "$exit_code" -ne 0 ]; then wait_for_terminate_or_retry; fi

  # There are cases where the Nextflow task had an error but we don't want to fail the whole
  # DX job exec tree, e.g. because the error strategy should continue,
  # so we let the DX job succeed but this output records Nextflow's exit code
  dx-jobutil-add-output exit_code $exit_code --class=int
}

# =========================================================
# Helpers: Docker login
# =========================================================

# Logs the user to the docker registry.
# Uses docker credentials that have to be in $CREDENTIALS location.
# Format of the file:
#      {
#          docker_registry: {
#              "registry": "<Docker registry name, e.g. quay.io or docker.io>",
#              "username": "<registry login name>",
#              "organization": "<(optional, default value equals username) organization as defined by DockerHub or Quay.io>",
#              "token": "<API token>"
#          }
#      }
docker_registry_login() {
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
      echo "    \"docker_registry\": {"
      echo "        \"registry\": \"<Docker registry name, e.g. quay.io or docker.io>\"",
      echo "        \"username\": \"<registry login name>\"",
      echo "        \"organization\": \"<(optional, default value equals username) organization as defined by DockerHub or Quay.io>\"",
      echo "        \"token\": \"<API token>\""
      echo "    }"
      echo "}"
      exit 1
  fi

  jq '.docker_registry.token' "$CREDENTIALS" -r | docker login $REGISTRY --username $REGISTRY_USERNAME --password-stdin 2> >(grep -v -E "WARNING! Your password will be stored unencrypted in |Configure a credential helper to remove this warning. See|https://docs.docker.com/engine/reference/commandline/login/#credentials-store")
  if [ ! $? -eq 0 ]; then
      echo "Docker authentication failed, please check if the docker credentials file is correct." 1>&2
      exit 2
  fi
}

# =========================================================
# Helpers: AWS login, job id tokens
# =========================================================

aws_login() {
  if [ -f "$AWS_ENV" ]; then
    source $AWS_ENV
    detect_if_using_s3_workdir
  
    # aws env file example values:
    # "iamRoleArnToAssume", "jobTokenAudience", "jobTokenSubjectClaims", "awsRegion"
    roleSessionName="dnanexus_${DX_JOB_ID}"
    job_id_token=$(dx-jobutil-get-identity-token --aud ${jobTokenAudience} --subject_claims ${jobTokenSubjectClaims})
    output=$(aws sts assume-role-with-web-identity --role-arn $iamRoleArnToAssume --role-session-name $roleSessionName --web-identity-token $job_id_token --duration-seconds 3600)
    mkdir -p /home/dnanexus/.aws/

    cat <<EOF > /home/dnanexus/.aws/credentials
[default]
aws_access_key_id = $(echo "$output" | jq -r '.Credentials.AccessKeyId')
aws_secret_access_key = $(echo "$output" | jq -r '.Credentials.SecretAccessKey')
aws_session_token = $(echo "$output" | jq -r '.Credentials.SessionToken')
EOF
    cat <<EOF > /home/dnanexus/.aws/config
[default]
region = $awsRegion
EOF
    echo "Successfully authenticated to AWS - $(aws sts get-caller-identity)"
  fi
}

aws_relogin_loop() {
  while true; do
    sleep 3300 # relogin every 55 minutes, first login is done separately, so we wait before the login
      if [ -f "$AWS_ENV" ]; then
        aws_login
      fi
  done
}

# =========================================================
# Helpers: workdir configuration
# =========================================================

detect_if_using_s3_workdir() {
  if [[ -f "$AWS_ENV" ]]; then
    source $AWS_ENV
  fi

  if [[ -n $workdir && $workdir != "null" ]]; then
    USING_S3_WORKDIR=true
  fi
}

setup_workdir() {
  if [[ -f "$AWS_ENV" ]]; then
    source $AWS_ENV
  fi

  if [[ -n $workdir && $workdir != "null" ]]; then
    # S3 work dir was specified, use that
    NXF_WORK="${workdir}/${NXF_UUID}/work"
    USING_S3_WORKDIR=true
  elif [[ $preserve_cache == true ]]; then
    # Work dir on platform and using cache, use project
    [[ -n $resume ]] || dx mkdir -p $DX_CACHEDIR/$NXF_UUID/work/
    NXF_WORK="dx://$DX_CACHEDIR/$NXF_UUID/work/"
  else
    # Work dir on platform and not using cache, use workspace
    NXF_WORK="dx://$DX_WORKSPACE_ID:/work/"
  fi

  export NXF_WORK
}

# =========================================================
# Helpers: basic run
# =========================================================

validate_run_opts() {
  profile_arg="@@PROFILE_ARG@@"

  IFS=" " read -r -a opts <<<"$nextflow_run_opts"
  for opt in "${opts[@]}"; do
    case $opt in
    -w=* | -work-dir=* | -w | -work-dir)
      dx-jobutil-report-error "Please remove workDir specification (-w|-work-dir path) in nextflow_run_opts. For Nextflow runs on DNAnexus, the workdir will be located at 1) In the workspace container-xxx 2) In project-yyy:/.nextflow_cache_db if preserve_cache=true, or 3) on S3, if specified."
      ;;
    -profile | -profile=*)
      if [ -n "$profile_arg" ]; then
        echo "Profile was given in run options... overriding the default profile ($profile_arg)"
        profile_arg=""
      fi
      ;;
    *) ;;
    esac
  done
}

detect_nextaur_plugin_version() {
  executable=$(cat dnanexus-executable.json | jq -r .id )
  bundled_dependency=$(dx describe ${executable} --json | jq -r '.runSpec.bundledDepends[] | select(.name=="nextaur.tar.gz") | .id."$dnanexus_link"')
  asset_dependency=$(dx describe ${bundled_dependency} --json | jq -r .properties.AssetBundle)
  export NXF_PLUGINS_VERSION=$(dx describe ${asset_dependency} --json | jq -r .properties.version)
}

get_nextflow_environment() {
  NEXTFLOW_CMD_ENV=("$@")
  set +e
  ENV_OUTPUT=$("${NEXTFLOW_CMD_ENV[@]}" 2>&1)
  ENV_EXIT=$?
  set -e

  if [ $ENV_EXIT -ne 0 ]; then
      echo "$ENV_OUTPUT"
      return $ENV_EXIT
  else
      echo "$ENV_OUTPUT" > "/home/dnanexus/.dx_get_env.log"
  fi
}

log_context_info() {
  echo "============================================================="
  echo "=== NF projectDir   : @@RESOURCES_SUBPATH@@"
  echo "=== NF session ID   : ${NXF_UUID}"
  echo "=== NF log file     : dx://${DX_JOB_OUTDIR%/}/${LOG_NAME}"
  echo "=== NF workdir      : ${NXF_WORK}"
  if [[ $preserve_cache == true ]]; then
    echo "=== NF cache folder : dx://${DX_CACHEDIR}/${NXF_UUID}/"
  fi
  echo "=== NF command      :" "${NEXTFLOW_CMD[@]}"
  echo "=== Built with dxpy : @@DXPY_BUILD_VERSION@@"
  echo "============================================================="
}

wait_for_terminate_or_retry() {
  terminate_record=$(dx find data --name $DX_JOB_ID --path $DX_WORKSPACE_ID:/.TERMINATE --brief | head -n 1)
  if [ -n "${terminate_record}" ]; then
    echo "Subjob exited with non-zero exit_code and the errorStrategy is terminate."
    echo "Waiting for the head job to kill the job tree..."
    sleep $MAX_WAIT_AFTER_JOB_ERROR
    echo "This subjob was not killed in time, exiting to prevent excessive waiting."
    exit
  fi

  retry_record=$(dx find data --name $DX_JOB_ID --path $DX_WORKSPACE_ID:/.RETRY --brief | head -n 1)
  if [ -n "${retry_record}" ]; then
    wait_period=0
    echo "Subjob exited with non-zero exit_code and the errorStrategy is retry."
    echo "Waiting for the head job to kill the job tree or for instruction to continue..."
    while true
    do
        errorStrategy_set=$(dx describe $DX_JOB_ID --json | jq .properties.nextflow_errorStrategy -r)
        if [ "$errorStrategy_set" = "retry" ]; then
          break
        fi
        wait_period=$(($wait_period+$WAIT_INTERVAL))
        if [ $wait_period -ge $MAX_WAIT_AFTER_JOB_ERROR ];then
          echo "This subjob was not killed in time, exiting to prevent excessive waiting."
          break
        else
          echo "No instruction to continue was given. Waiting for ${WAIT_INTERVAL} seconds"
          sleep $WAIT_INTERVAL
        fi
    done
  fi
}

download_cmd_launcher_file() {
  if [[ $USING_S3_WORKDIR == true ]]; then
    aws s3 cp "${cmd_launcher_file}" .command.run.tmp
  else
    dx download "${cmd_launcher_file}" --output .command.run.tmp
  fi

  # remove the line in .command.run to disable printing env vars if debugging is on
  cat .command.run.tmp | sed 's/\[\[ $NXF_DEBUG > 0 ]] && nxf_env//' > .command.run
}

# =========================================================
# Helpers: run with preserve cache, resume
# =========================================================

set_vars_session_and_cache() {
  # Path in project to store cached sessions
  export DX_CACHEDIR="${DX_PROJECT_CONTEXT_ID}:/.nextflow_cache_db"

  # Using the lenient mode to caching makes it possible to reuse working files for resume on the platform
  export NXF_CACHE_MODE=LENIENT

  # If resuming session, use resume id; otherwise create id for this session
  if [[ -n $resume ]]; then
    get_resume_session_id
  else
    NXF_UUID=$(uuidgen)
  fi
  export NXF_UUID
}

get_resume_session_id() {
  if [[ $resume == 'true' || $resume == 'last' ]]; then
    # find the latest job run by applet with the same name
    echo "Will try to find the session ID of the latest session run by $EXECUTABLE_NAME."
    PREV_JOB_SESSION_ID=$(
      dx api system findExecutions \
        '{"state":["done","failed"],
          "created": {"after":'$((($(date +%s) - 6 * 60 * 60 * 24 * 30) * 1000))'},
          "project":"'$DX_PROJECT_CONTEXT_ID'",
          "limit":1,
          "includeSubjobs":false,
          "describe":{"fields":{"properties":true}},
          "properties":{
            "nextflow_session_id":true,
            "nextflow_preserve_cache":"true",
            "nextflow_executable":"'$EXECUTABLE_NAME'"}}' 2>/dev/null |
        jq -r '.results[].describe.properties.nextflow_session_id'
    )

    [[ -n $PREV_JOB_SESSION_ID ]] ||
      dx-jobutil-report-error "Cannot find any jobs within the last 6 months to resume from. Please provide the exact sessionID for \"resume\" value or run without resume."
  else
    PREV_JOB_SESSION_ID=$resume
  fi

  valid_id_pattern='^\{?[A-Z0-9a-z]{8}-[A-Z0-9a-z]{4}-[A-Z0-9a-z]{4}-[A-Z0-9a-z]{4}-[A-Z0-9a-z]{12}\}?$'
  [[ "$PREV_JOB_SESSION_ID" =~ $valid_id_pattern ]] ||
    dx-jobutil-report-error "Invalid resume value. Please provide either \"true\", \"last\", or \"sessionID\". 
    If a sessionID was provided, Nextflow cached content could not be found under $DX_CACHEDIR/$PREV_JOB_SESSION_ID/. 
    Please provide the exact sessionID for \"resume\" or run without resume."

  NXF_UUID=$PREV_JOB_SESSION_ID
}

set_job_properties_cache() {
  # Set properties on job, which can be used to look up cached job later
  dx set_properties "$DX_JOB_ID" \
    nextflow_executable="$EXECUTABLE_NAME" \
    nextflow_session_id="$NXF_UUID" \
    nextflow_preserve_cache="$preserve_cache"
}

check_cache_db_storage_limit() {
  # Enforce a limit on cached session workdirs stored in the DNAnexus project
  # Removal must be manual, because applet can only upload, not delete project files
  # Limit does not apply when the workdir is external (e.g. S3)

  MAX_CACHE_STORAGE=20
  existing_cache=$(dx ls $DX_CACHEDIR --folders 2>/dev/null | wc -l)
  [[ $existing_cache -lt $MAX_CACHE_STORAGE || $USING_S3_WORKDIR == true ]] ||
    dx-jobutil-report-error "The limit for preserved sessions in the project is $MAX_CACHE_STORAGE. Please remove folders from $DX_CACHEDIR to be under the limit, run without preserve_cache=true, or use S3 as workdir."
}

check_no_concurrent_job_same_cache() {
  # Do not allow more than 1 concurrent run with the same session id,
  # to prevent conflicting workdir contents

  FIRST_RESUMED_JOB=$(
    dx api system findExecutions \
      '{"state":["idle", "waiting_on_input", "runnable", "running", "debug_hold", "waiting_on_output", "restartable", "terminating"],
        "project":"'$DX_PROJECT_CONTEXT_ID'",
        "includeSubjobs":false,
        "properties":{
          "nextflow_session_id":"'$NXF_UUID'",
          "nextflow_preserve_cache":"true",
          "nextflow_executable":"'$EXECUTABLE_NAME'"}}' 2>/dev/null |
      jq -r '.results[-1].id // empty'
  )

  [[ -n $FIRST_RESUMED_JOB && $DX_JOB_ID == $FIRST_RESUMED_JOB ]] ||
    dx-jobutil-report-error "There is at least one other non-terminal state job with the same sessionID $NXF_UUID. 
    Please wait until all other jobs sharing the same sessionID to enter their terminal state and rerun, 
    or run without preserve_cache set to true."
}

restore_cache_and_set_resume_cmd() {
  # download latest cache.tar from $DX_CACHEDIR/$PREV_JOB_SESSION_ID/
  PREV_JOB_CACHE_FILE=$(
    dx api system findDataObjects \
      '{"visibility": "either", 
        "name":"cache.tar",
        "scope": {
          "project": "'$DX_PROJECT_CONTEXT_ID'", 
          "folder": "/.nextflow_cache_db/'$NXF_UUID'", 
          "recurse": false}, 
        "describe": true}' 2>/dev/null |
      jq -r '.results | sort_by(.describe.created)[-1] | .id // empty'
  )

  [[ -n $PREV_JOB_CACHE_FILE ]] ||
    dx-jobutil-report-error "Cannot find any $DX_CACHEDIR/$PREV_JOB_SESSION_ID/cache.tar. Please provide a valid sessionID."

  local ret
  ret=$(dx download $PREV_JOB_CACHE_FILE --no-progress -f -o cache.tar 2>&1) ||
    {
      if [[ $ret == *"FileNotFoundError"* || $ret == *"ResolutionError"* ]]; then
        dx-jobutil-report-error "Nextflow cached content cannot be downloaded from $DX_CACHEDIR/$PREV_JOB_SESSION_ID/cache.tar. Please provide the exact sessionID for \"resume\" value or run without resume."
      else
        dx-jobutil-report-error "$ret"
      fi
    }

  # untar cache.tar, which needs to contain
  # 1. cache folder .nextflow/cache/$PREV_JOB_SESSION_ID
  tar -xf cache.tar
  [[ -n "$(ls -A .nextflow/cache/$PREV_JOB_SESSION_ID)" ]] ||
    dx-jobutil-report-error "Previous execution cache of session $PREV_JOB_SESSION_ID is empty."
  rm cache.tar

  # resume succeeded, set session id and add it to job properties
  echo "Will resume from previous session: $PREV_JOB_SESSION_ID"
  RESUME_CMD="-resume $NXF_UUID"
  dx tag "$DX_JOB_ID" "resumed"
}

upload_session_cache_file() {
  # wrap cache folder and upload cache.tar
  if [[ -n "$(ls -A .nextflow)" ]]; then
    tar -cf cache.tar .nextflow

    CACHE_ID=$(dx upload "cache.tar" --path "$DX_CACHEDIR/$NXF_UUID/cache.tar" --no-progress --brief --wait -p -r) &&
      echo "Upload cache of current session as file: $CACHE_ID" &&
      rm -f cache.tar ||
      echo "Failed to upload cache of current session $NXF_UUID"
  else
    echo "No cache is generated from this execution. Skip uploading cache."
  fi
}
