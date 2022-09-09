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

on_exit() {
  ret=$?
  # backup cache
  echo "=== Execution complete — uploading Nextflow cache metadata files"
  dx rm -r "$DX_PROJECT_CONTEXT_ID:/.nextflow/cache/$NXF_UUID/*" 2>&1 >/dev/null || true

  if [[ -d .nextflow/cache/$NXF_UUID ]]; then
    dx upload ".nextflow/cache/$NXF_UUID" --path "$DX_PROJECT_CONTEXT_ID:/.nextflow/cache/$NXF_UUID" --no-progress --brief --wait -p -r || true
  else
    echo "No nextflow cache has been generated."
  fi

  # parse dnanexus-job.json to get job output destination
  OUT_PROJECT=$(jq -r .project /home/dnanexus/dnanexus-job.json)
  OUT_FOLDER=$(jq -r .folder /home/dnanexus/dnanexus-job.json)
  OUTDIR="$OUT_PROJECT:${OUT_FOLDER#/}"

  # publish output files
  rm -rf .nextflow
  if [[ -s $LOG_NAME ]]; then
    mkdir ../nextflow_log
    mv $LOG_NAME ../nextflow_log/$LOG_NAME || true
  else
    echo "No nextflow log file available."
  fi
  
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
    if $debug ; then
      set -x && env | sort
      export NXF_DEBUG=2
    fi
    
    if [ -n "$docker_creds" ]; then
        dx-registry-login
    fi

    LOG_NAME="nextflow-$(date +"%y%m%d-%H%M%S").log"
    DX_WORK=${work_dir:-$DX_WORKSPACE_ID:/scratch/}
    DX_LOG=${log_file:-$DX_PROJECT_CONTEXT_ID:$LOG_NAME}
    export NXF_WORK=dx://$DX_WORK
    export NXF_HOME=/opt/nextflow
    export NXF_UUID=$(uuidgen)
    export NXF_ANSI_LOG=false
    export NXF_EXECUTOR=dnanexus
    export NXF_PLUGINS_DEFAULT=nextaur@1.0.0
    export NXF_DOCKER_LEGACY=true
    #export NXF_DOCKER_CREDS_FILE=$docker_creds_file
    #[[ $scm_file ]] && export NXF_SCM_FILE=$(dx_path $scm_file 'Nextflow CSM file')
    trap on_exit EXIT
    echo "============================================================="
    echo "=== NF work-dir : ${DX_WORK}"
    echo "=== NF log file : ${DX_LOG}"
    echo "=== NF cache    : $DX_PROJECT_CONTEXT_ID:/.nextflow/cache/$NXF_UUID"
    echo "============================================================="

    filtered_inputs=()

    @@RUN_INPUTS@@

    mkdir -p /home/dnanexus/out/output_files
    cd /home/dnanexus/out/output_files
    nextflow -trace nextflow.plugin $nf_advanced_opts -log ${LOG_NAME} run @@RESOURCES_SUBPATH@@ @@PROFILE_ARG@@ -name run-${NXF_UUID} $nf_run_args_and_pipeline_params "${filtered_inputs[@]}"
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
