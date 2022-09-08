#!/usr/bin/env bash

on_exit() {
  ret=$?
  # upload log file
  dx upload $LOG_NAME --path $DX_LOG --wait --brief --no-progress --parents || true
  # backup cache
  echo "=== Execution complete — uploading Nextflow cache metadata files"
  dx rm -r "$DX_PROJECT_CONTEXT_ID:/.nextflow/cache/$NXF_UUID/*" 2>&1 >/dev/null || true
  dx upload ".nextflow/cache/$NXF_UUID" --path "$DX_PROJECT_CONTEXT_ID:/.nextflow/cache/$NXF_UUID" --no-progress --brief --wait -p -r || true
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
    set -f

    [[ $debug ]] && set -x && env | sort
    [[ $debug ]] && export NXF_DEBUG=2
    
    if [ -n "$docker_creds" ]; then
        dx download "$docker_creds" -o /home/dnanexus/credentials
        source /.dx.nextflow/resources/usr/local/bin/dx-registry-login
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

    mkdir -p /home/dnanexus/nxf_playground
    cd /home/dnanexus/nxf_playground
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
  # capture the exit code
  trap nf_task_exit EXIT
  # run the task
  dx cat "${cmd_launcher_file}" > .command.run
  bash .command.run > >(tee .command.log) 2>&1 || true
}
