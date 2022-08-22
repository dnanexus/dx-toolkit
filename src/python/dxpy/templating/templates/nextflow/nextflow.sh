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
        ls /home/dnanexus
        source /.dx.nextflow/resources/usr/local/bin/dx-registry-login
    fi
    curl -s https://get.nextflow.io | bash
    
    mv nextflow /usr/bin


    cd /
    filtered_inputs=""
    
    @@RUN_INPUTS@@
    nextflow run @@PROFILE_ARG@@ / $nf_run_args_and_pipeline_params ${filtered_inputs}
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