def get_default_inputs():
    return [
        {
            "name": "nf_run_args_and_pipeline_params",
            "label": "Nextflow Run Arguments and Pipeline Parameters",
            "help": "Additional run arguments and pipeline parameters for Nextflow (i.e. -queue-size).",
            "class": "string",
            "optional": True
        },
        {
            "name": "resume",
            "label": "Resume",
            "help": "Enables resume functionality in Nextflow workflows.",
            "class": "boolean",
            "default": False
        },
        {
            "name": "resume_session",
            "label": "Resume Session",
            "help": "Session or job to be resumed.",
            "class": "string",
            "optional": True
        },
        {
            "name": "nf_advanced_opts",
            "label": "Nextflow Advanced Options",
            "help": "Advanced options for Nextflow (i.e. -quiet).",
            "class": "string",
            "optional": True
        },
        {
            "name": "docker_creds",
            "label": "Docker Credentials",
            "help": "Docker Credentials used to obtain private docker images.",
            "class": "file",
            "optional": True
        },
        {
            "name": "debug",
            "label": "Debug Mode",
            "help": "Shows additional information in Nextflow logs.",
            "class": "boolean",
            "default": False
        },
        {
            "name": "no_future_resume",
            "label": "No Future Resume",
            "help": "Allow saving workspace and cache files to the platform to be used later for resume functionality.",
            "class": "boolean",
            "default": False
        }
    ]
def get_nextflow_dxapp(custom_inputs=[]):
    inputs = custom_inputs + get_default_inputs()
    return {
        "name": "nextflow pipeline",
        "title": "Nextflow Pipeline",
        "summary": "nextflow",
        "dxapi": "1.0.0",
        "version": "1.0.0",
        "inputSpec": inputs,
        "outputSpec": [
        ],
        "runSpec": {
            "interpreter": "bash",
            "execDepends": [
                {
                    "name": "default-jre"
                }
            ],
            "distribution": "Ubuntu",
            "release": "20.04",
            "file": "nextflow.sh",
            "version": "0"
        },
        "regionalOptions": {
            "aws:us-east-1": {
                "systemRequirements": {
                    "*": {
                        "instanceType": "mem1_ssd1_v2_x8"
                    }
                },
                "assetDepends": [
                    {"id": "record-GG0q3X00fVVZQP9G4kFF16zp"},
                    {"id": "record-GG1P0xQ0F9gggxxjKzqV40vg"}
                ]
            }
        },
        "details": {
            "whatsNew": "1.0.0: Initial version"
        },
        "categories": [],
        "access": {
            "network": [
                "*"
            ],
            "project": "CONTRIBUTE",
            "allProjects": "VIEW"
        }
    }

# TODO: change args to individual arguments.
def get_nextflow_src(inputs=[], profile=None):
    run_inputs = ""
    for i in inputs:
        # override arguments that were not given at the runtime
        run_inputs = run_inputs + f'''
        if [ -n "${i['name']}" ]; then
            filtered_inputs="${{filtered_inputs}} --{i['name']}=${i['name']}"
        fi
        '''

    profile_arg = "-profile {}".format(profile) if profile else ""
    return f'''
    #!/usr/bin/env bash

    on_exit() {{
      ret=$?
      # upload log file
      dx upload $LOG_NAME --path $DX_LOG --wait --brief --no-progress --parents || true
      # backup cache
      echo "=== Execution complete — uploading Nextflow cache metadata files"
      dx rm -r "$DX_PROJECT_CONTEXT_ID:/.nextflow/cache/$NXF_UUID/*" 2>&1 >/dev/null || true
      dx upload ".nextflow/cache/$NXF_UUID" --path "$DX_PROJECT_CONTEXT_ID:/.nextflow/cache/$NXF_UUID" --no-progress --brief --wait -p -r || true
      # done
      exit $ret
    }}
    
      dx_path() {{
      local str=${{1#"dx://"}}
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
    }}
    
main() {{
    set -f

    [[ $debug ]] && set -x && env | sort
    [[ $debug ]] && export NXF_DEBUG=2
    
    # dx cat file-GFz1yK00469v4kv49xvvFb3k | tar -zxv -C /
    
    
    if [ -n "$docker_creds" ]; then
        dx download "$docker_creds" -o /home/dnanexus/credentials
        ls /home/dnanexus
        source /.dx.nextflow/resources/usr/local/bin/dx-registry-login
    fi
    
    # nextaur

    LOG_NAME="nextflow-$(date +"%y%m%d-%H%M%S").log"
    DX_WORK=${{work_dir:-$DX_WORKSPACE_ID:/scratch/}}
    DX_LOG=${{log_file:-$DX_PROJECT_CONTEXT_ID:$LOG_NAME}}

    export NXF_WORK=dx://$DX_WORK
    export NXF_HOME=/opt/nextflow
    export NXF_UUID=${{resume_session:-$(uuidgen)}}
    export NXF_IGNORE_RESUME_HISTORY=true
    export NXF_ANSI_LOG=false
    export NXF_EXECUTOR=dnanexus
    export NXF_PLUGINS_DEFAULT=nextaur@1.0.0
    export NXF_DOCKER_LEGACY=true
    #export NXF_DOCKER_CREDS_FILE=$docker_creds_file
    #[[ $scm_file ]] && export NXF_SCM_FILE=$(dx_path $scm_file 'Nextflow CSM file')
    trap on_exit EXIT

    echo "============================================================="
    echo "=== NF work-dir : ${{DX_WORK}}"
    echo "=== NF Resume ID: ${{NXF_UUID}}"
    echo "=== NF log file : ${{DX_LOG}}"
    echo "=== NF cache    : $DX_PROJECT_CONTEXT_ID:/.nextflow/cache/$NXF_UUID"
    echo "============================================================="


    cd /
    filtered_inputs=""
    
    {run_inputs}
    #nextflow -trace nextflow.plugin $nf_advanced_opts -log $LOG_NAME run . {profile_arg} $nf_run_args_and_pipeline_params ${{filtered_inputs}}
    #nextflow -trace nextflow.plugin $nf_advanced_opts -log $LOG_NAME run https://github.com/nextflow-io/hello -name test_hello
    nextflow -trace nextflow.plugin $nf_advanced_opts -log ${{LOG_NAME}}_local run . -name ${{NXF_UUID}}
    set +f
}}


nf_task_exit() {{
  ret=$?
  if [ -f .command.log ]; then
    dx upload .command.log --path "${{cmd_log_file}}" --brief --wait --no-progress || true
  else
    >&2 echo "Missing Nextflow .command.log file"
  fi
  # mark the job as successful in any case, real task
  # error code is managed by nextflow via .exitcode file
  dx-jobutil-add-output exit_code "0" --class=int
}}

nf_task_entry() {{
  # enable debugging mode
  [[ $NXF_DEBUG ]] && set -x
  # capture the exit code
  trap nf_task_exit EXIT
  # run the task
  dx cat "${{cmd_launcher_file}}" > .command.run
  bash .command.run > >(tee .command.log) 2>&1 || true
}}

    '''

# iterate through inputs of dxapp.json and add them here?
# put them in params file?