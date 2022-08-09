def get_docker_login():

    return

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
            "name": "secret_directive_file",
            "label": "Secret Directive File",
            "help": "Adds the built-in Nextflow support for pipeline secrets to allow users to handle and manage sensitive information for pipeline execution in a safe manner.",
            "class": "file",
            "optional": True
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
                }
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


def get_nextflow_src(inputs, args):
    run_inputs = ""
    for i in inputs:
        # we cannot override arguments that were not given at the runtime
        run_inputs = run_inputs + f'''
        if [ -n "${i['name']}" ]; then
            filtered_inputs="${{filtered_inputs}} --{i['name']}=${i['name']}"
        fi
        '''

    profile_arg = "-profile {}".format(args.profile) if args.profile else ""
    return f'''
    #!/usr/bin/env bash
    
    curl -s https://get.nextflow.io | bash
    mv nextflow /usr/bin
    filtered_inputs=""
    
    if "$debug" ; then
        export NXF_DEBUG=2
    fi
    {run_inputs}
    echo $filtered_inputs
    nextflow run {profile_arg} / $nf_run_args_and_pipeline_params ${{filtered_inputs}}
    '''

# iterate through inputs of dxapp.json and add them here?
# put them in params file?