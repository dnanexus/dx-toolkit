#!/usr/bin/env nextflow
nextflow.enable.dsl=2

// Minimal pipeline used for testing docker.enabled = true injection.
// Uses process.container so Docker must be active to run — crashes otherwise.
process sayHello {
    output:
        stdout

    script:
        """
        # /.dockerenv is created by the Docker runtime inside every container.
        # Its presence proves the process is actually running inside Docker,
        # not falling back to native execution on the bare worker host.
        [ -f /.dockerenv ] && echo 'docker is working!' || { echo 'NOT running in Docker — injection may have failed'; exit 1; }
        """
}

workflow {
    sayHello | view
}
