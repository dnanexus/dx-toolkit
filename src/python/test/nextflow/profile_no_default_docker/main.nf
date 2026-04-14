#!/usr/bin/env nextflow
nextflow.enable.dsl=2

// Minimal pipeline used for testing docker.enabled = true injection.
// Uses process.container so Docker must be active to run — crashes otherwise.
process sayHello {
    output:
        stdout

    script:
        """
        echo 'docker is working!'
        """
}

workflow {
    sayHello | view
}
