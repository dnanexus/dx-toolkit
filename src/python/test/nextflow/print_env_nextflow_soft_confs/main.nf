#!/usr/bin/env nextflow
nextflow.enable.dsl=2
process printEnv {
  output:
    stdout
  script:
    """
      echo The env var ALPHA is: $ALPHA
      echo The env var BETA is: $BETA
    """
}

workflow {
  printEnv | view
}
