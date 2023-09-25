#!/usr/bin/env nextflow
nextflow.enable.dsl=2
params.ALPHA = 'default alpha'
params.BETA = 'default beta'
process printParams {
  input:
    val alpha
    val beta
  output:
    stdout
  script:
    """
      echo The parameter ALPHA is: $alpha
      echo The parameter BETA is: $beta
    """
}

workflow {
    printParams(params.ALPHA, params.BETA) | view
}
