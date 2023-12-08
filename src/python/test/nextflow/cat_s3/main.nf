#!/usr/bin/env nextflow
nextflow.enable.dsl=2

process cat_s3 {
    debug true
    errorStrategy 'ignore'
    publishDir "$params.outdir/cat_output", mode: 'copy'
    input:
    path 'in_file'

    output:
      path 'cat_file.txt'
    script:
    """
    cat in_file > cat_file.txt
    """
}

workflow {
  cat_s3(params.in_file)| view
}