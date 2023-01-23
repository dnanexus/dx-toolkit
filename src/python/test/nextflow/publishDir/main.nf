#!/usr/bin/env nextflow
nextflow.enable.dsl=2

process catFile {
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


process listFolder {
    debug true
    errorStrategy 'ignore'
    publishDir "$params.outdir/ls_output", mode: 'copy'
    input:
    path 'in_folder'

    output:
      path 'ls_folder.txt'
    script:
    """
    ls in_folder > ls_folder.txt
    """
}

workflow {
  catFile(params.inFile)| view
  listFolder(params.inFolder) | view
}