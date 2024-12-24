#!/usr/bin/env nextflow
nextflow.enable.dsl=2
process sayHello {
  errorStrategy 'retry'
  maxRetries 2
  maxErrors 20
  input:
    val x
  output:
    stdout
  script:
    """
    ecsho '$x world!'
    """
}

workflow {
  Channel.of('Bonjour', 'Ciao', 'Hello', 'Hola') | sayHello | view
}
