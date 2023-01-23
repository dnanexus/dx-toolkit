#!/usr/bin/env nextflow
nextflow.enable.dsl=2
params.input = "default_input"
process sayHello {
  input:
    val x
  output:
    stdout
  script:
    """
    echo '$x world!'
    """
}

workflow {
  Channel.of('Bonjour', 'Ciao', 'Hello', 'Hola', params.input) | sayHello | view
}
