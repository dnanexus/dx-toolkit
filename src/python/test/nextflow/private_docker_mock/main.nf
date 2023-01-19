
nextflow.enable.dsl=2
process sayHello {
  container 'ubuntu:22.04'
  input:
    val x
  output:
    stdout
  script:
    """
    echo '$x world!'
    """
}

process sayHello2 {
  container 'private-docker-image'
  input:
    val x
  output:
    stdout
  script:
    """
    echo 'some other text world!'
    """
}

workflow {
  Channel.of('Bonjour', 'Ciao', 'Hello', 'Hola') | sayHello | sayHello2 | view
}
