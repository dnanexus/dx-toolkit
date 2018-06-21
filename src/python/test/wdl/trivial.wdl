# A trivial script, to test the basic sanity
# of a dxWDL release.

workflow trivial {
    Int x = 3
    Int y = 5

    call  Add {
        input: a=x, b=y
    }
    output {
        Int sum = Add.result
    }
}

task Add {
    Int a
    Int b

    command {
        echo $((${a} + ${b}))
    }
    output {
        Int result = read_int(stdout())
    }
}
