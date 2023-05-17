#!/bin/bash

main() {
    echo "Value of inp1: '$inp1'"
    echo "Value of inp2: '$inp2'"

    dx download "$inp2" -o inp2
    cat inp2
}
