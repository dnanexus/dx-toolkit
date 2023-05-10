#!/bin/bash

main() {
    echo "Started"
    sleep 10s
    echo "Test to stderr" 2>&1
    sleep 60s
    echo "Finished"
}
