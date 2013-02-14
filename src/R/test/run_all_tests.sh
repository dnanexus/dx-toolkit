#!/bin/bash -e

R -q --no-save --no-restore -e 'library(testthat); result <- test_dir("."); if (result$failed) { quit(status=1) }'

