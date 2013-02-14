# Testing R Bindings

We use the package `testthat` to run tests on our R code.  All test
files should be named to start with "test".  A full resource on this
package can be found in this journal article:
http://journal.r-project.org/archive/2011-1/RJournal_2011-1_Wickham.pdf

Tests that do not require an Internet connection and/or access to a
DNAnexus API server should be put in the appropriate R packages so
that they can be run automatically via `R CMD check`.  Otherwise, all
such integration tests should be placed in this directory.

## Integration Tests

For now, integration tests are written with the assumption that there
is a valid dx environment to be loaded, including a valid security
context and current project in which CONTRIBUTE+ permissions are
available.  (These requirements may be relaxed over time as tests
mature.)

To run tests in this directory, make sure you have installed R and the
`dxR` package, and then run the script `run_all_tests.sh` in this
directory.