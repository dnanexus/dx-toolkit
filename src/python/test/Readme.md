## Python Testing

For most tests in this directory, you will need to be logged in with
appropriate credentials to create projects.  To log in from the
command line, run `dx login`.

### Environment Variables

By default, only a minimal set of tests are run.  To include more
tests, the following variables can be set:

Environment Variable | Tests included
---------------------|---------------
`DXTEST_FULL`        | All tests will be run, including those which create apps and run jobs
`DXTEST_RUN_JOBS`    | Tests that will run jobs will be run
