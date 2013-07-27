## Python Testing

For most tests in this directory, you will need to be logged in with
appropriate credentials to create projects.  To log in from the
command line, run `dx login`.

### Environment Variables

By default, only a minimal set of tests are run.  To include more
tests, the following variables can be set:

Environment Variable | Tests included
---------------------|---------------
`DXTEST_FULL`        | Run tests in all categories below
`DXTEST_CREATE_APPS` | Run tests that create apps
`DXTEST_FUSE`        | Run tests against FUSE filesystems
`DXTEST_RUN_JOBS`    | Run tests that run jobs
