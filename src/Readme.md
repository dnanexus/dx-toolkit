## Tests

### Environment Variables

By default, only a minimal set of tests are run.  To include more
tests, the following variables can be set:

Environment Variable | Tests included
---------------------|---------------
`DXTEST_FULL`        | Run tests in all categories below
`DXTEST_CREATE_APPS` | Run tests that create apps
`DXTEST_FUSE`        | Run tests against FUSE filesystems
`DXTEST_RUN_JOBS`    | Run tests that run jobs
`DXTEST_HTTP_PROXY`  | Run tests that use squid3 to launch an HTTP proxy
`DXTEST_ENV`         | Run tests which may clobber your local environment

Python and Java tests recognize these environment variables and enable or
disable tests accordingly.
