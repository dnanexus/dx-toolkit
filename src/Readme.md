# DNAnexus Client Libraries and Tools

## Client Bindings Tests

### Environment Variables

By default, only a minimal set of tests are run.  To include more
tests, the following variables can be set:

Environment Variable         | Tests included
-----------------------------|---------------
`DXTEST_FULL`                | Run tests in all categories below
`DXTEST_CREATE_APPS`         | Run tests that may create apps and rely on the presence of some fake users and orgs
`DXTEST_ENV`                 | Run tests which may clobber your local environment
`DXTEST_FUSE`                | Run tests against FUSE filesystems
`DXTEST_HTTP_PROXY`          | Run tests that use squid3 to launch an HTTP proxy
`DXTEST_NO_RATE_LIMITS`      | Run tests that require one or more tokens where rate limiting is not enforced
`DXTEST_RUN_JOBS`            | Run tests that run jobs
`DXTEST_TCSH`                | Run tests that require `tcsh` to be installed
`DXTEST_WITH_AUTHSERVER`     | Run tests that require a running authserver

Python and Java tests recognize these environment variables and enable or
disable tests accordingly.
