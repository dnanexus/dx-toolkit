# DNAnexus Client Libraries and Tools

## Client Bindings Tests

### Environment Variables

By default, only a minimal set of tests are run.  To include more
tests, the following variables can be set:

Environment Variable         | Tests included
-----------------------------|---------------
`DXTEST_FULL`                | Run tests in all categories below
`DXTEST_AZURE`               | Run tests that require permission to use an Azure region (by default `azure:westus` but the region can also be read from the value of this variable)
`DXTEST_ENV`                 | Run tests which may clobber your local environment
`DXTEST_FUSE`                | Run tests against FUSE filesystems
`DXTEST_HTTP_PROXY`          | Run tests that use squid3 to launch an HTTP proxy
`DXTEST_ISOLATED_ENV`        | Run tests that may create, or rely on the presence of, certain apps, users, orgs, etc.
`DXTEST_NO_RATE_LIMITS`      | Run tests that require one or more tokens where rate limiting is not enforced
`DXTEST_RUN_JOBS`            | Run tests that run jobs
`DXTEST_SECOND_USER`         | Run tests that require multiple users
`DXTEST_TCSH`                | Run tests that require `tcsh` to be installed
`DXTEST_WITH_AUTHSERVER`     | Run tests that require a running authserver
`DX_RUN_NEXT_TESTS`          | Run tests that require synchronous updates to backend
`DXTEST_NF_DOCKER`           | Run tests that require docker for Nextflow


Python and Java tests recognize these environment variables and enable or
disable tests accordingly.

#### DXTEST_ENV

The DXTEST_ENV test flag is meant to protect tests that actually make changes
to the on-disk serialized config settings (`~/.dnanexus_config`), since that
would erase whatever token the calling user had set there from dx login,
etc. and we don't want to do that by default when a user runs tests.

Since the environment variables override the on-disk settings, setting
environment variables is usually sufficient for most tests where we'd like to
apply a particular project context or auth context. The notable exception is if
we want to test the behavior of some dx command in the *absence* of any project
context or auth context. In that case, unsetting the environment variable would
only allow the on-disk setting to be effective, so for such tests we have to
blow away the on-disk setting, too.
