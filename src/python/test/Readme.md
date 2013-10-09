## Python Testing

For most tests in this directory, you will need to be logged in with
appropriate credentials to create projects.  To log in from the
command line, run `dx login`.

To run a particular test case, you can do so by running the `.py` file
containing it with the argument `TestClassName.test_function_name`.
For example, the following runs the test `test_dx_project_tagging` in
`test_dxclient.py`:

```bash
$ ./test_dxclient.py TestDXClient.test_dx_project_tagging
```

Note however that in this case, the test runs your currently-installed
version of `dx`, so if you have made local changes, you should rebuild
it before running the test.

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
