## Python Testing

For most tests in this directory, you will need to be logged in with
appropriate credentials to create projects.  To log in from the
command line, run `dx login`.

You can run a particular test case by running the `.py` file
containing it with the argument `TestClassName.test_function_name`.
For example, the following runs the test `test_dx_project_tagging` in
`test_dxclient.py`:

```bash
$ ./test_dxclient.py TestDXClient.test_dx_project_tagging
```

Note however that in this case, the test runs your currently-installed
version of `dx`, so if you have made local changes, you should rebuild
it before running the test.
