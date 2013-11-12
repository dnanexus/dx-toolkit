# External APT Repo Example

This app demonstrates how to load dependencies from an external APT
repository. You might need to do something like this if your app depends
on a program or library of a version newer than that packaged in the
stock Ubuntu repositories. In this example app, we install R 3.x from an
external repository (only 2.x is available in the Ubuntu 12.04 stock
repo).

If you're familiar with setting up new APT repositories on Ubuntu, most
of the app code will be familiar. The most notable difference is that
**you must disable the default APT caching proxy** in the execution
environment so that you can reach the external APT server directly. You
can do this at the beginning of your app with the following code:

```
sudo rm -f /etc/apt/apt.conf.d/99dnanexus
```

Also, be sure to configure your app for network access in the
`access.network` field of your `dxapp.json` file. In this example app,
we've added blanket (`*`) network access. For additional safety, you can
also whitelist only the specific hosts you want to connect to (don't
forget to whitelist your keyserver, if applicable, in addition to the
APT server). See [Access
Requirements](https://wiki.dnanexus.com/API-Specification-v1.0.0/IO-and-Run-Specifications#Access-Requirements)
for more information about how to do this.
