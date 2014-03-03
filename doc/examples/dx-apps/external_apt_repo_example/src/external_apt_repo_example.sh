#!/bin/bash
#
# external_apt_repo_example

main() {

  # Bypass the APT caching proxy that is built into the execution environment.
  # It's configured to only allow access to the stock Ubuntu repos.
  sudo rm -f /etc/apt/apt.conf.d/99dnanexus

  # Set up access to the external APT repository.
  echo 'deb http://cran.rstudio.com/bin/linux/ubuntu precise/' | sudo tee /etc/apt/sources.list.d/rstudio.list

  # Trust the signing key for this repo.
  sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
  #   Reference: http://cran.rstudio.com/bin/linux/ubuntu/README.html
  #
  # Alternatively, you can save your key to a file somewhere in your resources
  # directory, e.g. resources/tmp/my-signing-key.gpg, then load it at runtime
  # as follows:
  #
  #   sudo apt-key add /tmp/my-signing-key.gpg

  sudo apt-get update --yes
  sudo apt-get remove --auto-remove --yes r-base
  sudo apt-get install --yes r-base

  # Verify that a new version of R was installed
  R --version

}
