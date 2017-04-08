#!/usr/bin/env python

"""Returns true if your python supports TLS v1.2, and false if not."""

import ssl
import sys

# From https://github.com/boto/botocore/blob/1.5.29/botocore/handlers.py#L636
openssl_version_tuple = ssl.OPENSSL_VERSION_INFO

upgrade_required_msg = """
PYTHON UPGRADE REQUIRED

Your Python implementation does not support TLS 1.2.

You must upgrade Python before using the DNAnexus CLI.

Next steps:
    * Install Homebrew using the guide at:  https://brew.sh/
    * Then run the command:  brew install python

Your OpenSSL version: '{0}'
""".format(ssl.OPENSSL_VERSION)

upgrade_not_required_msg = """
NO UPGRADE REQUIRED

Your Python supports TLS 1.2, and is ready to use with the DNAnexus CLI.

Your OpenSSL version: '{0}'
""".format(ssl.OPENSSL_VERSION)

if openssl_version_tuple < (1, 0, 1, 0, 0):
    print(upgrade_required_msg)
    sys.exit(1)

print(upgrade_not_required_msg)
