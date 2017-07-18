#!/usr/bin/env python

"""Returns true if your python supports TLS v1.2, and false if not."""

import ssl
import sys

# From https://github.com/boto/botocore/blob/1.5.29/botocore/handlers.py#L636
openssl_version_tuple = ssl.OPENSSL_VERSION_INFO

upgrade_required_msg = """
UPGRADE REQUIRED
Your OpenSSL version: '{0}'
The OpenSSL used by your Python implementation does not support TLS 1.2.
The DNAnexus CLI requires a Python release built with OpenSSL 1.0.1 or greater.
""".format(ssl.OPENSSL_VERSION)

upgrade_not_required_msg = """
NO UPGRADE REQUIRED
Your OpenSSL version: '{0}'
Your Python supports TLS 1.2, and is ready to use with the DNAnexus CLI.
""".format(ssl.OPENSSL_VERSION)

if openssl_version_tuple < (1, 0, 1, 0, 0):
    print(upgrade_required_msg)
    sys.exit(1)

print(upgrade_not_required_msg)
