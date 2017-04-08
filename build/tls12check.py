#!/usr/bin/env python

"""Returns true if your python supports TLS v1.2, and false if not."""

import ssl
import sys

# See https://github.com/boto/botocore/blob/1.5.29/botocore/handlers.py#L636
openssl_version_tuple = ssl.OPENSSL_VERSION_INFO
if openssl_version_tuple < (1, 0, 1, 0, 0):
    print("TLS 1.2 is not supported by the OpenSSL version ({0}) used by your Python implementation.".format(
        ssl.OPENSSL_VERSION))
    sys.exit(1)

print("TLS 1.2 is supported by the OpenSSL version ({0}) used by your Python implementation.".format(
    ssl.OPENSSL_VERSION))
