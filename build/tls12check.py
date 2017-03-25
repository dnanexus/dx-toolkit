#!/usr/bin/env python

"""Returns true if your python supports TLS v1.2, and false if not."""

import ssl
import sys

# See https://github.com/boto/botocore/blob/1.5.29/botocore/handlers.py#L636
openssl_version_tuple = ssl.OPENSSL_VERSION_INFO
if openssl_version_tuple < (1, 0, 1, 0, 0):
    print("The installed OpenSSL version ({0}) does not support TLS 1.2.".format(
        ssl.OPENSSL_VERSION))
    sys.exit(1)

print("The installed OpenSSL version ({0}) supports TLS 1.2.".format(
    ssl.OPENSSL_VERSION))
