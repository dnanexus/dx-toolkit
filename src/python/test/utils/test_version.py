import pytest

from parameterized import parameterized
from dxpy_testutil import DXTestCase

from dxpy.utils.version import Version


class TestVersion(DXTestCase):

    def test_version(self):
        assert Version("2") > Version("1")
        assert Version("2") >= Version("1")
        assert Version("1") >= Version("1")
        assert Version("1") < Version("3")
        assert Version("1") <= Version("3")
        assert Version("1") <= Version("1")
        assert Version("1") == Version("1")
        assert Version("2.1") > Version("2")
        assert Version("2.1") >= Version("2")
        assert Version("2.2") > Version("2.1")
        assert Version("2.2") >= Version("2.1")
        assert Version("2.1") >= Version("2.1")
        assert Version("2.1") < Version("2.2")
        assert Version("2.1") <= Version("2.2")
        assert Version("2.1") <= Version("2.1")
        assert Version("2.1") == Version("2.1")
        assert Version("2.1.1") > Version("2.1")
        assert Version("2.1.1") >= Version("2.1")
        assert Version("2.1.2") > Version("2.1.1")
        assert Version("2.1.2") >= Version("2.1.1")
        assert Version("2.1.1") >= Version("2.1.1")
        assert Version("2.1.1") < Version("2.1.2")
        assert Version("2.1.1") <= Version("2.1.2")
        assert Version("2.1.1") <= Version("2.1.1")
        assert Version("2.1.2") == Version("2.1.2")

    @parameterized.expand([
        (None, ),
        ("1.2.3.4", ),
        ("1.b.3", ),
        ("2.0 beta", ),
        ("version", )
    ])
    def test_version_invalid(self, version):
        with pytest.raises(Exception):
            Version(version)
