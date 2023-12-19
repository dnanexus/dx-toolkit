class Version:

    def __init__(self, version):
        parts = version.split(".")

        if len(parts) > 3:
            raise ValueError("Unsupported version format")

        self.major = int(parts[0])
        self.minor = None
        self.patch = None
        if len(parts) > 1:
            self.minor = int(parts[1])
        if len(parts) > 2:
            self.patch = int(parts[2])

    def _cmp(self, other):
        if self.major > other.major:
            return 1
        if self.major < other.major:
            return -1

        if (self.minor or 0) > (other.minor or 0):
            return 1
        if (self.minor or 0) < (other.minor or 0):
            return -1

        if (self.patch or 0) > (other.patch or 0):
            return 1
        if (self.patch or 0) < (other.patch or 0):
            return -1

        return 0

    def __eq__(self, other):
        return self._cmp(other) == 0

    def __lt__(self, other):
        return self._cmp(other) < 0

    def __le__(self, other):
        return self._cmp(other) <= 0

    def __gt__(self, other):
        return self._cmp(other) > 0

    def __ge__(self, other):
        return self._cmp(other) >= 0