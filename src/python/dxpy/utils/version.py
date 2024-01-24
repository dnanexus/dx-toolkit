class Version:

    def __init__(self, version):
        parts = version.split(".")

        if len(parts) > 3:
            raise ValueError("Unsupported version format")

        self.version = (
            int(parts[0]),
            int(parts[1]) if len(parts) > 1 else 0,
            int(parts[2]) if len(parts) > 2 else 0,
        )

    def __eq__(self, other):
        return self.version == other.version

    def __lt__(self, other):
        return self.version < other.version

    def __le__(self, other):
        return self.version <= other.version

    def __gt__(self, other):
        return self.version > other.version

    def __ge__(self, other):
        return self.version >= other.version
