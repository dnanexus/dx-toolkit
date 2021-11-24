# Copyright (C) 2013-2021 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.
import os
import re
from typing import Optional, Tuple


COMPRESSED_EXTS = {"gz", "zip", "bz2", "xz", "tar", "tgz"}
FASTQ_EXTS = {"fq", "fastq"}
FASTQ_RE = re.compile(r"(.*)([-_][12])")


def extract_prefix(*names):
    """Extracts the prefix from one or more filenames."""
    prefix, _, _ = get_file_parts(*names)
    return prefix


def get_file_parts(*names) -> Tuple[str, Tuple[str, ...], Optional[Tuple[str, ...]]]:
    """
    Splits filenames into parts (prefix, extension, and (optionally) fastq mate).

    For a single file:
    1. Extract common extensions for archives/compressed files.
    2. If it is a FASTQ file, also extract the .fq/.fastq extension and any mate signifier
    (e.g. _1, .2, -1)
    3. Otherwise just extract the extension (if any).

    For multiple files:
    1. Extract the leading characters that all have in common.
    2. Extensions are extracted as above.
    3. If the files are FASTQ and the characters immediately after the prefix are mate signifiers,
    they are extracted.

    Args:
        names: File names, IDs, or links.

    Returns:
        A tuple (prefix, exts, mates), where exts is a tuple of file extensions and mates is a
        tuple of fastq mates, or None if these are not fastq files.

    Examples:
        >>> get_file_parts("foo.1.fastq", "foo.2.fastq")
        # => ("foo", ("fastq",), (".1", ".2"))
        >>> get_file_parts("foo_1.fq.gz")
        # => ("foo", ("fq", "gz"), ("_1"))
    """
    if len(names) == 0:
        raise ValueError("Must specify at least one file")
    if len(names) == 1:
        return _extract_single_file_parts(names[0])
    else:
        return _extract_multi_file_parts(names)


def _extract_single_file_parts(
    name,
) -> Tuple[str, Tuple[str, ...], Optional[Tuple[str, ...]]]:
    parts = name.split(os.path.extsep)
    exts, mate = _extract_parts(parts)
    return os.path.extsep.join(parts), exts, (mate,) if mate else None


def _extract_multi_file_parts(
    names,
) -> Tuple[str, Tuple[str, ...], Optional[Tuple[str, ...]]]:
    end = _max_common_prefix(names)

    while end > 0:
        if names[0][end - 1].isalnum():
            break
        end -= 1

    prefix = names[0][:end]
    shared_exts = set()
    mates = []

    for name in names:
        parts = name[end:].split(os.path.extsep)
        exts, mate = _extract_parts(parts)
        shared_exts.add(exts)
        if not mate and parts and (parts[0] or len(parts) > 1):
            p = parts[0] or parts[1]
            if p in {"1", "2"}:
                mate = ".{}".format(p)
            else:
                match = FASTQ_RE.match(p)
                if match:
                    mate = match.group(2)
        if mate:
            mates.append(mate)

    if len(shared_exts) > 1:
        raise ValueError("Files had different extensions")

    if not mates:
        mates = None
    elif len(mates) != len(names):
        raise ValueError("Either all or no files must have mate information")
    else:
        mates = tuple(mates)

    return prefix, (shared_exts.pop(),), mates


def _extract_parts(parts) -> Tuple[Tuple[str, ...], str]:
    exts = []
    mate = None

    while parts and parts[-1] in COMPRESSED_EXTS:
        exts.append(parts.pop())

    if parts:
        if parts[-1] in FASTQ_EXTS:
            exts.append(parts.pop())
            # Special handling for FASTQ: remove [._-]{1|2} extension
            if parts:
                if parts[-1] in {"1", "2"}:
                    mate = ".{}".format(parts.pop())
                else:
                    match = FASTQ_RE.match(parts[-1])
                    if match:
                        parts[-1] = match.group(1)
                        mate = match.group(2)
        elif len(parts) > 1:
            exts.append(parts.pop())

    return tuple(reversed(exts)), mate


def _max_common_prefix(names) -> int:
    i = 0
    n = len(names)
    maxlen = min(len(name) for name in names)
    while i < maxlen:
        c = names[0][i]
        for j in range(1, n):
            if c != names[j][i]:
                return i
        i += 1

    raise ValueError(f"names {','.join(names)} share no prefix in common")
