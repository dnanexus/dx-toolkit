#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2016 DNAnexus, Inc.
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

"""Bash-side tests for the runtime ECR helpers in nextflow.sh and the bash
fragment embedded in DxBashLib.groovy. We extract the helper definitions
from the source files at test time so this file stays in sync with the
implementation, then exec them in a bash harness with `aws` and `docker`
mocked via PATH so we can assert on call arguments and exit codes
without launching a real DNAnexus job.

Tests cover:
  - is_ecr_host: hostname classifier (commercial AWS only)
  - extract_ecr_host_from_image: hostname-from-image-ref extractor
  - ecr_region_from_host: region parser
  - _is_ecr_auth_error: error-text classifier used to decide on relogin
  - ecr_docker_login: cache-hit short-circuit
"""

import os
import re
import shutil
import stat
import subprocess
import tempfile
import unittest

# Path to the Groovy file that defines the bash helpers used at task-runtime.
# We do not try to render the Groovy plugin; we just slice out the bash text
# between the `'''` delimiters in the dxLib() method, which is verbatim bash.
#
# Try, in order:
#   1. $DX_NEXTAUR_BASH_LIB env var (CI override).
#   2. The sibling nextaur-app checkout under the same parent directory as
#      dx-toolkit (or a worktree subdirectory). This is how a developer
#      typically has both repos available.
def _find_dx_bash_lib():
    env_override = os.environ.get("DX_NEXTAUR_BASH_LIB")
    if env_override and os.path.isfile(env_override):
        return env_override
    test_dir = os.path.dirname(os.path.abspath(__file__))
    # Walk up to the dnanexus parent dir (ancestor of dx-toolkit AND nextaur-app).
    cur = test_dir
    for _ in range(10):
        cur = os.path.dirname(cur)
        if not cur or cur == "/":
            break
        # Try worktrees first — they're more likely to have unmerged Phase 1
        # ECR helpers. Fall back to the main checkout (which becomes correct
        # after the Phase 1 PR lands).
        candidates = []
        wt_root = os.path.join(cur, "nextaur-app", ".claude", "worktrees")
        if os.path.isdir(wt_root):
            for wt in sorted(os.listdir(wt_root)):
                candidates.append(os.path.join(
                    wt_root, wt, "plugins", "nextaur", "src", "main",
                    "nextflow", "cloud", "dnanexus", "DxBashLib.groovy",
                ))
        candidates.append(os.path.join(
            cur, "nextaur-app", "plugins", "nextaur", "src", "main",
            "nextflow", "cloud", "dnanexus", "DxBashLib.groovy",
        ))
        # Pick the first candidate that actually contains the ECR helpers,
        # so a stale main checkout doesn't shadow a current worktree.
        for c in candidates:
            if os.path.isfile(c):
                try:
                    with open(c) as f:
                        if "is_ecr_host" in f.read():
                            return c
                except OSError:
                    continue
    return None


_DX_BASH_LIB_GROOVY = _find_dx_bash_lib()


def _extract_bash_block(groovy_path, method_name="dxLib"):
    """Extract the verbatim bash inside `groovy_method() { '''...''' }`.

    Returns the bash as a string ready to source. If the file doesn't
    exist (e.g. tests run in a CI env without the nextaur worktree), the
    caller skips with a clear reason.
    """
    if not os.path.isfile(groovy_path):
        return None
    with open(groovy_path, "r") as f:
        text = f.read()
    # Match `String dxLib() {\n        '''<BASH>'''.stripIndent()\n    }`
    pat = re.compile(
        r"String\s+" + re.escape(method_name) + r"\s*\(\s*\)\s*\{\s*'''(.+?)'''\.stripIndent\(\)",
        re.DOTALL,
    )
    m = pat.search(text)
    if not m:
        return None
    # Groovy triple-quoted multi-line strings still process escape sequences.
    # `\\.` in the source becomes a single `\.` in the rendered bash; without
    # this conversion, the extracted text has `\\.` which bash interprets
    # differently inside `[[ =~ ]]` regexes (the regex `[0-9]+\\.dkr` would
    # require a literal backslash before `dkr` — never matches a real host).
    body = m.group(1)
    # Order matters: handle `\\` (double-backslash → single) before single-char
    # escapes so we don't double-process. Use a placeholder pass.
    body = body.replace("\\\\", "\x00DBS\x00")
    body = body.replace("\\'", "'")
    body = body.replace('\\"', '"')
    body = body.replace("\\n", "\n")
    body = body.replace("\\t", "\t")
    body = body.replace("\x00DBS\x00", "\\")
    return body


def _run_bash(harness, env=None):
    """Run a bash one-liner harness, capturing stdout/stderr/rc."""
    proc = subprocess.run(
        ["bash", "-c", harness],
        capture_output=True, text=True, env=env, check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


class _MockBin:
    """Context manager that puts a fake-bin dir at the head of PATH so calls
    to `aws` / `docker` etc. inside a bash harness hit our scripts.
    """
    def __init__(self, scripts):
        # scripts: dict {"aws": "#!/bin/bash\n...\n", "docker": "..."}
        self.scripts = scripts

    def __enter__(self):
        self.tmp = tempfile.mkdtemp(prefix="dx-mockbin-")
        for name, content in self.scripts.items():
            path = os.path.join(self.tmp, name)
            with open(path, "w") as f:
                f.write(content)
            os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        self.env = os.environ.copy()
        self.env["PATH"] = self.tmp + os.pathsep + self.env.get("PATH", "")
        # Track call records via a file each mock writes to.
        self.calls_log = os.path.join(self.tmp, "_calls.log")
        self.env["MOCK_CALLS_LOG"] = self.calls_log
        return self

    def __exit__(self, *a):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def calls(self):
        if not os.path.exists(self.calls_log):
            return []
        with open(self.calls_log) as f:
            return [line.rstrip("\n") for line in f]


@unittest.skipUnless(
    _DX_BASH_LIB_GROOVY is not None and _extract_bash_block(_DX_BASH_LIB_GROOVY) is not None,
    "DxBashLib.groovy not available in this checkout (nextaur-app sibling absent — set DX_NEXTAUR_BASH_LIB to override)",
)
class TestDxBashLibEcrHelpers(unittest.TestCase):
    """Tests for the bash fragment in DxBashLib.groovy.dxLib()."""

    @classmethod
    def setUpClass(cls):
        cls.bash = _extract_bash_block(_DX_BASH_LIB_GROOVY)

    def _harness(self, body):
        # Source the bash block then run the test body. Stdin closed (`< /dev/null`)
        # so any accidental interactive prompt aborts.
        return f"set -e\n{self.bash}\n{body}\n"

    def test_is_ecr_host_commercial(self):
        rc, out, err = _run_bash(self._harness(
            'is_ecr_host "123456789012.dkr.ecr.us-east-1.amazonaws.com" '
            '&& echo YES || echo NO'
        ))
        self.assertEqual(out.strip(), "YES")

    def test_is_ecr_host_govcloud_rejected(self):
        rc, out, err = _run_bash(self._harness(
            'is_ecr_host "123.dkr.ecr.us-gov-west-1.amazonaws.com" '
            '&& echo YES || echo NO'
        ))
        self.assertEqual(out.strip(), "NO")

    def test_is_ecr_host_china_rejected(self):
        rc, out, err = _run_bash(self._harness(
            'is_ecr_host "123.dkr.ecr.cn-north-1.amazonaws.com.cn" '
            '&& echo YES || echo NO'
        ))
        self.assertEqual(out.strip(), "NO")

    def test_is_ecr_host_public_registry_rejected(self):
        rc, out, err = _run_bash(self._harness(
            'is_ecr_host "quay.io" && echo YES || echo NO'
        ))
        self.assertEqual(out.strip(), "NO")

    def test_extract_host_from_ecr_image(self):
        rc, out, err = _run_bash(self._harness(
            'extract_ecr_host_from_image "123.dkr.ecr.us-east-1.amazonaws.com/myrepo:tag"'
        ))
        self.assertEqual(out.strip(), "123.dkr.ecr.us-east-1.amazonaws.com")

    def test_extract_host_from_non_ecr_image_empty(self):
        rc, out, err = _run_bash(self._harness(
            'extract_ecr_host_from_image "quay.io/biocontainers/fastqc:1.0"'
        ))
        # No host echoed — function deliberately silent for non-ECR.
        self.assertEqual(out.strip(), "")

    def test_extract_host_lowercases(self):
        rc, out, err = _run_bash(self._harness(
            'extract_ecr_host_from_image "123.DKR.ECR.US-EAST-1.AMAZONAWS.COM/repo"'
        ))
        self.assertEqual(out.strip(), "123.dkr.ecr.us-east-1.amazonaws.com")

    def test_ecr_region_from_host(self):
        rc, out, err = _run_bash(self._harness(
            'ecr_region_from_host "123.dkr.ecr.eu-west-2.amazonaws.com"'
        ))
        self.assertEqual(out.strip(), "eu-west-2")

    def test_is_ecr_auth_error_recognises_denied(self):
        rc, out, err = _run_bash(self._harness(
            '_is_ecr_auth_error "Error response from daemon: denied: User not authorized" '
            '&& echo YES || echo NO'
        ))
        self.assertEqual(out.strip(), "YES")

    def test_is_ecr_auth_error_recognises_expired_token(self):
        rc, out, err = _run_bash(self._harness(
            '_is_ecr_auth_error "ExpiredToken: The security token included in the request is expired" '
            '&& echo YES || echo NO'
        ))
        self.assertEqual(out.strip(), "YES")

    def test_is_ecr_auth_error_rejects_network_error(self):
        rc, out, err = _run_bash(self._harness(
            '_is_ecr_auth_error "Error response from daemon: connection refused" '
            '&& echo YES || echo NO'
        ))
        self.assertEqual(out.strip(), "NO")

    def test_ecr_docker_login_cache_hit_short_circuits(self):
        """If the host is already in the cache file, ecr_docker_login must
        return immediately without invoking aws_ecr or docker."""
        # Mock aws and docker to write to a sentinel log on every call.
        # If the cache short-circuit works, the log stays empty.
        scripts = {
            "aws": '#!/bin/bash\necho "aws CALLED $*" >> "$MOCK_CALLS_LOG"\nexit 0\n',
            "docker": '#!/bin/bash\necho "docker CALLED $*" >> "$MOCK_CALLS_LOG"\nexit 0\n',
        }
        with _MockBin(scripts) as mock:
            cache = os.path.join(mock.tmp, "logged_in_hosts")
            with open(cache, "w") as f:
                f.write("123.dkr.ecr.us-east-1.amazonaws.com\n")
            harness = (
                f"export ECR_LOGGED_IN_HOSTS_FILE={shlex_quote(cache)}\n"
                f"export ECR_AWS_CONFIG_FILE=/dev/null\n"
                f"{self.bash}\n"
                'ecr_docker_login "123.dkr.ecr.us-east-1.amazonaws.com" '
                '&& echo OK || echo FAIL\n'
            )
            rc, out, err = _run_bash("set -e\n" + harness, env=mock.env)
            self.assertEqual(out.strip(), "OK")
            self.assertEqual(mock.calls(), [])  # neither aws nor docker invoked


def shlex_quote(s):
    # Local import to keep top of file tidy.
    import shlex
    return shlex.quote(s)


if __name__ == "__main__":
    unittest.main()
