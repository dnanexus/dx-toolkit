#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013-2016 DNAnexus, Inc.
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

"""Unit tests for parse_nextflow_config_dx_fields — the best-effort parser
that extracts dnanexus.* and aws.region keys from the user's local
nextflow.config so they can be forwarded to the Nextflow Pipeline Importer
app at build time.
"""

import os
import shutil
import tempfile
import unittest

from dxpy.nextflow.nextflow_utils import (
    parse_nextflow_config_dx_fields,
    _strip_groovy_comments,
)


class _ConfigFixture(unittest.TestCase):
    """Helper base class — writes a fresh nextflow.config into a tempdir."""

    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="dx-nf-config-")

    def tearDown(self):
        shutil.rmtree(self._tmp, ignore_errors=True)

    def write_config(self, content):
        with open(os.path.join(self._tmp, "nextflow.config"), "w") as f:
            f.write(content)
        return self._tmp


class TestParseDottedForm(_ConfigFixture):
    """Dotted-form keys: `dnanexus.foo = 'bar'` on a single line."""

    def test_all_keys(self):
        src_dir = self.write_config("""
            dnanexus.iamRoleArnToAssume = 'arn:aws:iam::1:role/wd'
            dnanexus.jobTokenAudience = 'sts.amazonaws.com'
            dnanexus.jobTokenSubjectClaims = 'job_id'
            dnanexus.ecrRoleArnToAssume = 'arn:aws:iam::1:role/ecr'
            dnanexus.ecrJobTokenAudience = 'sts.amazonaws.com'
            dnanexus.ecrJobTokenSubjectClaims = 'job_id'
            aws.region = 'us-east-1'
        """)
        result = parse_nextflow_config_dx_fields(src_dir)
        self.assertEqual(result["iam_role_arn_to_assume"], "arn:aws:iam::1:role/wd")
        self.assertEqual(result["job_token_audience"], "sts.amazonaws.com")
        self.assertEqual(result["job_token_subject_claims"], "job_id")
        self.assertEqual(result["ecr_role_arn_to_assume"], "arn:aws:iam::1:role/ecr")
        self.assertEqual(result["ecr_job_token_audience"], "sts.amazonaws.com")
        self.assertEqual(result["ecr_job_token_subject_claims"], "job_id")
        self.assertEqual(result["aws_region"], "us-east-1")

    def test_double_quoted_values_accepted(self):
        src_dir = self.write_config('dnanexus.iamRoleArnToAssume = "arn:aws:iam::1:role/x"\n')
        result = parse_nextflow_config_dx_fields(src_dir)
        self.assertEqual(result["iam_role_arn_to_assume"], "arn:aws:iam::1:role/x")


class TestParseScopeBlockForm(_ConfigFixture):
    """Scope-block form: `dnanexus { foo = 'bar' }`."""

    def test_dnanexus_block(self):
        src_dir = self.write_config("""
            dnanexus {
                iamRoleArnToAssume = 'arn:aws:iam::1:role/wd'
                ecrRoleArnToAssume = "arn:aws:iam::1:role/ecr"
                ecrJobTokenAudience = 'sts.amazonaws.com'
            }
        """)
        result = parse_nextflow_config_dx_fields(src_dir)
        self.assertEqual(result["iam_role_arn_to_assume"], "arn:aws:iam::1:role/wd")
        self.assertEqual(result["ecr_role_arn_to_assume"], "arn:aws:iam::1:role/ecr")
        self.assertEqual(result["ecr_job_token_audience"], "sts.amazonaws.com")

    def test_aws_block(self):
        src_dir = self.write_config("""
            aws {
                region = 'eu-west-2'
            }
        """)
        result = parse_nextflow_config_dx_fields(src_dir)
        self.assertEqual(result["aws_region"], "eu-west-2")

    def test_dotted_form_wins_over_scope_block(self):
        """If a key appears in both forms, the dotted form (first pass) wins.
        This is intentional: it matches Nextflow's "last wins" precedence for
        most users, who almost never set the same key twice in different forms.
        """
        src_dir = self.write_config("""
            dnanexus.iamRoleArnToAssume = 'arn:dotted'
            dnanexus {
                iamRoleArnToAssume = 'arn:scope'
            }
        """)
        result = parse_nextflow_config_dx_fields(src_dir)
        self.assertEqual(result["iam_role_arn_to_assume"], "arn:dotted")


class TestComments(_ConfigFixture):
    """Comment-stripping must preserve string-literal content (URI values)."""

    def test_line_comment_in_value_preserved(self):
        """`'job://workspace_id'` must not have its `//` eaten as a comment."""
        src_dir = self.write_config(
            "dnanexus.ecrJobTokenSubjectClaims = 'job://workspace_id'\n"
        )
        result = parse_nextflow_config_dx_fields(src_dir)
        self.assertEqual(result["ecr_job_token_subject_claims"], "job://workspace_id")

    def test_trailing_line_comment_stripped(self):
        src_dir = self.write_config(
            "dnanexus.iamRoleArnToAssume = 'arn:aws:iam::1:role/x'  // trailing\n"
        )
        result = parse_nextflow_config_dx_fields(src_dir)
        self.assertEqual(result["iam_role_arn_to_assume"], "arn:aws:iam::1:role/x")

    def test_block_comment_stripped(self):
        src_dir = self.write_config("""
            dnanexus {
                /* this whole block
                   is a comment */
                iamRoleArnToAssume = 'arn:aws:iam::1:role/x'
            }
        """)
        result = parse_nextflow_config_dx_fields(src_dir)
        self.assertEqual(result["iam_role_arn_to_assume"], "arn:aws:iam::1:role/x")

    def test_full_line_comment_value_ignored(self):
        """A commented-out key must not be picked up."""
        src_dir = self.write_config(
            "// dnanexus.iamRoleArnToAssume = 'commented-out'\n"
        )
        result = parse_nextflow_config_dx_fields(src_dir)
        self.assertNotIn("iam_role_arn_to_assume", result)


class TestParseEdgeCases(unittest.TestCase):
    """Robust no-op behaviour when input is missing or empty."""

    def test_missing_src_dir(self):
        self.assertEqual(parse_nextflow_config_dx_fields(None), {})

    def test_empty_string_src_dir(self):
        self.assertEqual(parse_nextflow_config_dx_fields(""), {})

    def test_directory_without_nextflow_config(self):
        tmp = tempfile.mkdtemp()
        try:
            self.assertEqual(parse_nextflow_config_dx_fields(tmp), {})
        finally:
            shutil.rmtree(tmp)

    def test_unrelated_config_returns_empty(self):
        tmp = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmp, "nextflow.config"), "w") as f:
                f.write("process { executor = 'local' }\n")
            self.assertEqual(parse_nextflow_config_dx_fields(tmp), {})
        finally:
            shutil.rmtree(tmp)


class TestStripGroovyComments(unittest.TestCase):
    """Direct unit tests for _strip_groovy_comments — focuses on the
    string-masking behaviour added in pass-1 review fixes."""

    def test_uri_in_double_quoted_string_preserved(self):
        out = _strip_groovy_comments('x = "https://example.com/path"\n')
        self.assertIn('"https://example.com/path"', out)

    def test_uri_in_single_quoted_string_preserved(self):
        out = _strip_groovy_comments("x = 'job://workspace_id'\n")
        self.assertIn("'job://workspace_id'", out)

    def test_line_comment_stripped(self):
        out = _strip_groovy_comments("x = 1 // trailing\n")
        self.assertNotIn("trailing", out)

    def test_block_comment_stripped(self):
        out = _strip_groovy_comments("/* a\nb */ x = 1\n")
        self.assertNotIn("a", out.split("=")[0])
        self.assertNotIn("b", out.split("=")[0])

    def test_block_comment_preserves_newlines(self):
        """Block comment newlines must be kept so line-anchored regexes
        on the output don't shift."""
        text = "/* a\nb */"
        out = _strip_groovy_comments(text)
        self.assertEqual(out.count("\n"), text.count("\n"))


if __name__ == "__main__":
    unittest.main()
