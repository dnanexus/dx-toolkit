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

"""Unit tests for the deployed-NPI input-spec gate
(`_apply_npi_input_gate`). This is the helper extracted from
`build_pipeline_with_npi` that decides which `nextflow.config` fields to
forward to the importer and emits warnings for fields the deployed NPI
does not declare.

Behaviour the tests pin down:
  - Describe failure -> single warning, nothing forwarded.
  - All fields accepted -> all forwarded, no warning.
  - ECR-specific drop -> ECR-cluster warning (private ECR auth disabled).
  - Workdir/aws_region drop with ECR intent -> escalated ECR-blocked warning
    explicitly naming `aws_region` (BUG-3 regression test).
  - Workdir/aws_region drop without ECR intent -> generic warning only.
"""

import io
import unittest

from dxpy.nextflow.nextflow_builder import _apply_npi_input_gate


class TestApplyNpiInputGate(unittest.TestCase):

    def setUp(self):
        self.input_hash = {}
        self.stderr = io.StringIO()

    def _run(self, config_fields, accepted_inputs):
        _apply_npi_input_gate(
            config_fields=config_fields,
            accepted_inputs=accepted_inputs,
            input_hash=self.input_hash,
            stderr=self.stderr,
        )

    # --- describe-failure path ---

    def test_describe_failure_warns_and_forwards_nothing(self):
        self._run(
            config_fields={"ecr_role_arn_to_assume": "arn:role/x"},
            accepted_inputs=None,
        )
        self.assertEqual(self.input_hash, {})
        self.assertIn("Could not describe", self.stderr.getvalue())
        self.assertIn("Private ECR auth will not be configured",
                      self.stderr.getvalue())

    def test_describe_failure_with_empty_config_silent(self):
        """Empty config + describe failure -> no warning at all."""
        self._run(config_fields={}, accepted_inputs=None)
        self.assertEqual(self.input_hash, {})
        self.assertEqual(self.stderr.getvalue(), "")

    # --- happy paths ---

    def test_all_accepted_fields_forwarded_no_warning(self):
        cfg = {
            "ecr_role_arn_to_assume": "arn:role/ecr",
            "ecr_job_token_audience": "aud",
            "ecr_job_token_subject_claims": "sc",
            "aws_region": "us-east-1",
        }
        accepted = set(cfg.keys()) | {"repository_url"}
        self._run(cfg, accepted)
        self.assertEqual(self.input_hash, cfg)
        self.assertEqual(self.stderr.getvalue(), "")

    def test_empty_value_not_forwarded(self):
        """Falsy values are skipped (avoids forwarding empty-string defaults)."""
        cfg = {"ecr_role_arn_to_assume": "", "aws_region": "us-east-1"}
        self._run(cfg, accepted_inputs={"ecr_role_arn_to_assume", "aws_region"})
        self.assertEqual(self.input_hash, {"aws_region": "us-east-1"})

    # --- dropped-field warnings ---

    def test_ecr_specific_drop_emits_ecr_cluster_warning(self):
        cfg = {"ecr_role_arn_to_assume": "arn:role/ecr",
               "ecr_job_token_audience": "aud",
               "ecr_job_token_subject_claims": "sc"}
        # Older NPI: declares none of the ecr_* fields.
        self._run(cfg, accepted_inputs={"repository_url", "cache_docker"})
        self.assertEqual(self.input_hash, {})
        text = self.stderr.getvalue()
        self.assertIn("ecr_role_arn_to_assume", text)
        self.assertIn("private ECR authentication will not be set up", text)

    def test_aws_region_drop_with_ecr_intent_escalates(self):
        """BUG-3 regression: when ECR is being configured but the deployed NPI
        does not declare `aws_region`, we must emit the specific
        ECR-blocked-without-region warning, not just the generic dropped-field one.
        """
        cfg = {
            "ecr_role_arn_to_assume": "arn:role/ecr",
            "ecr_job_token_audience": "aud",
            "ecr_job_token_subject_claims": "sc",
            "aws_region": "us-east-1",
        }
        # NPI accepts all ECR-specific fields but NOT aws_region.
        accepted = {"ecr_role_arn_to_assume", "ecr_job_token_audience",
                    "ecr_job_token_subject_claims", "repository_url"}
        self._run(cfg, accepted)
        text = self.stderr.getvalue()
        self.assertIn("ECR is configured but no AWS region is available", text)
        self.assertIn("aws_region", text)

    def test_aws_region_drop_without_ecr_intent_uses_generic_warning(self):
        """User has aws.region in config but no ECR — drop should emit
        only the generic warning."""
        cfg = {"aws_region": "us-east-1"}
        self._run(cfg, accepted_inputs={"repository_url"})
        text = self.stderr.getvalue()
        self.assertIn("aws_region", text)
        self.assertNotIn("ECR is configured but no AWS region", text)
        self.assertNotIn("private ECR authentication will not be set up", text)

    def test_mixed_drop_emits_both_warnings(self):
        """If both ECR-specific and other fields are dropped, both
        warnings should appear so the user sees the full picture."""
        cfg = {
            "ecr_region_override": "us-west-2",
            "iam_role_arn_to_assume": "arn:role/wd",
        }
        self._run(cfg, accepted_inputs={"repository_url"})
        text = self.stderr.getvalue()
        self.assertIn("ecr_region_override", text)
        self.assertIn("iam_role_arn_to_assume", text)


if __name__ == "__main__":
    unittest.main()
