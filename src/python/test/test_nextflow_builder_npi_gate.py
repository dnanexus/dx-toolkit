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
  - Describe failure with ECR intent -> hard error (refuse to launch).
  - Describe failure without ECR intent -> warning, nothing forwarded.
  - All fields accepted -> all forwarded, no warning.
  - ECR-specific drop -> hard error (refuse to launch; otherwise the build
    would fail several minutes in at the docker-pull step).
  - aws_region drop with ECR intent -> hard error (ECR needs a region).
  - aws_region drop without ECR intent -> generic warning only.
"""

import io
import unittest
from unittest import mock

import dxpy
from dxpy.nextflow.nextflow_builder import (
    _apply_npi_input_gate,
    preflight_validate_for_cache_docker,
)


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

    def test_describe_failure_with_ecr_intent_raises(self):
        """ECR was clearly requested but we can't verify the NPI accepts the
        fields — refuse to launch rather than fail several minutes in."""
        with self.assertRaises(dxpy.exceptions.DXError) as cm:
            self._run(
                config_fields={"ecr_role_arn_to_assume": "arn:role/x"},
                accepted_inputs=None,
            )
        self.assertIn("Could not describe", str(cm.exception))
        self.assertEqual(self.input_hash, {})

    def test_describe_failure_without_ecr_intent_warns(self):
        """No ECR intent — degrade to a warning so non-ECR pipelines still build."""
        self._run(
            config_fields={"iam_role_arn_to_assume": "arn:role/wd"},
            accepted_inputs=None,
        )
        self.assertEqual(self.input_hash, {})
        self.assertIn("Could not describe", self.stderr.getvalue())

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

    def test_ecr_specific_drop_raises(self):
        """Older NPI lacks ecr_* inputs: refuse to launch (the docker pull
        step would fail without credentials)."""
        cfg = {"ecr_role_arn_to_assume": "arn:role/ecr",
               "ecr_job_token_audience": "aud",
               "ecr_job_token_subject_claims": "sc"}
        with self.assertRaises(dxpy.exceptions.DXError) as cm:
            self._run(cfg, accepted_inputs={"repository_url", "cache_docker"})
        self.assertIn("ecr_role_arn_to_assume", str(cm.exception))
        self.assertIn("Upgrade the importer app", str(cm.exception))

    def test_aws_region_drop_with_ecr_intent_raises(self):
        """ECR needs a region; if the NPI doesn't accept aws_region, refuse
        to launch rather than fail mid-job with the opaque 'no AWS region
        available' error."""
        cfg = {
            "ecr_role_arn_to_assume": "arn:role/ecr",
            "ecr_job_token_audience": "aud",
            "ecr_job_token_subject_claims": "sc",
            "aws_region": "us-east-1",
        }
        # NPI accepts all ECR-specific fields but NOT aws_region.
        accepted = {"ecr_role_arn_to_assume", "ecr_job_token_audience",
                    "ecr_job_token_subject_claims", "repository_url"}
        with self.assertRaises(dxpy.exceptions.DXError) as cm:
            self._run(cfg, accepted)
        self.assertIn("aws_region", str(cm.exception))

    def test_aws_region_drop_without_ecr_intent_uses_generic_warning(self):
        """User has aws.region in config but no ECR — drop should emit
        only the generic warning."""
        cfg = {"aws_region": "us-east-1"}
        self._run(cfg, accepted_inputs={"repository_url"})
        text = self.stderr.getvalue()
        self.assertIn("aws_region", text)
        self.assertNotIn("private ECR authentication", text)

    def test_mixed_drop_with_ecr_intent_raises(self):
        """If any ECR-specific field is dropped, refuse to launch (ECR
        intent + missing input slot would silently fail at docker-pull)."""
        cfg = {
            "ecr_region_override": "us-west-2",
            "iam_role_arn_to_assume": "arn:role/wd",
        }
        with self.assertRaises(dxpy.exceptions.DXError) as cm:
            self._run(cfg, accepted_inputs={"repository_url"})
        self.assertIn("ecr_region_override", str(cm.exception))


class TestPreflightValidateForCacheDocker(unittest.TestCase):
    """F9-2/F9-3/F9-4: pre-upload validation that runs before
    `.nf_source/` upload so a fail-fast does not leave orphaned data.
    """

    def test_describe_failure_always_raises(self):
        """F9-4: tighter than the in-build gate — describe failure on a
        --cache-docker build always raises, regardless of ECR intent."""
        with mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value=None,
        ):
            with self.assertRaises(dxpy.exceptions.DXError) as cm:
                preflight_validate_for_cache_docker(src_dir=None, ecr_region=None)
            self.assertIn("Could not describe", str(cm.exception))

    def test_repository_mode_with_complete_npi_passes(self):
        """F9-3: --repository mode + NPI declares ECR slots -> OK
        (we cannot detect ECR intent from the remote repo's config)."""
        with mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value={
                "ecr_role_arn_to_assume", "ecr_job_token_audience",
                "ecr_job_token_subject_claims", "ecr_region_override",
                "aws_region", "repository_url",
            },
        ):
            preflight_validate_for_cache_docker(src_dir=None, ecr_region=None)

    def test_repository_mode_with_older_npi_raises(self):
        """F9-3: --repository mode against an older NPI lacking ECR slots
        must raise — otherwise the importer would clone a repo with
        ecrRoleArnToAssume and silently fail at docker pull."""
        with mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value={"repository_url", "cache_docker"},
        ):
            with self.assertRaises(dxpy.exceptions.DXError) as cm:
                preflight_validate_for_cache_docker(src_dir=None, ecr_region=None)
            self.assertIn("ecr_role_arn_to_assume", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
