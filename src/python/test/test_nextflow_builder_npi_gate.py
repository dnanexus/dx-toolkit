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

"""Unit tests for APPS-3915 NPI-related checks in nextflow_builder.py.

Covers:
  - preflight_validate_for_cache_docker: NPI describe failure, ECR slot check,
    floating-tag guard ordering, and ECR-in-config-without-CLI-flag non-regression.
  - _npi_input_names / get_importer_name: custom NPI override via DX_NPI_NAME.
  - scan_ecr_floating_tags_in_config: profile-aware floating-tag detection.

NOTE(APPS-3915): The workdir config forwarding path (_apply_npi_input_gate,
parse_nextflow_config_dx_fields in build_pipeline_with_npi) was removed — the
NPI reads nextflow.config from the uploaded source directory directly.  This
file no longer contains tests for workdir forwarding.

TODO(APPS-3915): Delete this entire file once the NPI version that declares
ecr_role_arn_to_assume / ecr_job_token_audience / ecr_job_token_subject_claims
is the minimum deployed version. See companion TODO in nextflow_builder.py
above _npi_input_names().
"""

import os
import tempfile
import textwrap
import unittest
from unittest import mock

import dxpy
from dxpy.nextflow.nextflow_builder import (
    preflight_validate_for_cache_docker,
)
from dxpy.nextflow.collect_images import scan_ecr_floating_tags_in_config
from dxpy.nextflow.nextflow_utils import get_importer_name


class TestPreflightValidateForCacheDocker(unittest.TestCase):
    """Pre-upload validation that runs before `.nf_source/` upload so a
    fail-fast does not leave orphaned data.

    ECR intent is now signalled via the explicit ``ecr_role_arn`` parameter
    (from ``--ecr-role-arn`` CLI flag), NOT from nextflow.config.  Tests
    reflect this: passing ecr_role_arn= triggers the NPI slot check and the
    floating-tag guard; omitting it skips both regardless of what the config
    contains.
    """

    def test_describe_failure_always_raises(self):
        """Describe failure on a --cache-docker build always raises."""
        with mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value=None,
        ):
            with self.assertRaises(dxpy.exceptions.DXError) as cm:
                preflight_validate_for_cache_docker(src_dir=None)
            self.assertIn("Could not describe", str(cm.exception))

    def test_no_ecr_role_arg_passes_even_with_older_npi(self):
        """When --ecr-role-arn is not passed, no ECR slot check fires.
        Non-ECR builds succeed against older NPI."""
        with mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value={"repository_url", "cache_docker"},
        ):
            preflight_validate_for_cache_docker(src_dir=None, ecr_role_arn=None)

    def test_ecr_role_arg_with_complete_npi_passes(self):
        """--ecr-role-arn + NPI declares ECR slots -> OK."""
        with mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value={
                "ecr_role_arn_to_assume", "ecr_job_token_audience",
                "ecr_job_token_subject_claims", "repository_url",
            },
        ):
            preflight_validate_for_cache_docker(
                src_dir=None,
                ecr_role_arn="arn:aws:iam::123456789:role/EcrRole",
            )

    def test_ecr_role_arg_with_older_npi_raises(self):
        """--ecr-role-arn against an older NPI lacking ECR slots must raise
        before upload — the importer would silently fall back to anonymous
        pulls and fail at the docker-pull step."""
        with mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value={"repository_url", "cache_docker"},
        ):
            with self.assertRaises(dxpy.exceptions.DXError) as cm:
                preflight_validate_for_cache_docker(
                    src_dir=None,
                    ecr_role_arn="arn:aws:iam::123456789:role/EcrRole",
                )
            self.assertIn("ecr_role_arn_to_assume", str(cm.exception))

    def test_ecr_in_config_without_cli_flag_does_not_raise(self):
        """--ecr-role-arn not passed -> ECR slot check never fires, even when
        nextflow.config contains ecrRoleArnToAssume.  Runtime config is irrelevant
        to build-time ECR auth (Option 2 design).  preflight no longer reads
        nextflow.config at all; this test pins the no-raise guarantee."""
        with mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value={"repository_url", "cache_docker"},
        ):
            # Must not raise — runtime ECR config does not drive preflight.
            preflight_validate_for_cache_docker(src_dir=None, ecr_role_arn=None)


class TestGetImporterName(unittest.TestCase):
    """get_importer_name() must fall back to the default when DX_NPI_NAME is
    absent, None, or empty string (GHA sets it to "" when the workflow input
    is left blank — os.environ.get("DX_NPI_NAME", default) returns "" in that
    case, which must not be used as the app name)."""

    def test_unset_returns_default(self):
        env = {k: v for k, v in os.environ.items() if k != "DX_NPI_NAME"}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(get_importer_name(), "nextflow_pipeline_importer")

    def test_empty_string_returns_default(self):
        with mock.patch.dict(os.environ, {"DX_NPI_NAME": ""}):
            self.assertEqual(get_importer_name(), "nextflow_pipeline_importer")

    def test_custom_name_returned(self):
        with mock.patch.dict(os.environ, {"DX_NPI_NAME": "my_custom_importer"}):
            self.assertEqual(get_importer_name(), "my_custom_importer")

    def test_applet_id_returned(self):
        with mock.patch.dict(os.environ, {"DX_NPI_NAME": "applet-xxxx0000xxxx0000xxxx0000"}):
            self.assertEqual(get_importer_name(), "applet-xxxx0000xxxx0000xxxx0000")


def _write_config(directory, content):
    """Write *content* to nextflow.config inside *directory*."""
    path = os.path.join(directory, "nextflow.config")
    with open(path, "w") as fh:
        fh.write(textwrap.dedent(content))
    return path


_ECR_ROLE = "arn:aws:iam::123456789012:role/EcrRole"
_ECR_IMAGE_LATEST = "123456789012.dkr.ecr.us-east-1.amazonaws.com/myrepo:latest"
_ECR_IMAGE_PINNED = "123456789012.dkr.ecr.us-east-1.amazonaws.com/myrepo@sha256:abc123"
_ECR_IMAGE_TAG = "123456789012.dkr.ecr.us-east-1.amazonaws.com/myrepo:v1.0.0"
_PUBLIC_IMAGE_LATEST = "ubuntu:latest"


class TestScanEcrFloatingTagsInConfig(unittest.TestCase):
    """Unit tests for scan_ecr_floating_tags_in_config."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_no_config_returns_empty(self):
        result = scan_ecr_floating_tags_in_config(self._tmpdir)
        self.assertEqual(result, [])

    def test_pinned_ecr_tag_not_flagged(self):
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_PINNED}'
        """)
        self.assertEqual(scan_ecr_floating_tags_in_config(self._tmpdir), [])

    def test_explicit_semver_ecr_tag_not_flagged(self):
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_TAG}'
        """)
        self.assertEqual(scan_ecr_floating_tags_in_config(self._tmpdir), [])

    def test_public_image_latest_not_flagged(self):
        """Only ECR hostnames are checked — public :latest is fine."""
        _write_config(self._tmpdir, f"""
            process.container = '{_PUBLIC_IMAGE_LATEST}'
        """)
        self.assertEqual(scan_ecr_floating_tags_in_config(self._tmpdir), [])

    def test_top_level_ecr_latest_flagged(self):
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_LATEST}'
        """)
        self.assertEqual(
            scan_ecr_floating_tags_in_config(self._tmpdir),
            [_ECR_IMAGE_LATEST],
        )

    def test_profile_override_ecr_latest_flagged_when_profile_active(self):
        """A floating tag inside the active profile block is caught."""
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_PINNED}'
            profiles {{
                bad_profile {{
                    process.container = '{_ECR_IMAGE_LATEST}'
                }}
            }}
        """)
        result = scan_ecr_floating_tags_in_config(self._tmpdir, profile="bad_profile")
        self.assertEqual(result, [_ECR_IMAGE_LATEST])

    def test_inactive_profile_ecr_latest_not_flagged(self):
        """A floating tag in an INACTIVE profile must not cause a false positive."""
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_PINNED}'
            profiles {{
                bad_profile {{
                    process.container = '{_ECR_IMAGE_LATEST}'
                }}
            }}
        """)
        # No profile specified → only the top-level (pinned) container is checked.
        self.assertEqual(scan_ecr_floating_tags_in_config(self._tmpdir), [])

    def test_profile_without_container_override_falls_back_to_top_level(self):
        """When the active profile doesn't set process.container, use the default."""
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_LATEST}'
            profiles {{
                no_container_override {{
                    docker.enabled = true
                }}
            }}
        """)
        result = scan_ecr_floating_tags_in_config(self._tmpdir, profile="no_container_override")
        self.assertEqual(result, [_ECR_IMAGE_LATEST])


class TestPreflightFloatingTagGuard(unittest.TestCase):
    """Floating-tag guard in preflight_validate_for_cache_docker.

    The guard fires when BOTH conditions hold:
      1. --ecr-role-arn is passed (ecr_role_arn parameter is set).
      2. The pipeline uses a floating ECR tag (latest or no tag).

    It does NOT fire based on ecrRoleArnToAssume in nextflow.config —
    that drives runtime auth, not build-time caching (Option 2 design).
    """

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _complete_npi(self):
        """Mock _npi_input_names to return a set WITH ECR slots."""
        return mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value={
                "ecr_role_arn_to_assume", "ecr_job_token_audience",
                "ecr_job_token_subject_claims", "repository_url", "cache_docker",
            },
        )

    def test_ecr_floating_tag_with_ecr_role_arg_raises(self):
        """Floating-tag ECR container + --ecr-role-arn flag → DXError before upload."""
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_LATEST}'
        """)
        with self._complete_npi():
            with self.assertRaises(dxpy.exceptions.DXError) as cm:
                preflight_validate_for_cache_docker(
                    src_dir=self._tmpdir,
                    ecr_role_arn=_ECR_ROLE,
                )
        msg = str(cm.exception)
        self.assertIn("floating", msg.lower())
        self.assertIn(_ECR_IMAGE_LATEST, msg)

    def test_ecr_floating_tag_profile_with_ecr_role_arg_raises(self):
        """Floating tag in active profile + --ecr-role-arn → DXError."""
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_PINNED}'
            profiles {{
                floating {{
                    process.container = '{_ECR_IMAGE_LATEST}'
                }}
            }}
        """)
        with self._complete_npi():
            with self.assertRaises(dxpy.exceptions.DXError) as cm:
                preflight_validate_for_cache_docker(
                    src_dir=self._tmpdir,
                    profile="floating",
                    ecr_role_arn=_ECR_ROLE,
                )
        self.assertIn(_ECR_IMAGE_LATEST, str(cm.exception))

    def test_pinned_ecr_container_with_ecr_role_arg_passes(self):
        """Digest-pinned ECR container + --ecr-role-arn → no error."""
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_PINNED}'
        """)
        with self._complete_npi():
            preflight_validate_for_cache_docker(
                src_dir=self._tmpdir,
                ecr_role_arn=_ECR_ROLE,
            )  # must not raise

    def test_no_ecr_role_arg_floating_tag_does_not_raise(self):
        """Floating ECR container but no --ecr-role-arn flag → guard does not fire.
        Without the flag, no build-time ECR auth is requested, so the floating-
        tag constraint is irrelevant."""
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_LATEST}'
        """)
        with mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value={"repository_url", "cache_docker"},
        ):
            preflight_validate_for_cache_docker(
                src_dir=self._tmpdir,
                ecr_role_arn=None,
            )  # must not raise

    def test_ecr_in_config_only_no_cli_flag_does_not_raise(self):
        """ecrRoleArnToAssume in nextflow.config without --ecr-role-arn CLI flag
        does NOT trigger the floating-tag guard — runtime config is not build-time
        intent (Option 2 design principle)."""
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_LATEST}'
            dnanexus {{
                ecrRoleArnToAssume = '{_ECR_ROLE}'
            }}
        """)
        with mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value={"repository_url", "cache_docker"},
        ):
            preflight_validate_for_cache_docker(
                src_dir=self._tmpdir,
                ecr_role_arn=None,  # flag not passed
            )  # must not raise

    def test_inactive_profile_floating_tag_does_not_raise(self):
        """Floating tag in a profile that is NOT active → no false positive."""
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_PINNED}'
            profiles {{
                floating {{
                    process.container = '{_ECR_IMAGE_LATEST}'
                }}
            }}
        """)
        with self._complete_npi():
            # No profile passed → default container (pinned) is active.
            preflight_validate_for_cache_docker(
                src_dir=self._tmpdir,
                ecr_role_arn=_ECR_ROLE,
            )  # must not raise

    def test_floating_tag_raises_before_ecr_slot_check_when_npi_lacks_ecr_inputs(self):
        """Floating-tag guard fires even when the deployed NPI does not yet
        declare ECR input slots.

        Ordering guarantee: the guard must reject user config errors (floating
        tags) BEFORE the infrastructure check (NPI slot availability).  This
        allows ``test_cache_docker_floating_tag_rejected`` to pass on environments
        where the NPI has not yet been upgraded to support ECR inputs.
        """
        _write_config(self._tmpdir, f"""
            process.container = '{_ECR_IMAGE_LATEST}'
        """)
        # NPI that does NOT declare ECR slots (simulates old NPI version).
        with mock.patch(
            "dxpy.nextflow.nextflow_builder._npi_input_names",
            return_value={"repository_url", "cache_docker"},  # no ECR slots
        ):
            with self.assertRaises(dxpy.exceptions.DXError) as cm:
                preflight_validate_for_cache_docker(
                    src_dir=self._tmpdir,
                    ecr_role_arn=_ECR_ROLE,
                )
        # The error must be the floating-tag rejection, NOT the ECR-slot-missing error.
        msg = str(cm.exception)
        self.assertIn("floating", msg.lower(), "Expected floating-tag error, got: " + msg)
        self.assertNotIn("does not declare", msg,
                         "Got ECR-slot-missing error instead of floating-tag error: " + msg)


if __name__ == "__main__":
    unittest.main()
