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

import json
import unittest
from unittest.mock import patch, MagicMock

from parameterized import parameterized
from dxpy.nextflow.collect_images import (
    _ImageRef,
    _docker_manifest_inspect,
    _parse_docker_ref,
    _parse_dx_uri,
    _resolve_digest,
    collect_docker_images,
    _populate_cached_file_ids,
)


class TestParseDockerRef(unittest.TestCase):
    """Unit tests for _parse_docker_ref()."""

    @parameterized.expand([
        # host:port/path/image:tag — port vs tag disambiguation
        ("myregistryhost:5000/fedora/httpd:version1.0",
         "myregistryhost:5000/fedora/", "httpd", "version1.0", None),
        # bare image:tag
        ("rabbit:3",
         None, "rabbit", "3", None),
        # bare image, no tag
        ("rabbit",
         None, "rabbit", None, None),
        # real nf-core pattern with complex tag
        ("quay.io/biocontainers/fastqc:0.12.1--hdfd78af_0",
         "quay.io/biocontainers/", "fastqc", "0.12.1--hdfd78af_0", None),
        # digest-only
        ("rabbit@sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d",
         None, "rabbit", None, "sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d"),
        # digest-only with repository
        ("repository/rabbit@sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d",
         "repository/", "rabbit", None, "sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d"),
        # docker:// URI scheme (Singularity-style)
        ("docker://ubuntu:20.04",
         None, "ubuntu", "20.04", None),
        # tag + digest (both present)
        ("busybox:1.36@sha256:abcdef1234567890",
         None, "busybox", "1.36", "sha256:abcdef1234567890"),
    ])
    def test_parse(self, ref, exp_repo, exp_image, exp_tag, exp_digest):
        repo, image, tag, digest = _parse_docker_ref(ref)
        self.assertEqual(repo, exp_repo)
        self.assertEqual(image, exp_image)
        self.assertEqual(tag, exp_tag)
        self.assertEqual(digest, exp_digest)


class TestResolveDigest(unittest.TestCase):
    """Tests for _resolve_digest() with mocked subprocess."""

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_multiarch_resolves_config_digest(self, mock_run):
        """Multi-arch: two calls — manifest list then platform manifest for config digest."""
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout=json.dumps({
                "manifests": [
                    {
                        "digest": "sha256:arm64digest",
                        "platform": {"architecture": "arm64", "os": "linux"},
                    },
                    {
                        "digest": "sha256:amd64digest",
                        "platform": {"architecture": "amd64", "os": "linux"},
                    },
                ]
            })),
            MagicMock(returncode=0, stdout=json.dumps({
                "schemaVersion": 2,
                "config": {"digest": "sha256:amd64configdigest"},
            })),
        ]
        result = _resolve_digest("quay.io/biocontainers/fastqc:0.12.1")
        self.assertEqual(result, "sha256:amd64configdigest")
        self.assertEqual(mock_run.call_count, 2)
        second_call_args = mock_run.call_args_list[1][0][0]
        self.assertIn("sha256:amd64digest", second_call_args[-1])

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_single_arch_flat_manifest_returns_config_digest(self, mock_run):
        """Single-arch images: fall back to config digest."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "schemaVersion": 2,
                "config": {"digest": "sha256:configdigest"},
            }),
        )
        result = _resolve_digest("singlearch/image:1.0")
        self.assertEqual(result, "sha256:configdigest")

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_multiarch_returns_manifest_digest_when_requested(self, mock_run):
        """Multi-arch with use_manifest_digest=True: single call, returns platform manifest digest."""
        mock_run.return_value = MagicMock(returncode=0, stdout=json.dumps({
            "manifests": [
                {
                    "digest": "sha256:arm64digest",
                    "platform": {"architecture": "arm64", "os": "linux"},
                },
                {
                    "digest": "sha256:amd64digest",
                    "platform": {"architecture": "amd64", "os": "linux"},
                },
            ]
        }))
        result = _resolve_digest("quay.io/nextflow/bash", use_manifest_digest=True)
        self.assertEqual(result, "sha256:amd64digest")
        self.assertEqual(mock_run.call_count, 1)

    @patch("dxpy.nextflow.collect_images.time.sleep")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_retries_on_transient_failure(self, mock_run, mock_sleep):
        """Transient failure followed by success — should retry and succeed."""
        mock_run.side_effect = [
            MagicMock(returncode=1, stdout="", stderr="timeout"),
            MagicMock(returncode=0, stdout=json.dumps({
                "schemaVersion": 2,
                "config": {"digest": "sha256:recovered"},
            })),
        ]
        result = _docker_manifest_inspect("quay.io/bio/samtools:1.17")
        self.assertEqual(result["config"]["digest"], "sha256:recovered")
        self.assertEqual(mock_run.call_count, 2)
        mock_sleep.assert_called_once_with(60)


class TestCollectDockerImages(unittest.TestCase):
    """Tests for collect_docker_images() with mocked subprocess."""

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images._resolve_digest", return_value="sha256:resolved")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_basic_inspect_output(self, mock_run, mock_resolve, mock_populate):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "processes": [
                    {"name": "FASTQC", "container": "quay.io/biocontainers/fastqc:0.12.1"},
                    {"name": "MULTIQC", "container": "quay.io/biocontainers/multiqc:1.14"},
                ]
            }),
        )
        refs = collect_docker_images("/tmp/pipeline", "docker", "")
        self.assertEqual(len(refs), 2)
        self.assertEqual(refs[0]["process"], "FASTQC")
        self.assertEqual(refs[0]["repository"], "quay.io/biocontainers/")
        self.assertEqual(refs[0]["image_name"], "fastqc")
        self.assertEqual(refs[0]["tag"], "0.12.1")
        self.assertEqual(refs[0]["digest"], "sha256:resolved")
        self.assertEqual(mock_resolve.call_count, 2)
        mock_populate.assert_called_once()

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images._resolve_digest")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_digest_not_resolved_when_present(self, mock_run, mock_resolve, mock_populate):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "processes": [
                    {"name": "PROC", "container": "rabbit@sha256:abc123"},
                ]
            }),
        )
        refs = collect_docker_images("/tmp/pipeline", "", "")
        self.assertEqual(refs[0]["digest"], "sha256:abc123")
        mock_resolve.assert_not_called()

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images._resolve_digest", return_value="sha256:resolved")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_digest_resolved_for_untagged_images(self, mock_run, mock_resolve, mock_populate):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "processes": [
                    {"name": "PROC", "container": "repository/rabbit"},
                ]
            }),
        )
        refs = collect_docker_images("/tmp/pipeline", "", "")
        self.assertEqual(refs[0]["digest"], "sha256:resolved")
        mock_resolve.assert_called_once_with("repository/rabbit", use_manifest_digest=False)

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images._resolve_digest", return_value="sha256:resolved")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_manifest_digest_only_for_latest_and_untagged(self, mock_run, mock_resolve, mock_populate):
        """use_manifest_digest=True only applies to latest/untagged; tagged images always get config digest."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "processes": [
                    {"name": "TAGGED", "container": "quay.io/bio/fastqc:0.12.1"},
                    {"name": "LATEST", "container": "quay.io/bio/bash:latest"},
                    {"name": "UNTAGGED", "container": "quay.io/bio/samtools"},
                ]
            }),
        )
        collect_docker_images("/tmp/pipeline", "", "", use_manifest_digest=True)
        calls = mock_resolve.call_args_list
        self.assertEqual(len(calls), 3)
        # Tagged image -> use_manifest_digest=False (scope guard)
        self.assertEqual(calls[0], unittest.mock.call("quay.io/bio/fastqc:0.12.1", use_manifest_digest=False))
        # Explicit :latest -> use_manifest_digest=True
        self.assertEqual(calls[1], unittest.mock.call("quay.io/bio/bash:latest", use_manifest_digest=True))
        # Untagged -> use_manifest_digest=True
        self.assertEqual(calls[2], unittest.mock.call("quay.io/bio/samtools", use_manifest_digest=True))

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_inspect_failure_raises(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
        from dxpy.nextflow.ImageRefFactory import ImageRefFactoryError
        with self.assertRaises(ImageRefFactoryError):
            collect_docker_images("/tmp/pipeline", "", "")

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_tag_and_digest_rejected(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "processes": [
                    {"name": "PROC", "container": "busybox:1.36@sha256:abcdef1234567890"},
                ]
            }),
        )
        from dxpy.nextflow.ImageRefFactory import ImageRefFactoryError
        with self.assertRaises(ImageRefFactoryError) as ctx:
            collect_docker_images("/tmp/pipeline", "", "")
        self.assertIn("both tag and digest", str(ctx.exception))

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_invalid_json_with_rc0_raises(self, mock_run, mock_populate):
        """nextflow inspect can emit warnings before JSON."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="WARN: some deprecation notice\n{not valid json",
        )
        from dxpy.nextflow.ImageRefFactory import ImageRefFactoryError
        with self.assertRaises(ImageRefFactoryError):
            collect_docker_images("/tmp/pipeline", "", "")

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images._resolve_digest")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_dx_uri_passes_through_with_file_id(self, mock_run, mock_resolve, mock_populate):
        """dx:// URIs set file_id directly, skipping pull/save/upload."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "processes": [
                    {"name": "PROC", "container": "dx://project-xxxx:file-yyyy"},
                ]
            }),
        )
        refs = collect_docker_images("/tmp/pipeline", "", "")
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0]["file_id"], "file-yyyy")
        self.assertIsNone(refs[0]["repository"])
        self.assertEqual(refs[0]["image_name"], "file-yyyy")
        mock_resolve.assert_not_called()


class TestParseDxUri(unittest.TestCase):
    """Unit tests for _parse_dx_uri()."""

    def test_file_only(self):
        project_id, file_id = _parse_dx_uri("dx://file-xxxx")
        self.assertIsNone(project_id)
        self.assertEqual(file_id, "file-xxxx")

    def test_project_and_file(self):
        project_id, file_id = _parse_dx_uri("dx://project-AAAA:file-BBBB")
        self.assertEqual(project_id, "project-AAAA")
        self.assertEqual(file_id, "file-BBBB")


class TestPopulateCachedFileIds(unittest.TestCase):
    """Tests for _populate_cached_file_ids() with mocked DX API."""

    @patch("dxpy.nextflow.collect_images.find_data_objects")
    @patch("dxpy.nextflow.collect_images.config")
    def test_cache_hit(self, mock_config, mock_find):
        mock_config.get.return_value = "project-123"
        mock_find.return_value = iter([{"id": "file-AAAA", "describe": {"name": "test-name", "project": "project-123", "folder": "/dir"}}])
        refs = [_ImageRef(process="", repository="quay.io/bio/", image_name="fastqc",
                          tag="0.12.1", digest="sha256:abc123", file_id=None, engine="docker")]
        _populate_cached_file_ids(refs)
        self.assertEqual(refs[0].file_id, "file-AAAA")
        call_kwargs = mock_find.call_args[1]
        self.assertEqual(call_kwargs["properties"], {"image_digest": "sha256:abc123"})

    @patch("dxpy.nextflow.collect_images.find_data_objects")
    @patch("dxpy.nextflow.collect_images.config")
    def test_duplicate_refs_each_get_cache_hit(self, mock_config, mock_find):
        mock_config.get.return_value = "project-123"
        mock_find.side_effect = [
            iter([{"id": "file-BBBB", "describe": {"name": "test-name", "project": "project-123", "folder": "/dir"}}]),
            iter([{"id": "file-BBBB", "describe": {"name": "test-name", "project": "project-123", "folder": "/dir"}}]),
        ]
        refs = [
            _ImageRef(process="PROC_A", repository="quay.io/bio/", image_name="fastqc",
                      tag="0.12", digest="sha256:xyz", file_id=None, engine="docker"),
            _ImageRef(process="PROC_B", repository="quay.io/bio/", image_name="fastqc",
                      tag="0.12", digest="sha256:xyz", file_id=None, engine="docker"),
        ]
        _populate_cached_file_ids(refs)
        self.assertEqual(refs[0].file_id, "file-BBBB")
        self.assertEqual(refs[1].file_id, "file-BBBB")
        self.assertEqual(mock_find.call_count, 2)

    @patch("dxpy.nextflow.collect_images.find_data_objects")
    @patch("dxpy.nextflow.collect_images.config")
    def test_cache_skipped_when_digest_is_none(self, mock_config, mock_find):
        mock_config.get.return_value = "project-123"
        refs = [_ImageRef(process="", repository="quay.io/bio/", image_name="singlearch",
                          tag="1.0", digest=None, file_id=None, engine="docker")]
        _populate_cached_file_ids(refs)
        self.assertIsNone(refs[0].file_id)
        mock_find.assert_not_called()

    @patch("dxpy.nextflow.collect_images.find_data_objects")
    @patch("dxpy.nextflow.collect_images.config")
    def test_cross_registry_collision_prevented(self, mock_config, mock_find):
        mock_config.get.return_value = "project-123"
        mock_find.side_effect = [
            iter([{"id": "file-FROM-QUAY", "describe": {"name": "test-name", "project": "project-123", "folder": "/dir"}}]),
            iter([]),
        ]
        refs = [
            _ImageRef(process="PROC_A", repository="quay.io/bio/", image_name="samtools",
                      tag="1.17", digest="sha256:aaa", file_id=None, engine="docker"),
            _ImageRef(process="PROC_B", repository="dockerhub/", image_name="samtools",
                      tag="1.17", digest="sha256:bbb", file_id=None, engine="docker"),
        ]
        _populate_cached_file_ids(refs)
        self.assertEqual(refs[0].file_id, "file-FROM-QUAY")
        self.assertIsNone(refs[1].file_id)
        self.assertEqual(mock_find.call_count, 2)


class TestReconstructImageRef(unittest.TestCase):
    """Tests for DockerImageRef._reconstruct_image_ref()."""

    def _make_ref(self, tag=None, digest=None, repository=None, image_name="samtools",
                  digest_is_original=False):
        from dxpy.nextflow.ImageRef import DockerImageRef
        return DockerImageRef(
            process="PROC", digest=digest, repository=repository,
            image_name=image_name, tag=tag, digest_is_original=digest_is_original,
        )

    def test_tag_only(self):
        ref = self._make_ref(tag="1.17", repository="quay.io/bio/")
        self.assertEqual(ref._reconstruct_image_ref(), "quay.io/bio/samtools:1.17")

    def test_tag_and_digest_prefers_tag(self):
        ref = self._make_ref(tag="1.17", digest="sha256:abc", repository="quay.io/bio/",
                             digest_is_original=True)
        self.assertEqual(ref._reconstruct_image_ref(), "quay.io/bio/samtools:1.17")

    def test_digest_only_original(self):
        """Original @sha256: digest is used in pull command."""
        ref = self._make_ref(digest="sha256:abc", repository="quay.io/bio/",
                             digest_is_original=True)
        self.assertEqual(ref._reconstruct_image_ref(), "quay.io/bio/samtools@sha256:abc")

    def test_digest_only_resolved(self):
        """Resolved config digest is NOT used in pull command — bare name instead."""
        ref = self._make_ref(digest="sha256:abc", repository="quay.io/bio/",
                             digest_is_original=False)
        self.assertEqual(ref._reconstruct_image_ref(), "quay.io/bio/samtools")


class TestDockerImageRefEcrFailLoud(unittest.TestCase):
    """Regression test for the BUG-1 fail-loud branch in DockerImageRef._cache.
    When `ensure_ecr_login_for_image` returns False on a confirmed ECR image,
    `_cache` must call err_exit with a message naming the host — not silently
    proceed to `sudo docker pull` and let it surface a generic registry error.
    """

    def setUp(self):
        from dxpy.nextflow import collect_images
        collect_images._ECR_LOGGED_IN_HOSTS.clear()

    def _make_ecr_ref(self):
        from dxpy.nextflow.ImageRef import DockerImageRef
        return DockerImageRef(
            process="P", digest=None,
            repository="123.dkr.ecr.us-east-1.amazonaws.com/",
            image_name="repo", tag="latest",
            digest_is_original=False,
        )

    @patch("dxpy.nextflow.collect_images.ensure_ecr_login_for_image")
    def test_cache_raises_when_ecr_login_fails(self, mock_login):
        import io
        import sys
        from dxpy.exceptions import DXCLIError
        mock_login.return_value = False  # simulate ECR auth setup failure
        ref = self._make_ecr_ref()
        # err_exit prints to stderr and raises SystemExit / DXCLIError.
        captured = io.StringIO()
        original = sys.stderr
        sys.stderr = captured
        try:
            with self.assertRaises((DXCLIError, SystemExit)):
                ref._cache("/tmp/dummy.tar.gz")
        finally:
            sys.stderr = original
        # The error message must name the ECR host so the user can debug.
        text = captured.getvalue()
        self.assertIn("123.dkr.ecr.us-east-1.amazonaws.com", text)
        self.assertIn("ECR authentication failed", text)

    @patch("dxpy.nextflow.collect_images.ensure_ecr_login_for_image")
    @patch("dxpy.nextflow.ImageRef.subprocess.check_output")
    @patch("dxpy.nextflow.ImageRef.upload_local_file")
    def test_cache_proceeds_when_ecr_login_ok(self, mock_upload, mock_subproc, mock_login):
        """The fail-loud branch must NOT trigger when login succeeded."""
        mock_login.return_value = True
        mock_subproc.return_value = b""
        mock_upload.return_value = MagicMock(get_id=lambda: "file-XYZ")
        ref = self._make_ecr_ref()
        ref._digest = "sha256:abc"  # avoid the digest_cmd path
        result = ref._cache("/tmp/dummy.tar.gz")
        self.assertEqual(result, "file-XYZ")


class TestEcrHostExtraction(unittest.TestCase):
    """Tests for _extract_ecr_host_and_region — must match the bash-side
    is_ecr_host helper in nextaur's DxBashLib.groovy."""

    @parameterized.expand([
        # Commercial AWS partition — accepted.
        ("123456789012.dkr.ecr.us-east-1.amazonaws.com/myrepo:latest",
         "123456789012.dkr.ecr.us-east-1.amazonaws.com", "us-east-1"),
        ("999999999999.dkr.ecr.eu-west-2.amazonaws.com/foo/bar@sha256:abc",
         "999999999999.dkr.ecr.eu-west-2.amazonaws.com", "eu-west-2"),
        ("1.dkr.ecr.ap-northeast-3.amazonaws.com/r:t",
         "1.dkr.ecr.ap-northeast-3.amazonaws.com", "ap-northeast-3"),
        # Mixed case host — must be lowercased before matching.
        ("123.DKR.ECR.US-EAST-1.AMAZONAWS.COM/repo",
         "123.dkr.ecr.us-east-1.amazonaws.com", "us-east-1"),
        # Non-ECR — public registries.
        ("quay.io/biocontainers/fastqc:1.0", None, None),
        ("docker.io/library/ubuntu:22.04", None, None),
        ("ubuntu:22.04", None, None),
        # Excluded partitions — GovCloud, Secret, China.
        ("123.dkr.ecr.us-gov-west-1.amazonaws.com/r:t", None, None),
        ("123.dkr.ecr.us-iso-east-1.amazonaws.com/r:t", None, None),
        ("123.dkr.ecr.us-isob-east-1.amazonaws.com/r:t", None, None),
        # China partition has the .cn TLD — regex anchor rejects.
        ("123456789012.dkr.ecr.cn-north-1.amazonaws.com.cn/r:t", None, None),
        # Empty / None.
        ("", None, None),
        (None, None, None),
        # Almost-ECR but malformed (alpha account id, missing region segment).
        ("abc.dkr.ecr.us-east-1.amazonaws.com/r:t", None, None),
        ("123.dkr.ecr.amazonaws.com/r:t", None, None),
    ])
    def test_extract(self, ref, exp_host, exp_region):
        from dxpy.nextflow.collect_images import _extract_ecr_host_and_region
        host, region = _extract_ecr_host_and_region(ref)
        self.assertEqual(host, exp_host)
        self.assertEqual(region, exp_region)


class TestEcrDockerLogin(unittest.TestCase):
    """Tests for _ecr_docker_login — verifies command construction around
    `aws ecr get-login-password | docker login` and the per-(host,region)
    cache. The actual aws/docker calls are mocked.
    """

    def setUp(self):
        # The login cache is module-global; reset for each test so test order
        # does not affect outcomes.
        from dxpy.nextflow import collect_images
        collect_images._ECR_LOGGED_IN_HOSTS.clear()

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_login_success_invokes_aws_then_docker_then_sudo_docker(self, mock_run):
        """Happy path: aws get-login-password, then docker login, then sudo docker login (mirror)."""
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="dummy-12h-token\n", stderr=""),  # aws
            MagicMock(returncode=0, stdout="Login Succeeded", stderr=""),    # docker login
            MagicMock(returncode=0, stdout="Login Succeeded", stderr=""),    # sudo docker login
        ]
        from dxpy.nextflow.collect_images import _ecr_docker_login
        ok = _ecr_docker_login("123.dkr.ecr.us-east-1.amazonaws.com", "us-east-1")
        self.assertTrue(ok)
        aws_call, docker_call, sudo_call = mock_run.call_args_list
        # aws: --profile ecr ecr get-login-password (no --region — profile drives region)
        self.assertEqual(
            aws_call[0][0],
            ["aws", "--profile", "ecr", "ecr", "get-login-password"],
        )
        # docker: login --username AWS --password-stdin <host>
        self.assertEqual(
            docker_call[0][0],
            ["docker", "login", "--username", "AWS", "--password-stdin",
             "123.dkr.ecr.us-east-1.amazonaws.com"],
        )
        self.assertEqual(docker_call[1]["input"], "dummy-12h-token\n")
        # sudo mirror — same args with `sudo -n` prefix.
        self.assertEqual(
            sudo_call[0][0],
            ["sudo", "-n", "docker", "login", "--username", "AWS", "--password-stdin",
             "123.dkr.ecr.us-east-1.amazonaws.com"],
        )

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_aws_failure_returns_false_no_docker_call(self, mock_run):
        """If `aws ecr get-login-password` fails, do not call docker login."""
        mock_run.return_value = MagicMock(returncode=255, stdout="", stderr="profile not found")
        from dxpy.nextflow.collect_images import _ecr_docker_login
        ok = _ecr_docker_login("123.dkr.ecr.us-east-1.amazonaws.com", "us-east-1")
        self.assertFalse(ok)
        self.assertEqual(mock_run.call_count, 1)

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_sudo_docker_login_failure_returns_false(self, mock_run):
        """sudo mirror failing must fail the whole login — otherwise sudo
        docker pull later fails with a confusing anonymous-access error."""
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="token\n", stderr=""),
            MagicMock(returncode=0, stdout="Login Succeeded", stderr=""),
            MagicMock(returncode=1, stdout="", stderr="sudo: a password is required"),
        ]
        from dxpy.nextflow.collect_images import _ecr_docker_login
        ok = _ecr_docker_login("123.dkr.ecr.us-east-1.amazonaws.com", "us-east-1")
        self.assertFalse(ok)

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_cache_hit_skips_aws_and_docker(self, mock_run):
        """A second login for the same (host,region) must not re-call aws or docker."""
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="t", stderr=""),
            MagicMock(returncode=0, stdout="ok", stderr=""),
            MagicMock(returncode=0, stdout="ok", stderr=""),
        ]
        from dxpy.nextflow.collect_images import _ecr_docker_login
        host, region = "123.dkr.ecr.us-east-1.amazonaws.com", "us-east-1"
        self.assertTrue(_ecr_docker_login(host, region))
        self.assertEqual(mock_run.call_count, 3)
        self.assertTrue(_ecr_docker_login(host, region))
        self.assertEqual(mock_run.call_count, 3)


class TestEnsureEcrLoginForImage(unittest.TestCase):
    """Public entry point — must be a no-op for non-ECR images."""

    def setUp(self):
        from dxpy.nextflow import collect_images
        collect_images._ECR_LOGGED_IN_HOSTS.clear()

    @patch("dxpy.nextflow.collect_images._ecr_docker_login")
    def test_noop_for_non_ecr(self, mock_login):
        from dxpy.nextflow.collect_images import ensure_ecr_login_for_image
        self.assertTrue(ensure_ecr_login_for_image("quay.io/biocontainers/fastqc:1.0"))
        self.assertTrue(ensure_ecr_login_for_image("docker.io/library/ubuntu"))
        self.assertTrue(ensure_ecr_login_for_image(""))
        mock_login.assert_not_called()

    @patch("dxpy.nextflow.collect_images._ecr_docker_login")
    def test_calls_login_for_ecr(self, mock_login):
        mock_login.return_value = True
        from dxpy.nextflow.collect_images import ensure_ecr_login_for_image
        ok = ensure_ecr_login_for_image("123.dkr.ecr.us-east-1.amazonaws.com/foo:bar")
        self.assertTrue(ok)
        mock_login.assert_called_once_with(
            "123.dkr.ecr.us-east-1.amazonaws.com", "us-east-1"
        )


if __name__ == "__main__":
    unittest.main()
