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
import subprocess
import unittest
from unittest.mock import patch, MagicMock

from parameterized import parameterized
from dxpy.nextflow.collect_images import (
    _parse_docker_ref,
    _resolve_digest,
    collect_docker_images,
    _populate_cached_file_ids,
)


class TestParseDockerRef(unittest.TestCase):
    """Unit tests for _parse_docker_ref()."""

    # ── Core registry format cases ─────────────────────────────────────
    @parameterized.expand([
        # (image_reference, expected_repository, expected_image, expected_tag, expected_digest)
        ("myregistryhost:5000/fedora/httpd:version1.0",
         "myregistryhost:5000/fedora/", "httpd", "version1.0", None),
        ("fedora/httpd:version1.0-alpha",
         "fedora/", "httpd", "version1.0-alpha", None),
        ("fedora/httpd:version1.0",
         "fedora/", "httpd", "version1.0", None),
        ("rabbit:3",
         None, "rabbit", "3", None),
        ("rabbit",
         None, "rabbit", None, None),
        ("repository/rabbit:3",
         "repository/", "rabbit", "3", None),
        ("repository/rabbit",
         "repository/", "rabbit", None, None),
        ("rabbit@sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d",
         None, "rabbit", None, "sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d"),
        ("repository/rabbit@sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d",
         "repository/", "rabbit", None, "sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d"),
    ])
    def test_core_cases(self, ref, exp_repo, exp_image, exp_tag, exp_digest):
        repo, image, tag, digest = _parse_docker_ref(ref)
        self.assertEqual(repo, exp_repo)
        self.assertEqual(image, exp_image)
        self.assertEqual(tag, exp_tag)
        self.assertEqual(digest, exp_digest)

    # ── Additional cases for nf-core / wave / multi-level registries ───
    @parameterized.expand([
        # quay.io biocontainers (common nf-core pattern)
        ("quay.io/biocontainers/fastqc:0.12.1--hdfd78af_0",
         "quay.io/biocontainers/", "fastqc", "0.12.1--hdfd78af_0", None),
        # wave container with short hash tag
        ("community.wave.seqera.io/library/star:5acb4e8c",
         "community.wave.seqera.io/library/", "star", "5acb4e8c", None),
        # plain image with tag
        ("ubuntu:20.04",
         None, "ubuntu", "20.04", None),
        # docker:// URI scheme (Singularity-style)
        ("docker://ubuntu:20.04",
         None, "ubuntu", "20.04", None),
        ("docker://quay.io/biocontainers/samtools:1.16.1",
         "quay.io/biocontainers/", "samtools", "1.16.1", None),
        # tag + digest
        ("busybox:1.36@sha256:abcdef1234567890",
         None, "busybox", "1.36", "sha256:abcdef1234567890"),
        # docker:// with digest
        ("docker://ubuntu@sha256:abc123",
         None, "ubuntu", None, "sha256:abc123"),
    ])
    def test_extended_cases(self, ref, exp_repo, exp_image, exp_tag, exp_digest):
        repo, image, tag, digest = _parse_docker_ref(ref)
        self.assertEqual(repo, exp_repo)
        self.assertEqual(image, exp_image)
        self.assertEqual(tag, exp_tag)
        self.assertEqual(digest, exp_digest)


class TestResolveDigest(unittest.TestCase):
    """Tests for _resolve_digest() with mocked subprocess."""

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_selects_amd64_from_multiarch(self, mock_run):
        """Multi-arch manifests: must pick amd64/linux, not arm64."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
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
            }),
        )
        result = _resolve_digest("quay.io/biocontainers/fastqc:0.12.1")
        self.assertEqual(result, "sha256:amd64digest")
        mock_run.assert_called_once_with(
            ["docker", "manifest", "inspect", "quay.io/biocontainers/fastqc:0.12.1"],
            capture_output=True, text=True,
        )

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_no_amd64_manifest(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "manifests": [
                    {
                        "digest": "sha256:arm64only",
                        "platform": {"architecture": "arm64", "os": "linux"},
                    },
                ]
            }),
        )
        result = _resolve_digest("some/image:latest")
        self.assertIsNone(result)

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_subprocess_failure(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1)
        result = _resolve_digest("nonexistent/image:1.0")
        self.assertIsNone(result)

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_invalid_json(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="not json")
        result = _resolve_digest("some/image:latest")
        self.assertIsNone(result)

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_empty_manifests_list(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"manifests": []}),
        )
        result = _resolve_digest("some/image:latest")
        self.assertIsNone(result)

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_null_manifests_value(self, mock_run):
        """If manifests key exists but is null, should return None."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"manifests": None}),
        )
        result = _resolve_digest("some/image:latest")
        self.assertIsNone(result)

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_single_arch_flat_manifest(self, mock_run):
        """Single-arch images return a flat manifest (no manifests array)."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {"digest": "sha256:configdigest"},
            }),
        )
        result = _resolve_digest("singlearch/image:1.0")
        self.assertIsNone(result)


class TestCollectDockerImages(unittest.TestCase):
    """Tests for collect_docker_images() with mocked subprocess."""

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images._resolve_digest")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_basic_inspect_output(self, mock_run, mock_resolve, mock_populate):
        """Tagged images should NOT trigger _resolve_digest."""
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
        self.assertIsNone(refs[0]["digest"])
        self.assertEqual(refs[1]["process"], "MULTIQC")
        self.assertEqual(refs[1]["image_name"], "multiqc")
        mock_resolve.assert_not_called()
        mock_populate.assert_called_once()

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images._resolve_digest")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_digest_not_resolved_when_present(self, mock_run, mock_resolve, mock_populate):
        """When @sha256: is already in the ref, _resolve_digest is not called."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "processes": [
                    {"name": "PROC", "container": "rabbit@sha256:abc123"},
                ]
            }),
        )
        refs = collect_docker_images("/tmp/pipeline", "", "")
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0]["digest"], "sha256:abc123")
        mock_resolve.assert_not_called()

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images._resolve_digest", return_value="sha256:resolved")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_digest_resolved_for_untagged_images(self, mock_run, mock_resolve, mock_populate):
        """Untagged images (implicit latest) should trigger _resolve_digest."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "processes": [
                    {"name": "PROC", "container": "repository/rabbit"},
                ]
            }),
        )
        refs = collect_docker_images("/tmp/pipeline", "", "")
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0]["digest"], "sha256:resolved")
        mock_resolve.assert_called_once_with("repository/rabbit")

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_empty_processes(self, mock_run, mock_populate):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"processes": []}),
        )
        refs = collect_docker_images("/tmp/pipeline", "", "")
        self.assertEqual(refs, [])
        mock_populate.assert_not_called()

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_processes_without_container(self, mock_run, mock_populate):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "processes": [
                    {"name": "LOCAL_PROC", "container": ""},
                    {"name": "FASTQC", "container": "biocontainers/fastqc:0.12.1"},
                ]
            }),
        )
        refs = collect_docker_images("/tmp/pipeline", "", "")
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0]["process"], "FASTQC")

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_inspect_failure_raises(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="some error"
        )
        from dxpy.nextflow.ImageRefFactory import ImageRefFactoryError
        with self.assertRaises(ImageRefFactoryError):
            collect_docker_images("/tmp/pipeline", "", "")

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_profile_and_params_passed(self, mock_run, mock_populate):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"processes": []}),
        )
        collect_docker_images("/tmp/pipeline", "docker,test", "--outdir /tmp/out")
        cmd = mock_run.call_args[0][0]
        self.assertIn("-profile", cmd)
        self.assertIn("docker,test", cmd)
        self.assertIn("--outdir", cmd)
        self.assertIn("/tmp/out", cmd)

    def test_malformed_params_raises(self):
        """Unbalanced quotes in pipeline params should raise, not crash."""
        from dxpy.nextflow.ImageRefFactory import ImageRefFactoryError
        with self.assertRaises(ImageRefFactoryError) as ctx:
            collect_docker_images("/tmp/pipeline", "", '--input "unclosed')
        self.assertIn("Malformed pipeline parameters", str(ctx.exception))


class TestPopulateCachedFileIds(unittest.TestCase):
    """Tests for _populate_cached_file_ids() with mocked DX API."""

    @patch("dxpy.nextflow.collect_images.config")
    def test_no_project_context(self, mock_config):
        mock_config.get.return_value = None
        refs = [{"repository": "", "image_name": "busybox", "tag": "1.36", "digest": "", "file_id": None}]
        _populate_cached_file_ids(refs)
        self.assertIsNone(refs[0]["file_id"])

    @patch("dxpy.nextflow.collect_images.find_data_objects")
    @patch("dxpy.nextflow.collect_images.config")
    def test_cache_hit(self, mock_config, mock_find):
        mock_config.get.return_value = "project-123"
        mock_find.return_value = iter([{
            "id": "file-AAAA",
            "describe": {"properties": {"image_digest": "sha256:abc123"}},
        }])
        refs = [{"repository": "quay.io/bio/", "image_name": "fastqc", "tag": "0.12.1",
                 "digest": "", "file_id": None}]
        _populate_cached_file_ids(refs)
        self.assertEqual(refs[0]["file_id"], "file-AAAA")

    @patch("dxpy.nextflow.collect_images.find_data_objects")
    @patch("dxpy.nextflow.collect_images.config")
    def test_cache_digest_mismatch(self, mock_config, mock_find):
        mock_config.get.return_value = "project-123"
        mock_find.return_value = iter([{
            "id": "file-AAAA",
            "describe": {"properties": {"image_digest": "sha256:wrong"}},
        }])
        refs = [{"repository": "", "image_name": "busybox", "tag": "1.36",
                 "digest": "sha256:correct", "file_id": None}]
        _populate_cached_file_ids(refs)
        self.assertIsNone(refs[0]["file_id"])

    @patch("dxpy.nextflow.collect_images.find_data_objects")
    @patch("dxpy.nextflow.collect_images.config")
    def test_dedup_by_repository(self, mock_config, mock_find):
        """Two refs with same image_name/tag but different repository should
        NOT share cache lookups."""
        mock_config.get.return_value = "project-123"
        mock_find.side_effect = [iter([]), iter([])]
        refs = [
            {"repository": "quay.io/bio/", "image_name": "samtools", "tag": "1.16",
             "digest": "", "file_id": None},
            {"repository": "docker.io/lib/", "image_name": "samtools", "tag": "1.16",
             "digest": "", "file_id": None},
        ]
        _populate_cached_file_ids(refs)
        # find_data_objects called twice (once per unique key)
        self.assertEqual(mock_find.call_count, 2)

    @patch("dxpy.nextflow.collect_images.find_data_objects")
    @patch("dxpy.nextflow.collect_images.config")
    def test_cache_hit_applies_to_all_matching_refs(self, mock_config, mock_find):
        """Cache hit for (repo, image, tag) should apply to ALL refs with
        that key, not just the first one."""
        mock_config.get.return_value = "project-123"
        mock_find.return_value = iter([{
            "id": "file-BBBB",
            "describe": {"properties": {"image_digest": "sha256:xyz"}},
        }])
        refs = [
            {"repository": "quay.io/bio/", "image_name": "fastqc", "tag": "0.12",
             "digest": "", "file_id": None, "process": "PROC_A"},
            {"repository": "quay.io/bio/", "image_name": "fastqc", "tag": "0.12",
             "digest": "", "file_id": None, "process": "PROC_B"},
        ]
        _populate_cached_file_ids(refs)
        self.assertEqual(refs[0]["file_id"], "file-BBBB")
        self.assertEqual(refs[1]["file_id"], "file-BBBB")

    @patch("dxpy.nextflow.collect_images.find_data_objects")
    @patch("dxpy.nextflow.collect_images.config")
    def test_cache_lookup_untagged_image(self, mock_config, mock_find):
        """Untagged images use bare image_name as cache key (no trailing _)."""
        mock_config.get.return_value = "project-123"
        mock_find.return_value = iter([{
            "id": "file-UNTAGGED",
            "describe": {"properties": {"image_digest": "sha256:latestdigest"}},
        }])
        refs = [{"repository": "library/", "image_name": "ubuntu", "tag": "",
                 "digest": "", "file_id": None}]
        _populate_cached_file_ids(refs)
        self.assertEqual(refs[0]["file_id"], "file-UNTAGGED")
        # Verify the cache file name is just "ubuntu", not "ubuntu_"
        call_kwargs = mock_find.call_args[1]
        self.assertEqual(call_kwargs["name"], "ubuntu")
        self.assertEqual(call_kwargs["folder"], "/.cached_docker_images/ubuntu/")

    @patch("dxpy.nextflow.collect_images.find_data_objects")
    @patch("dxpy.nextflow.collect_images.config")
    def test_find_data_objects_exception_graceful(self, mock_config, mock_find):
        """If find_data_objects raises, the image is skipped (file_id stays None)."""
        mock_config.get.return_value = "project-123"
        mock_find.side_effect = Exception("network timeout")
        refs = [{"repository": "", "image_name": "busybox", "tag": "1.36",
                 "digest": "", "file_id": None}]
        _populate_cached_file_ids(refs)
        self.assertIsNone(refs[0]["file_id"])

    @patch("dxpy.nextflow.collect_images.find_data_objects")
    @patch("dxpy.nextflow.collect_images.config")
    def test_cache_hit_no_describe_key(self, mock_config, mock_find):
        """Cached file without image_digest property is not trustworthy, skip it."""
        mock_config.get.return_value = "project-123"
        mock_find.return_value = iter([{"id": "file-NODESC"}])
        refs = [{"repository": "", "image_name": "alpine", "tag": "3.18",
                 "digest": "", "file_id": None}]
        _populate_cached_file_ids(refs)
        self.assertIsNone(refs[0]["file_id"])


class TestCollectDockerImagesExtended(unittest.TestCase):
    """Additional edge-case tests for collect_docker_images()."""

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_invalid_json_with_rc0_raises(self, mock_run, mock_populate):
        """nextflow inspect can emit warnings before JSON; rc=0 but bad stdout."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="WARN: some deprecation notice\n{not valid json",
        )
        from dxpy.nextflow.ImageRefFactory import ImageRefFactoryError
        with self.assertRaises(ImageRefFactoryError) as ctx:
            collect_docker_images("/tmp/pipeline", "", "")
        self.assertIn("Failed to parse", str(ctx.exception))
        mock_populate.assert_not_called()

    @patch("dxpy.nextflow.collect_images._populate_cached_file_ids")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_missing_processes_key(self, mock_run, mock_populate):
        """If NF output schema changes and 'processes' key is absent, return empty."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"containers": ["ubuntu:20.04"]}),
        )
        refs = collect_docker_images("/tmp/pipeline", "", "")
        self.assertEqual(refs, [])
        mock_populate.assert_not_called()

    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_tag_and_digest_rejected(self, mock_run):
        """Image refs with both tag and digest should raise."""
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
    @patch("dxpy.nextflow.collect_images._resolve_digest")
    @patch("dxpy.nextflow.collect_images.subprocess.run")
    def test_duplicate_containers_not_deduped(self, mock_run, mock_resolve, mock_populate):
        """Multiple processes sharing the same image should each get an entry.
        Dedup is _populate_cached_file_ids' job, not collect_docker_images'."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "processes": [
                    {"name": "FASTQC_RAW", "container": "quay.io/biocontainers/fastqc:0.12.1"},
                    {"name": "FASTQC_TRIMMED", "container": "quay.io/biocontainers/fastqc:0.12.1"},
                    {"name": "MULTIQC", "container": "quay.io/biocontainers/multiqc:1.14"},
                ]
            }),
        )
        refs = collect_docker_images("/tmp/pipeline", "docker", "")
        self.assertEqual(len(refs), 3)
        self.assertEqual(refs[0]["process"], "FASTQC_RAW")
        self.assertEqual(refs[1]["process"], "FASTQC_TRIMMED")
        self.assertEqual(refs[0]["image_name"], "fastqc")
        self.assertEqual(refs[1]["image_name"], "fastqc")
        self.assertEqual(refs[2]["process"], "MULTIQC")
        mock_resolve.assert_not_called()


class TestParseDockerRefEdgeCases(unittest.TestCase):
    """Edge cases for _parse_docker_ref not covered by parity tests."""

    def test_empty_string(self):
        repo, image, tag, digest = _parse_docker_ref("")
        self.assertIsNone(repo)
        self.assertEqual(image, "")
        self.assertIsNone(tag)
        self.assertIsNone(digest)

    def test_three_level_registry(self):
        """gcr.io/google-containers/cadvisor:v0.36.0 — 3 path segments."""
        repo, image, tag, digest = _parse_docker_ref(
            "gcr.io/google-containers/cadvisor:v0.36.0")
        self.assertEqual(repo, "gcr.io/google-containers/")
        self.assertEqual(image, "cadvisor")
        self.assertEqual(tag, "v0.36.0")
        self.assertIsNone(digest)


if __name__ == "__main__":
    unittest.main()
