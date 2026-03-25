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
        mock_resolve.assert_called_once_with("repository/rabbit")

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


if __name__ == "__main__":
    unittest.main()
