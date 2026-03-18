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

import dataclasses
import json
import logging
import shlex
import subprocess
from typing import Optional

from dxpy import config, find_data_objects
from dxpy.nextflow.ImageRefFactory import ImageRefFactory, ImageRefFactoryError

log = logging.getLogger(__name__)


@dataclasses.dataclass
class _ImageRef:
    """Internal representation of a container image reference."""
    process: str
    repository: Optional[str]
    image_name: str
    tag: Optional[str]
    digest: Optional[str]
    file_id: Optional[str]
    engine: str


def bundle_docker_images(image_refs):
    """
    :param image_refs: Image references extracted from collect_docker_images().
    :type image_refs: Dict
    :returns: Array of dicts for bundledDepends attribute of the applet resources. Also saves images on the platform
    if not done that before.
    """
    image_factories = [ImageRefFactory(x) for x in image_refs]
    images = [x.get_image() for x in image_factories]
    seen_images = set()
    bundled_depends = []
    for image in images:
        if image.identifier in seen_images:
            continue
        else:
            bundled_depends.append(image.bundled_depends.copy())
            seen_images.add(image.identifier)
    return bundled_depends


def _parse_docker_ref(ref):
    """Parse a Docker image reference into (repository, image_name, tag, digest).

    ``repository + image_name`` reconstructs the full image path.
    ``image_name`` is always the **last** path component (no slashes) so it
    can be used safely as a local filename by ``ImageRef._construct_cache_file_name()``.

    Examples:
        quay.io/biocontainers/fastqc:0.12.1
          -> ("quay.io/biocontainers/", "fastqc", "0.12.1", "")
        community.wave.seqera.io/library/star:5acb4e8c
          -> ("community.wave.seqera.io/library/", "star", "5acb4e8c", "")
        ubuntu:20.04
          -> ("", "ubuntu", "20.04", "")
        myregistry:5000/myimage:latest
          -> ("myregistry:5000/", "myimage", "latest", "")
    """
    digest = None
    tag = None

    # Strip docker:// URI scheme if present
    if ref.startswith("docker://"):
        ref = ref[len("docker://"):]

    # Split off @sha256:... digest
    if "@sha256:" in ref:
        ref, digest = ref.rsplit("@", 1)

    # Split off :tag — but not :port in the registry host.
    # The tag is after the last colon; if there is a / after that colon
    # then it is a host:port separator, not an image:tag separator.
    colon_idx = ref.rfind(":")
    if colon_idx != -1:
        after_colon = ref[colon_idx + 1:]
        if "/" not in after_colon:
            tag = after_colon
            ref = ref[:colon_idx]

    # Split at the last "/" so image_name is always a simple name (no slashes).
    # Everything before (including the trailing "/") goes into repository.
    last_slash = ref.rfind("/")
    if last_slash == -1:
        return None, ref, tag, digest

    return ref[:last_slash + 1], ref[last_slash + 1:], tag, digest


def _resolve_digest(full_ref):
    """Resolve registry manifest digest via ``docker manifest inspect``.

    Runs ``docker manifest inspect <image>`` and extracts the digest
    from the manifest entry matching platform amd64/linux.

    Returns the ``sha256:...`` digest string, or ``None`` on failure.
    """
    try:
        result = subprocess.run(
            ["docker", "manifest", "inspect", full_ref],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            log.warning("Failed to resolve digest for %s", full_ref)
            return None
        data = json.loads(result.stdout)
        manifests = data.get("manifests") or []
        for manifest in manifests:
            platform = manifest.get("platform", {})
            if (platform.get("architecture") == "amd64"
                    and platform.get("os") == "linux"):
                return manifest.get("digest")
        log.warning("No amd64/linux manifest found for %s", full_ref)
        return None
    except (json.JSONDecodeError, KeyError):
        log.warning("Failed to resolve digest for %s", full_ref)
        return None


def collect_docker_images(resources_dir, profile, nextflow_pipeline_params):
    """Collect container image references using ``nextflow inspect``.

    Uses the native ``nextflow inspect`` command (NF >= 25.04).  It scans
    pipeline ``include`` statements statically — no plugins, parameters,
    or profile are required for container discovery.

    :param resources_dir: Path to the local(ized) NF pipeline.
    :type resources_dir: str
    :param profile: Custom Nextflow profile (comma-separated, no spaces).
    :type profile: str
    :param nextflow_pipeline_params: Pipeline parameters forwarded to inspect
        (e.g. ``"--input samplesheet.csv --outdir results"``).  Some older
        nf-core pipelines have hard parameter validation that runs before
        process definitions are reached; passing the required params here
        lets ``nextflow inspect`` get past those checks.
    :type nextflow_pipeline_params: str
    :returns: List of dicts with keys: process, repository, image_name,
              tag, digest, file_id, engine.
    """
    cmd_parts = ["nextflow", "inspect", resources_dir, "-format", "json"]
    if profile:
        cmd_parts.extend(["-profile", profile])
    if nextflow_pipeline_params:
        try:
            cmd_parts.extend(shlex.split(nextflow_pipeline_params))
        except ValueError as e:
            raise ImageRefFactoryError(f"Malformed pipeline parameters: {e}")

    process = subprocess.run(cmd_parts, capture_output=True, text=True)
    if process.returncode != 0:
        raise ImageRefFactoryError(
            f"nextflow inspect failed (rc={process.returncode}): stdout: {process.stdout}\nstderr: {process.stderr}")

    try:
        inspect_data = json.loads(process.stdout)
    except (json.JSONDecodeError, ValueError) as e:
        raise ImageRefFactoryError(f"Failed to parse nextflow inspect output: {e}")

    processes = inspect_data.get("processes", [])
    if not processes:
        log.warning("No processes found in nextflow inspect output")
        return []

    image_refs = []
    for entry in processes:
        container = entry.get("container", "")
        if not container:
            continue

        repository, image_name, tag, digest = _parse_docker_ref(container)

        # Reject refs that specify both tag and digest. A ref should
        # identify an image by tag OR by digest, not both.
        if tag and digest:
            raise ImageRefFactoryError(f"Image reference has both tag and digest: {container}")

        # Resolve registry digest only for images with no tag AND no digest
        # (implicit latest). Tagged images get their digest resolved
        # downstream by ImageRef._cache() after pull.
        if not digest and not tag:
            full_ref = (repository or "") + image_name
            digest = _resolve_digest(full_ref)

        image_refs.append(_ImageRef(
            process=entry.get("name", ""),
            repository=repository,
            image_name=image_name,
            tag=tag,
            digest=digest,
            file_id=None,
            engine="docker",
        ))

    _populate_cached_file_ids(image_refs)
    return [dataclasses.asdict(ref) for ref in image_refs]


def _populate_cached_file_ids(image_refs):
    """Look up previously cached Docker images in the current project.

    Searches ``/.cached_docker_images/<image_name>/`` for files matching
    ``<image_name>_<tag>``.  When found, verifies the image digest stored
    in the file's ``properties`` and populates ``file_id`` so that
    ``DockerImageRef._package_bundle()`` skips the pull/save/upload cycle
    and reuses the existing platform file.

    This benefits:
    - Repeat builds of the same pipeline (faster rebuilds)
    - Colleagues in the same project building other pipelines that share
      images (e.g. samtools, fastqc are used across many nf-core pipelines)
    """
    project_id = config.get("DX_PROJECT_CONTEXT_ID")
    if not project_id:
        return

    # Deduplicate by (repository, image_name, tag) to avoid redundant API calls
    unique_keys = {}
    for ref in image_refs:
        key = (ref.repository, ref.image_name, ref.tag)
        if key not in unique_keys:
            unique_keys[key] = ref

    cache_hits = 0
    for (repository, image_name, tag), ref in unique_keys.items():
        cache_file_name = "_".join(filter(lambda x: x, [image_name, tag]))
        cache_folder = f"/.cached_docker_images/{image_name}/"

        try:
            results = list(find_data_objects(
                classname="file",
                state="closed",
                project=project_id,
                folder=cache_folder,
                name=cache_file_name,
                describe={"fields": {"properties": True}},
                limit=1,
            ))
        except Exception as e:
            log.warning(f"Docker image cache: failed to search for {image_name}/{tag}: {e}")
            continue

        if results:
            file_id = results[0]["id"]
            desc = results[0].get("describe", {})
            props = desc.get("properties", {})
            stored_digest = props.get("image_digest")
            if not stored_digest:
                log.warning(f"Docker image cache: skipping {image_name}/{tag} (no digest in properties)")
                continue

            # If the ref has a known digest, verify it matches
            if ref.digest and ref.digest != stored_digest:
                log.warning(f"Docker image cache: digest mismatch for {image_name}/{tag} (expected {ref.digest}, got {stored_digest})")
                continue

            cache_hits += 1
            # Apply to all refs with the same (repository, image_name, tag)
            for r in image_refs:
                if r.repository == repository and r.image_name == image_name and r.tag == tag:
                    r.file_id = file_id

    if cache_hits:
        log.info(f"Docker image cache: {cache_hits}/{len(unique_keys)} images found in project {project_id}")
