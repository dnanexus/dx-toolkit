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
import sys
import time
from typing import Optional

from dxpy import config, find_data_objects
from dxpy.nextflow.ImageRefFactory import ImageRefFactory, ImageRefFactoryError

log = logging.getLogger(__name__)


def _progress(msg):
    """Write user-facing progress to stderr (stdout is reserved for --json output)."""
    print(msg, file=sys.stderr, flush=True)


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
          -> ("quay.io/biocontainers/", "fastqc", "0.12.1", None)
        community.wave.seqera.io/library/star:5acb4e8c
          -> ("community.wave.seqera.io/library/", "star", "5acb4e8c", None)
        ubuntu:20.04
          -> (None, "ubuntu", "20.04", None)
        myregistry:5000/myimage:latest
          -> ("myregistry:5000/", "myimage", "latest", None)
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


def _parse_dx_uri(uri):
    """Extract (project_id, file_id) from a ``dx://`` URI.

    Supported formats::

        dx://file-xxxx              → (None, "file-xxxx")
        dx://project-xxxx:file-xxxx → ("project-xxxx", "file-xxxx")
    """
    path = uri[len("dx://"):]
    parts = path.split(":")
    project_id = None
    file_id = None
    for part in parts:
        if part.startswith("project-"):
            project_id = part
        elif part.startswith("file-"):
            file_id = part
    return project_id, file_id


def _docker_manifest_inspect(ref):
    """Run ``docker manifest inspect`` with retries on failure.

    Retries twice (after 60 s, then 300 s) to handle transient registry
    or network errors.  Returns the parsed JSON dict, or ``None`` on
    permanent failure.
    """
    attempts = [0, 60, 300]
    for i, delay in enumerate(attempts):
        if delay:
            log.info(f"Retrying docker manifest inspect for {ref} in {delay}s (attempt {i + 1}/{len(attempts)})")
            time.sleep(delay)
        try:
            result = subprocess.run(
                ["docker", "manifest", "inspect", ref],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                return json.loads(result.stdout)
            log.warning(f"docker manifest inspect failed for {ref} (rc={result.returncode}, attempt {i + 1}/{len(attempts)})")
        except (json.JSONDecodeError, OSError) as e:
            log.warning(f"docker manifest inspect error for {ref} (attempt {i + 1}/{len(attempts)}): {e}")
    return None


def _resolve_digest(full_ref):
    """Resolve the image config digest via ``docker manifest inspect``.

    Always returns the **config digest** (``config.digest``), which is
    the same value as ``docker images --no-trunc``.  This keeps the
    digest format consistent with pre-existing cached files so that
    upgrading does not cause a one-time cache invalidation.

    For multi-arch images this requires two calls: one to get the
    manifest list and find the amd64/linux platform digest, then a
    second to fetch that platform's manifest and extract its config
    digest.  Single-arch images return a flat manifest with the config
    digest directly.

    Returns the ``sha256:...`` digest string, or ``None`` on failure.
    """
    data = _docker_manifest_inspect(full_ref)
    if data is None:
        return None

    # Multi-arch: find the amd64/linux manifest, then fetch its
    # config digest with a second inspect call.
    manifests = data.get("manifests") or []
    for manifest in manifests:
        platform = manifest.get("platform", {})
        if (platform.get("architecture") == "amd64"
                and platform.get("os") == "linux"):
            platform_digest = manifest.get("digest")
            if not platform_digest:
                break
            return _get_config_digest(full_ref, platform_digest)

    # Single-arch: config digest is directly in the flat manifest
    config = data.get("config")
    if isinstance(config, dict):
        config_digest = config.get("digest")
        if config_digest:
            return config_digest

    log.warning(f"No digest found for {full_ref}")
    return None


def _get_config_digest(image_ref, platform_digest):
    """Fetch the config digest from a platform-specific manifest.

    Given the platform manifest digest (from a multi-arch manifest list),
    fetches ``<image_ref>@<platform_digest>`` and extracts ``config.digest``.
    """
    # Strip :tag from image_ref (e.g. "quay.io/bio/samtools:1.17" → "quay.io/bio/samtools")
    # so we can address the platform manifest by digest.  Use the same
    # host:port disambiguation as _parse_docker_ref: the tag is after
    # the last colon only if there's no "/" after it.
    colon_idx = image_ref.rfind(":")
    if colon_idx != -1 and "/" not in image_ref[colon_idx + 1:]:
        base_ref = image_ref[:colon_idx]
    else:
        base_ref = image_ref
    ref_by_digest = f"{base_ref}@{platform_digest}"
    data = _docker_manifest_inspect(ref_by_digest)
    if data is None:
        return None
    config = data.get("config")
    if isinstance(config, dict):
        return config.get("digest")
    log.warning(f"No config digest in platform manifest for {ref_by_digest}")
    return None


def collect_docker_images(resources_dir, profile, nextflow_pipeline_params):
    """Collect container image references using ``nextflow inspect``.

    Uses the native ``nextflow inspect`` command.  It scans
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
    resolved_digests = {}
    _progress(f"Resolving {len(processes)} container image(s) from pipeline...")
    for entry in processes:
        container = entry.get("container", "")
        if not container:
            continue

        # dx:// URIs reference platform files directly — no pull/save/upload
        # needed. Extract the file ID so _package_bundle() reuses it.
        if container.startswith("dx://"):
            _, file_id = _parse_dx_uri(container)
            if not file_id:
                raise ImageRefFactoryError(f"Could not parse dx:// URI: {container}")
            _progress(f"  {container} -> platform file")
            image_refs.append(_ImageRef(
                process=entry.get("name", ""),
                repository=None,
                image_name=file_id,
                tag=None,
                digest=None,
                file_id=file_id,
                engine="docker",
            ))
            continue

        repository, image_name, tag, digest = _parse_docker_ref(container)

        # Reject refs that specify both tag and digest. A ref should
        # identify an image by tag OR by digest, not both.
        if tag and digest:
            raise ImageRefFactoryError(f"Image reference has both tag and digest: {container}")

        # Resolve registry digest for all images that don't already have one.
        # The digest is used for cache validation in _populate_cached_file_ids()
        # to prevent cross-registry collisions (see dx-toolkit/devdocs/nextflow/cache_collision_scenarios.md).
        if not digest:
            full_ref = (repository or "") + image_name
            if tag:
                full_ref += ":" + tag
            if full_ref not in resolved_digests:
                resolved_digests[full_ref] = _resolve_digest(full_ref)
            digest = resolved_digests[full_ref]

        display_ref = (repository or "") + image_name + (":" + tag if tag else "")
        if digest:
            # Show algo prefix (sha256:) + first 12 hex chars
            _progress(f"  {display_ref} (digest: {digest[:19]}...)")
        else:
            _progress(f"  {display_ref} (digest: unresolved)")

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

    cacheable = [ref for ref in image_refs if ref.digest and not ref.file_id]
    if not cacheable:
        return

    _progress(f"Checking project cache for {len(cacheable)} docker image(s)...")
    cache_hits = 0
    for ref in cacheable:
        display = ref.image_name + (":" + ref.tag if ref.tag else "")
        cache_file_name = "_".join(filter(lambda x: x, [ref.image_name, ref.tag]))
        cache_folder = f"/.cached_docker_images/{ref.image_name}/"

        try:
            results = list(find_data_objects(
                classname="file",
                state="closed",
                project=project_id,
                folder=cache_folder,
                name=cache_file_name,
                properties={"image_digest": ref.digest},
                describe={"fields": {"name": True, "folder": True, "project": True}},
                limit=1,
            ))
        except Exception as e:
            log.warning(f"Docker image cache: failed to search for {display}: {e}")
            _progress(f"  {display}: SKIP (lookup error)")
            continue

        if results:
            ref.file_id = results[0]["id"]
            cache_hits += 1
            desc = results[0]["describe"]
            _progress(f"  {display}: CACHED ({desc['project']}:{desc['folder']}/{desc['name']} ({results[0]['id']}))")
        else:
            _progress(f"  {display}: MISS (will pull and upload)")

    cache_misses = len(cacheable) - cache_hits
    _progress(f"Docker image cache: {cache_hits} cached, {cache_misses} to upload")
