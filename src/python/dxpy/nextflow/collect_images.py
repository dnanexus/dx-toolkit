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
import os
import re
import shlex
import subprocess
import sys
import time
from typing import Optional

from dxpy import config, find_data_objects
from dxpy.nextflow.ImageRefFactory import ImageRefFactory, ImageRefFactoryError

log = logging.getLogger(__name__)

# Hostname pattern for AWS ECR registries. Must match the bash-side `is_ecr_host`
# function in nextaur's DxBashLib.groovy / nextflow.sh — keep in sync. Commercial
# AWS partition only; GovCloud (us-gov-*) and China (.amazonaws.com.cn) are
# rejected because their STS/ECR endpoints differ and have not been validated.
_ECR_HOST_RE = re.compile(r"^[0-9]+\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com$", re.IGNORECASE)

# Tracks (host, region) pairs we've successfully `docker login`-ed during this
# build. The Docker registry auth token returned by `aws ecr get-login-password`
# is valid for ~12h; the underlying STS web-identity session credentials are
# only ~1h. For typical NPI-hosted builds (well under an hour) one login per
# host per build is sufficient. If a build legitimately runs longer than the
# STS session, subsequent `aws ecr get-login-password` calls will fail with
# ExpiredToken — `_ecr_docker_login` returns False and the caller logs the
# specific AWS error to the build log.
_ECR_LOGGED_IN_HOSTS = set()


def _extract_ecr_host_and_region(image_ref):
    """If `image_ref`'s registry hostname is an AWS ECR endpoint, return
    `(host_lowercased, region)`. Otherwise return `(None, None)`.

    GovCloud and China partitions are rejected — see `_ECR_HOST_RE`.
    """
    if not image_ref:
        return None, None
    # Hostname is everything up to the first '/'. Strip an optional :port (rare
    # for ECR but safe to handle).
    first = image_ref.split("/", 1)[0].split(":", 1)[0].lower()
    m = _ECR_HOST_RE.match(first)
    if not m:
        return None, None
    region = m.group(1).lower()
    # Reject all non-commercial AWS partitions. STS / ECR endpoints differ in:
    #   us-gov-*   GovCloud
    #   us-iso-*   Secret Region (and us-isob-* Top Secret)
    # Only commercial partitions have been validated end-to-end with the JIT-
    # based auth flow used by Phase 1/2.
    if region.startswith(("us-gov-", "us-iso-", "us-isob-")):
        return None, None
    return first, region


def _ecr_docker_login(host, region):
    """Authenticate `docker` to the given ECR host using the local AWS [ecr] profile.

    Uses `aws ecr get-login-password` (which the importer environment already
    has via the awscli asset) rather than boto3 to avoid adding boto3 as a dxpy
    dependency. The `[ecr]` profile must have been configured by the importer
    job entrypoint before `--cache-docker` invokes this code; if it isn't, the
    `aws` call fails and we return False so the caller can log a clear warning
    (the subsequent `docker pull` will fail with a registry auth error).

    Cached per (host, region) for the lifetime of the process.
    """
    key = (host, region)
    if key in _ECR_LOGGED_IN_HOSTS:
        return True
    try:
        # `aws ecr get-login-password` prints a 12h auth token to stdout.
        token_proc = subprocess.run(
            ["aws", "--profile", "ecr", "ecr", "get-login-password", "--region", region],
            capture_output=True, text=True, check=False,
        )
        if token_proc.returncode != 0:
            log.warning(
                "ECR get-login-password failed for host=%s region=%s rc=%d stderr=%s. "
                "Is dnanexus.ecrRoleArnToAssume configured and the [ecr] AWS profile set up?",
                host, region, token_proc.returncode, token_proc.stderr.strip(),
            )
            return False
        # Pipe the password into `docker login --password-stdin`. Using stdin
        # avoids the password ever appearing on a command line / process list.
        #
        # We login twice: once as the job user (so `docker manifest inspect` —
        # which runs without sudo — can read the registry), and once via sudo
        # (so `sudo docker pull` / `sudo docker save` in DockerImageRef._cache
        # also see the auth). Without the sudo login, `sudo docker pull` reads
        # /root/.docker/config.json which does not contain the ECR token, and
        # falls back to anonymous access — failing for private registries.
        login_proc = subprocess.run(
            ["docker", "login", "--username", "AWS", "--password-stdin", host],
            input=token_proc.stdout, capture_output=True, text=True, check=False,
        )
        if login_proc.returncode != 0:
            log.warning(
                "docker login to ECR host %s failed rc=%d stderr=%s",
                host, login_proc.returncode, login_proc.stderr.strip(),
            )
            return False
        # Mirror auth into root's docker config for `sudo docker pull/save` callers.
        # Failure here is logged but non-fatal — the job-user login already
        # succeeded, which is enough for non-sudo callers.
        sudo_login = subprocess.run(
            ["sudo", "-n", "docker", "login", "--username", "AWS", "--password-stdin", host],
            input=token_proc.stdout, capture_output=True, text=True, check=False,
        )
        if sudo_login.returncode != 0:
            log.warning(
                "sudo docker login to ECR host %s failed rc=%d stderr=%s "
                "(`sudo docker pull` may fail with anonymous-access auth errors)",
                host, sudo_login.returncode, sudo_login.stderr.strip(),
            )
        _ECR_LOGGED_IN_HOSTS.add(key)
        _progress(f"  Logged in to ECR registry {host}")
        return True
    except (OSError, subprocess.SubprocessError) as e:
        log.warning("ECR login error for host=%s region=%s: %s", host, region, e)
        return False


def ensure_ecr_login_for_image(image_ref):
    """Best-effort ECR login for an image ref. No-op for non-ECR images.

    Returns True if the image is non-ECR or the ECR login succeeded; False if
    the image is ECR but auth could not be set up. The caller is expected to
    proceed with `docker pull` either way — a False result simply means the
    pull is likely to fail with an auth error, which the user can then debug.
    """
    host, region = _extract_ecr_host_and_region(image_ref)
    if host is None:
        return True
    return _ecr_docker_login(host, region)


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
    digest_is_original: bool = False  # True only if the ref had @sha256:...


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


def _resolve_digest(full_ref, use_manifest_digest=False):
    """Resolve the image digest via ``docker manifest inspect``.

    By default returns the **config digest** (``config.digest``), which is
    the same value as ``docker images --no-trunc``.  This matches the
    lookup behavior of nextaur >= 1.12.1.

    When ``use_manifest_digest=True``, returns the **platform manifest
    digest** instead (a single API call).  This matches the lookup
    behavior of older nextaur versions (< 1.12.1) which use the manifest
    digest from ``docker manifest inspect`` to search the cache.

    For multi-arch images with ``use_manifest_digest=False``, this
    requires two calls: one to get the manifest list, then a second to
    fetch the platform manifest and extract its config digest.

    Single-arch images always return the config digest directly,
    regardless of ``use_manifest_digest``.  This is safe because old
    nextaur's ``implicitLatestGetDigest()`` cannot parse single-arch
    manifests (no ``manifests`` array) and returns null, so it never
    does a digest-based lookup for single-arch images.

    Returns the ``sha256:...`` digest string, or ``None`` on failure.
    """
    data = _docker_manifest_inspect(full_ref)
    if data is None:
        return None

    # Multi-arch: find the amd64/linux manifest.
    manifests = data.get("manifests") or []
    for manifest in manifests:
        platform = manifest.get("platform", {})
        if (platform.get("architecture") == "amd64"
                and platform.get("os") == "linux"):
            platform_digest = manifest.get("digest")
            if not platform_digest:
                break
            if use_manifest_digest:
                return platform_digest
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


def collect_docker_images(resources_dir, profile, nextflow_pipeline_params, use_manifest_digest=False):
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
    :param use_manifest_digest: If True, store the platform manifest digest
        (instead of config digest) for latest/untagged images.  This is needed
        for nextaur versions prior to 1.12.1 which look up cached images by
        manifest digest.  Only affects images without a specific tag — tagged
        images always use config digest for cache deduplication consistency.
    :type use_manifest_digest: bool
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

        # Track whether the digest came from the original reference (@sha256:...)
        # vs resolved by us. Original digests are manifest digests (pullable),
        # resolved digests are config digests (not pullable, used for cache only).
        digest_is_original = bool(digest)

        # Resolve registry digest for all images that don't already have one.
        # The digest is used for cache validation in _populate_cached_file_ids()
        # to prevent cross-registry collisions (see dx-toolkit/devdocs/nextflow/cache_collision_scenarios.md).
        if not digest:
            full_ref = (repository or "") + image_name
            if tag:
                full_ref += ":" + tag
            # `docker manifest inspect` against a private ECR registry requires
            # auth too (not just `docker pull`). Run an ECR login proactively so
            # digest resolution works for ECR images. No-op for non-ECR images.
            ensure_ecr_login_for_image(full_ref)
            # Only use manifest digest for latest/untagged images where old
            # nextaur (<1.12.1) does a digest-based lookup.  Tagged images use
            # name-based lookup at runtime, so keep config digest for cache
            # deduplication consistency with existing cached files.
            # Note: "image" and "image:latest" get separate cache keys via
            # full_ref, but both use manifest digest when use_manifest_digest
            # is True — old nextaur treats both as implicit-latest.
            is_implicit_latest = not tag or tag.lower() == "latest"
            effective_use_manifest = use_manifest_digest and is_implicit_latest
            cache_key = (full_ref, effective_use_manifest)
            if cache_key not in resolved_digests:
                resolved_digests[cache_key] = _resolve_digest(full_ref, use_manifest_digest=effective_use_manifest)
            digest = resolved_digests[cache_key]

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
            digest_is_original=digest_is_original,
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

    # Skip cache lookup when digest is unknown (e.g. _resolve_digest
    # failed).  Without a digest we cannot guarantee correctness of
    # the cache hit.  Also skip refs that already have a file_id (dx:// URIs).
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
