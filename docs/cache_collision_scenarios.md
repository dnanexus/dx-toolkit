# Docker Image Cache: Architecture and Collision Scenarios

## Image Reference Flow

How a container reference from a Nextflow pipeline flows through
`collect_images.py` → `ImageRef.py` → the platform.

```
nextflow inspect pipeline/ -format json
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  For each { "name": "PROC", "container": "..." }        │
└─────────────────────────────────────────────────────────┘
         │
         ▼
    ┌────────────┐
    │ dx:// URI?  │───Yes──▶  _parse_dx_uri(container)
    └────────────┘            Extract file-xxxx
         │ No                 Set file_id = file-xxxx
         ▼                    (skip pull/save/upload/cache)
┌─────────────────────────────────────────────────────────┐
│  _parse_docker_ref(container)                           │
│  ──────────────────────────────                         │
│  Input examples:                                        │
│    "quay.io/bio/samtools:1.17"                          │
│    "rabbit@sha256:abc"                                  │
│    "docker://ubuntu:20.04"                              │
│                                                         │
│  Output: (repository, image_name, tag, digest)          │
│    → ("quay.io/bio/", "samtools", "1.17", None)         │
│    → (None, "rabbit", None, "sha256:abc")               │
│    → (None, "ubuntu", "20.04", None)                    │
└─────────────────────────────────────────────────────────┘
         │
         ▼
    ┌────────────┐
    │ tag AND    │───Yes──▶  REJECT: ImageRefFactoryError
    │ digest?    │           "both tag and digest"
    └────────────┘
         │ No
         ▼
    ┌────────────┐
    │ digest     │───Yes──▶  Use parsed digest as-is
    │ present?   │           (from @sha256:abc in ref)
    └────────────┘                           │
         │ No                                │
         ▼                                   │
┌──────────────────────────────────────┐     │
│  _resolve_digest()                   │     │
│  Query image's digest                │     │
│                                      │     │
│  Returns sha256:… or None on failure │     │
│  (see _resolve_digest diagram below) │     │
└──────────────────────────────────────┘     │
         │                                   │
    ┌────┴────┐                              │
    ▼         ▼                              │
 sha256:…   None                             │
 (resolved) (failed)                         │
    │         │                              │
    ▼         ▼                              ▼
┌─────────────────────────────────────────────────────────┐
│  _ImageRef(process, repo, image_name, tag, digest,      │
│            file_id=None, engine="docker")               │
└─────────────────────────────────────────────────────────┘
         │
         ▼  (after all refs collected)
┌─────────────────────────────────────────────────────────┐
│  _populate_cached_file_ids(image_refs)                  │
│  ─────────────────────────────────────                  │
│  For each image ref:                                    │
│                                                         │
│    digest is None? ──Yes──▶ SKIP (no cache lookup)      │
│         │                   file_id stays None          │
│         │ No                                            │
│         ▼                                               │
│    find_data_objects(                                   │
│      folder = /.cached_docker_images/{image_name}/      │
│      name   = {image_name}_{tag}                        │
│      properties = {"image_digest": digest}  ◀── server  │
│      limit  = 1                                side     │
│    )                                          filter    │
│         │                                               │
│    ┌────┴────┐                                          │
│    ▼         ▼                                          │
│  Match     No match                                     │
│    │         │                                          │
│    ▼         ▼                                          │
│  file_id   file_id                                      │
│  = hit ✓   = None                                       │
│                                                         │
│  Set file_id on this ref                                │
└─────────────────────────────────────────────────────────┘
         │
         ▼  return [dataclasses.asdict(ref) for ref in image_refs]
         │
═════════════════════════════════════════════════════════════
         │  bundle_docker_images() / ImageRefFactory
         ▼
┌─────────────────────────────────────────────────────────┐
│  DockerImageRef._package_bundle()                       │
│                                                         │
│    file_id present? ──Yes──▶ REUSE cached file          │
│         │                    (skip pull/save/upload)    │
│         │ No                                            │
│         ▼                                               │
│    _reconstruct_image_ref()                             │
│         │                                               │
│    ┌────┴──────────┬───────────────┐                    │
│    ▼               ▼               ▼                    │
│  has tag         digest only     neither                │
│  (even if        (no tag)        (bare name)            │
│   digest too)                                           │
│    │               │               │                    │
│    ▼               ▼               ▼                    │
│  repo/img:tag    repo/img@sha256  repo/img              │
│                                                         │
│         │                                               │
│         ▼                                               │
│    docker pull <ref>                                    │
│    docker save <ref> | gzip > file                      │
│    upload_local_file(                                   │
│      folder = /.cached_docker_images/{image_name}/      │
│      properties = {"image_digest": digest}              │
│    )                                                    │
└─────────────────────────────────────────────────────────┘
```

### `_resolve_digest()` detail

Always returns the **config digest** (`config.digest`), which is the
same value as `docker images --no-trunc`.  This keeps the digest format
consistent with pre-existing cached files.

```
docker manifest inspect <full_ref>
         │
    ┌────┴────┐
    ▼         ▼
  rc = 0    rc != 0 / JSON error / Docker missing
    │              │
    │              ▼
    │         return None
    ▼
┌──────────────────┐
│ has manifests[]? │──Yes──▶ Find amd64/linux entry
└──────────────────┘              │         │
    │ No                        Found    Not found
    │                             │         │
    │                             ▼         │
    │              docker manifest inspect  │
    │             <image>@<platform_digest> │
    │                             │         │
    │                        ┌────┴────┐    │
    │                        ▼         ▼    │
    │                      rc = 0    fail   │
    │                        │         │    │
    │                        ▼         │    │
    │                   return         │    │
    │                   config.digest  │    │
    │                                  │    │
    ◀──────────────────────────────────┴────┘
    │ (fall through)
    ▼
┌──────────────────┐
│ has config.digest│
│ (single-arch)?   │
└──────────────────┘
    │         │
   Yes        No
    │         │
    ▼         ▼
return      return None
config.digest
```

## Cache Architecture

Three layers share the same schema, all ignoring `repository`:

| Layer | Path construction |
|-------|-------------------|
| **Python writer** ([`ImageRef.py:35`][writer]) | `/.cached_docker_images/{image_name}/` |
| **Python reader** ([`collect_images.py:248`][reader]) | `/.cached_docker_images/{image_name}/{image_name}_{tag}` |
| **Groovy reader** (`DockerImage.groovy:44` in nextaur-app) | `/.cached_docker_images/{image_name}/{image_name}_{tag}` |

[writer]: https://github.com/dnanexus/dx-toolkit/blob/f9b65800de863217fa6685519fb669f7ccf35e27/src/python/dxpy/nextflow/ImageRef.py#L35
[reader]: https://github.com/dnanexus/dx-toolkit/blob/f9b65800de863217fa6685519fb669f7ccf35e27/src/python/dxpy/nextflow/collect_images.py#L248


## Image Reference Formats

| Format | Example | tag | digest |
|--------|---------|-----|--------|
| Tagged | `quay.io/bio/samtools:1.17` | `1.17` | resolved via `docker manifest inspect` |
| Digest-only | `quay.io/bio/fastqc@sha256:abc` | `None` | `sha256:abc` (from ref) |
| Untagged | `quay.io/bio/samtools` | `None` | resolved via `docker manifest inspect` |
| Tag+digest | `quay.io/bio/samtools:1.17@sha256:abc` | — | — ([rejected][reject]) |
| Platform file | `dx://project-xxxx:file-yyyy` | `None` | `None` (file_id set directly, no cache) |

[reject]: https://github.com/dnanexus/dx-toolkit/blob/f9b65800de863217fa6685519fb669f7ccf35e27/src/python/dxpy/nextflow/collect_images.py#L196-L197

## Collision Scenarios

### 1. Same image, same repo, multiple processes

```
Process A: quay.io/bio/samtools:1.17
Process B: quay.io/bio/samtools:1.17
```

- Cache path: `/.cached_docker_images/samtools/samtools_1.17`
- Both refs look up with `properties={"image_digest": "sha256:xxx"}` — same file returned
- **Result: Correct.** Same image, same cache file.

### 2. Different images, same repo

```
Process A: quay.io/bio/samtools:1.17
Process B: quay.io/bio/fastqc:0.12.1
```

- Different image names (`samtools` vs `fastqc`)
- Cache paths differ (`samtools/samtools_1.17` vs `fastqc/fastqc_0.12.1`)
- **Result: Correct.** No collision possible.

### 3. Same image name + tag, different repos (cross-registry)

```
Process A: quay.io/bio/samtools:1.17     → digest sha256:aaa
Process B: dockerhub/samtools:1.17       → digest sha256:bbb
```

- Cache path: both resolve to `/.cached_docker_images/samtools/samtools_1.17` — **same filename**
- Ref A lookup: `properties={"image_digest": "sha256:aaa"}` → match → cache hit
- Ref B lookup: `properties={"image_digest": "sha256:bbb"}` → no match → cache miss, re-pulled

**Guard: "always resolve digest" fix.** Without it, tagged images have `digest=None` and the server-side `properties` filter cannot distinguish images — silent collision. With digest always resolved, the filter returns only the correct file.

### 4. Same image name, digest-only, different digests (same repo)

```
Process A: quay.io/bio/fastqc@sha256:aaa
Process B: quay.io/bio/fastqc@sha256:bbb
```

- Cache path: both resolve to `/.cached_docker_images/fastqc/fastqc` — **same filename**
- Ref A lookup: `properties={"image_digest": "sha256:aaa"}` → match → cache hit
- Ref B lookup: `properties={"image_digest": "sha256:bbb"}` → no match → cache miss

**Guard: server-side `properties` filter.** Each ref's lookup includes its own digest, so different digests return different (or no) files.

### 5. Same image name, digest-only, different repos

```
Process A: quay.io/bio/fastqc@sha256:aaa
Process B: dockerhub/fastqc@sha256:bbb
```

- Cache path: both `/.cached_docker_images/fastqc/fastqc` — **same filename**
- Same as Scenario 4: each ref's digest filter prevents incorrect reuse.

**Guard: server-side `properties` filter.**

### 6. Untagged images (implicit :latest), same repo

```
Process A: quay.io/bio/samtools
Process B: quay.io/bio/samtools
```

- Both resolve to same digest via `docker manifest inspect quay.io/bio/samtools`
- Both look up with `properties={"image_digest": "sha256:xxx"}` — same file returned
- **Result: Correct.** Same image, same cache file.

### 7. Untagged images, different repos

```
Process A: quay.io/bio/samtools         → digest sha256:aaa
Process B: dockerhub/samtools           → digest sha256:bbb
```

- Cache path: both `/.cached_docker_images/samtools/samtools` — **same filename**
- Ref A lookup: `properties={"image_digest": "sha256:aaa"}` → match → cache hit
- Ref B lookup: `properties={"image_digest": "sha256:bbb"}` → no match → cache miss

**Guard: digest always resolved for untagged images (existing behavior) + server-side `properties` filter.**

### 8. Same image, different tags

```
Process A: quay.io/bio/samtools:1.17    → digest sha256:aaa
Process B: quay.io/bio/samtools:1.18    → digest sha256:bbb
```

- Different tags
- Cache paths differ (`samtools_1.17` vs `samtools_1.18`)
- **Result: Correct.** No collision.

### 9. Tag + digest (rejected)

```
Process A: quay.io/bio/samtools:1.17@sha256:abc
```

- [Rejected][reject] with `ImageRefFactoryError`
- **Result: Correct.** Ambiguous refs are not allowed.

## Summary of Guards

| Guard | What it prevents | Status |
|-------|------------------|--------|
| [Reject tag+digest][reject] | Ambiguous image refs | Existing |
| Resolve digest for all images | Images missing digest for cache validation | Existing (untagged) → **broadened to all** (This PR) |
| Server-side `properties` filter | Reusing cached file with wrong content | **This PR** (replaces client-side check) |
| Skip cache when no digest | `digest=None` fallback matching arbitrary files | **This PR** |
| `ImageRef`: tag over digest in pull ref | Tagged images pulled by `@sha256:` instead of `:tag` | **This PR** |

## Known Limitations

### Cache path ignores `repository`

The cache **path** still ignores `repository`. This means:
- A cache miss due to digest mismatch causes a re-pull and upload of a new file. DNAnexus allows multiple files with the same name in the same folder, so both coexist.
- With server-side `properties={"image_digest": ref.digest}` filtering, `limit=1` always returns the correct file (or no file). Duplicate file names are no longer a correctness concern.
- This is acceptable: the cache is an optimization, and correctness is guaranteed by the server-side digest filter.

Fixing the cache path to include `repository` would require changes across all three layers (Python writer, Python reader, Groovy reader) and is tracked separately.

### Degraded behavior when digest resolution fails

If `_resolve_digest()` fails for any reason (network error, Docker not available),
the image gets `digest=None` and **skips the cache entirely**. The image will be
re-pulled on every build — safe but slower.
