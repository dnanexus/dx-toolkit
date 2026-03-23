# Multi-version Nextflow Support

## Overview

dx-toolkit supports building Nextflow pipelines against multiple Nextflow engine versions. Users select a version via the `--nextflow-version` CLI flag; if omitted, the configured default version is used.

## CLI Usage

```bash
# Build with default (latest) version
dx build --nextflow ./pipeline

# Build with explicit version
dx build --nextflow --nextflow-version 25.10 ./pipeline

# Works with NPI (remote) builds too
dx build --nextflow --nextflow-version 24.10 --repository https://github.com/org/pipeline
```

## CLI Reference

```
--nextflow-version VERSION
    Nextflow engine version to use (e.g., 25.10, 24.10).
    Defaults to the configured default version.
    Deprecated versions will print a warning.
    An invalid version will list all available versions.
    Requires --nextflow.
```

**Validation**: `--nextflow-version` without `--nextflow` is an error.

**Applet metadata**: The resolved version is stored in `dxapp.details.nextflowVersion` and can be inspected with:

```bash
dx describe <applet> --json | jq .details.nextflowVersion
```

## Flow Diagram

```
dx build --nextflow [--nextflow-version X.Y] [--repository URL] ./pipeline
│
├─ --repository provided? ──────────────────────────── YES ──┐
│                                                            │
│  NO (Local Build)                                 (NPI Build)
│                                                            │
▼                                                            ▼
prepare_nextflow()                              build_pipeline_with_npi()
│                                                            │
▼                                                            ├─ resolve_version(warn=False)
get_nextflow_dxapp()                                         │  (validate only, no deprecation warning)
│                                                            │
▼                                                            ▼
resolve_version()                               _npi_supports_version_selection()
│                                               │  (describe NPI inputSpec at runtime)
├─ version=None? ── use default from            │
│                   versions.json                ├─ NPI has nextflow_version? ─── YES ──┐
│                                               │                                      │
├─ version not found? ── DXCLIError             │  NO                                  │
│  "Available versions: ..."                    │                                      │
│                                               ▼                                      ▼
├─ status=deprecated? ── WARNING to stderr      Print WARNING:                  input_hash[
│  "Consider upgrading to {default}"            "NPI does not support           "nextflow_version"
│                                               version selection"              ] = version
▼                                               │                                      │
returns (version_key, version_config)           │                                      │
│                                               └──────────┬───────────────────────────┘
▼                                                          │
get_regional_options()                                     ▼
│                                                   DXApp.run(input_hash)
▼                                                   │  (launches NPI job)
get_nextflow_assets(region, version_config)         │
│                                                   ▼
├─ load *_assets_{ver}.json for region       NPI worker runs:
│                                            dx build --nextflow
├─ describe(nextflow_asset) ── OK ─┐         [--nextflow-version X.Y]
│                                  │         │
├─ ResourceNotFound ───┐           │         ▼
│  or FileNotFoundError│           │         (same local build flow
│                      ▼           │          on worker's dxpy)
│  load staging files  │           │
│  *_assets.staging.json           │
│                      │           │
└──────────────────────┴───────────┘
                       │
                       ▼
              Build applet with resolved assets
              details.nextflowVersion = resolved version
```

## Architecture

### Version Manifest (`versions.json`)

All version metadata lives in `versions.json`:

```json
{
  "default": "25.10",
  "versions": {
    "25.10": {
      "status": "supported",
      "nextflow_assets": "nextflow_assets_25_10.json",
      "nextaur_assets": "nextaur_assets_25_10.json",
      "awscli_assets": "awscli_assets_25_10.json"
    },
    "24.10": {
      "status": "deprecated",
      "nextflow_assets": "nextflow_assets_24_10.json",
      "nextaur_assets": "nextaur_assets_24_10.json",
      "awscli_assets": "awscli_assets_24_10.json"
    }
  }
}
```

Each version entry maps to its own set of asset files containing per-region record IDs. Staging fallback files are derived by replacing `.json` with `.staging.json`.

### Version Statuses

- **`supported`** — current, no warnings
- **`deprecated`** — still functional, prints a warning to stderr recommending upgrade

### Build Paths

**Local build** (`dx build --nextflow ./pipeline`):
1. `dx.py` parses `--nextflow-version`
2. `prepare_nextflow()` → `get_nextflow_dxapp()` → `resolve_version()`
3. `resolve_version()` reads `versions.json`, validates, returns version config
4. `get_nextflow_assets()` loads the correct per-version asset files
5. Resolved version is recorded in `dxapp.details.nextflowVersion`

**NPI build** (`dx build --nextflow --repository URL`):
1. `build_pipeline_with_npi()` validates the version early via `resolve_version()`
2. Auto-detects whether the deployed NPI app accepts `nextflow_version` input
3. If NPI supports it: passes version to NPI, which forwards it to `dx build --nextflow`
4. If NPI doesn't support it: prints a warning, builds with NPI's default version

### NPI Auto-detection

The `_npi_supports_version_selection()` function describes the NPI app's inputSpec at runtime to check for `nextflow_version`. This bridges the gap during deployment: dx-toolkit can be released before the NPI app is updated.

**Limitation**: The NPI build path depends on the **worker's dxpy version**, not the local dxpy. When NPI runs `dx build --nextflow --nextflow-version X.Y` on the worker, it uses the worker's installed dxpy to resolve version configs and load asset files. If the worker's dxpy has different or outdated asset files, the resulting applet may use different assets than what the local dxpy specifies. For this reason, asset verification tests (`test_dx_build_version_flag_selects_correct_assets`) only cover the local build path.

## Adding a New Version

1. Create asset files: `nextflow_assets_XX_YY.json`, `nextaur_assets_XX_YY.json`, `awscli_assets_XX_YY.json` (prod + staging variants) with per-region record IDs
2. Add an entry to `versions.json` under `"versions"`
3. Update `"default"` in `versions.json` if the new version should be the default
4. Optionally set the old default's status to `"deprecated"`

## Deprecating a Version

Set `"status": "deprecated"` in `versions.json`. Users selecting that version will see a stderr warning but can still build.

## Removing a Version

Remove the entry from `versions.json` and delete the associated asset files. Users requesting that version will get an error listing available versions.

## File Layout

```
dxpy/nextflow/
├── versions.json                      # Version manifest
├── nextflow_assets_25_10.json         # 25.10 prod assets
├── nextflow_assets_25_10.staging.json # 25.10 staging assets
├── nextaur_assets_25_10.json
├── nextaur_assets_25_10.staging.json
├── awscli_assets_25_10.json
├── awscli_assets_25_10.staging.json
├── nextflow_assets_24_10.json         # 24.10 prod assets
├── nextflow_assets_24_10.staging.json # 24.10 staging assets
├── nextaur_assets_24_10.json
├── nextaur_assets_24_10.staging.json
├── awscli_assets_24_10.json
├── awscli_assets_24_10.staging.json
└── ...
```

## Key Functions

| Function | File | Purpose |
|----------|------|---------|
| `resolve_version()` | `nextflow_utils.py` | Validates version, returns config, prints deprecation warnings |
| `get_nextflow_assets()` | `nextflow_utils.py` | Loads per-version, per-region asset record IDs |
| `_npi_supports_version_selection()` | `nextflow_builder.py` | Runtime check of NPI inputSpec |

## Deployment Order

1. Release dx-toolkit with multi-version support (auto-detect bridges the gap)
2. Release NPI app with `nextflow_version` input
3. Once NPI is deployed, the full flow works end-to-end

The auto-detect logic ensures no breakage between steps 1 and 2.

## Test Coverage

**File**: `src/python/test/test_nextflow.py`

### Summary

| Category | Count | Coverage |
|----------|-------|----------|
| Unit tests (`TestNextflowVersionResolution`) | 29 | ~90% of error paths |
| Integration tests (`TestDXBuildNextflowApplet`) | 7 | Local build happy/error paths |
| Total version-related tests | 36 | |

### Unit Tests — `TestNextflowVersionResolution`

#### Manifest loading (`_load_versions_manifest`)

| Test | Scenario | Assertion |
|------|----------|-----------|
| `test_manifest_file_not_found` | `versions.json` missing | DXCLIError, "Failed to load" |
| `test_manifest_invalid_json` | Corrupt JSON content | DXCLIError, "Failed to load" |
| `test_manifest_missing_default_key` | JSON has `versions` but no `default` | DXCLIError, "missing" |
| `test_manifest_missing_versions_key` | JSON has `default` but no `versions` | DXCLIError, "missing" |
| `test_manifest_validation_missing_keys` | Version entry missing required keys | DXCLIError, "missing" |

#### Version resolution (`resolve_version`)

| Test | Scenario | Assertion |
|------|----------|-----------|
| `test_resolve_version_default` | `None` input | Returns "25.10", status "supported" |
| `test_resolve_version_explicit` | `"25.10"` input | Returns correct config with asset filenames |
| `test_resolve_version_deprecated_warning` | `"24.10"` with `warn=True` | Writes "deprecated" to stderr |
| `test_resolve_version_deprecated_warn_false` | `"24.10"` with `warn=False` | No stderr output |
| `test_resolve_version_invalid` | `"99.99"` | DXCLIError with version and "Available versions" |
| `test_resolve_version_default_misconfigured` | Default points to nonexistent version | DXCLIError, "misconfigured" |
| `test_invalid_version_error_lists_available` | Invalid version error message | Lists both "24.10" and "25.10" |
| `test_deprecated_warning_suggests_default` | Deprecation warning text | Mentions default version "25.10" |

#### Asset loading (`get_nextflow_assets`, `get_regional_options`)

| Test | Scenario | Assertion |
|------|----------|-----------|
| `test_get_nextflow_assets_with_version_config` | Happy path with version_config | Returns record IDs, calls describe once |
| `test_get_nextflow_assets_staging_fallback` | Prod describe raises ResourceNotFound | Falls through to staging, returns record IDs |
| `test_get_nextflow_assets_invalid_region` | Bad region key | DXCLIError with region name |
| `test_get_nextflow_assets_both_params_error` | Both `nextflow_version` and `version_config` | DXCLIError, "not both" |
| `test_get_nextflow_assets_staging_file_not_found` | Both prod and staging files missing | DXCLIError, "Staging asset files not found" |
| `test_get_nextflow_assets_staging_region_missing` | Staging files exist, region absent | DXCLIError with region name |
| `test_regional_options_assets_match_local_files` | For each version, build `regionalOptions` | `assetDepends` record IDs exactly match local asset JSON files |

#### dxapp template (`get_nextflow_dxapp`)

| Test | Scenario | Assertion |
|------|----------|-----------|
| `test_dxapp_records_nextflow_version` | Explicit version | `details.nextflowVersion == "25.10"` |
| `test_dxapp_records_default_nextflow_version` | Default version | `details.nextflowVersion == "25.10"` |
| `test_dxapp_threads_default_version_config` | Default version config threading | `version_config` passed to `get_regional_options` has 25.10 asset filenames |
| `test_dxapp_threads_deprecated_version_config` | Deprecated version config threading | `version_config` passed to `get_regional_options` has 24.10 asset filenames |

#### NPI auto-detect (`_npi_supports_version_selection`)

| Test | Scenario | Assertion |
|------|----------|-----------|
| `test_npi_auto_detect_unsupported` | inputSpec lacks `nextflow_version` | Returns `False` |
| `test_npi_auto_detect_supported` | inputSpec has `nextflow_version` | Returns `True` |
| `test_npi_auto_detect_exception` | DXAPIError 404 | Returns `False` (no crash) |
| `test_npi_auto_detect_empty_inputspec` | Empty `inputSpec` list | Returns `False` |
| `test_npi_auto_detect_auth_error` | DXAPIError 401 | Returns `False` (no crash) |

### Integration Tests — `TestDXBuildNextflowApplet`

| Test | Scenario | Assertion |
|------|----------|-----------|
| `test_dx_build_nextflow_with_default_version` | Build without `--nextflow-version` | `details.nextflowVersion == "25.10"` |
| `test_dx_build_nextflow_with_explicit_version` | `--nextflow-version 25.10` | `details.nextflowVersion == "25.10"` |
| `test_dx_build_nextflow_with_invalid_version` | `--nextflow-version 99.99` | Exit code 3, stderr "not supported" |
| `test_dx_build_nextflow_version_without_nextflow_flag` | `--nextflow-version` without `--nextflow` | Exit code 2 |
| `test_dx_build_nextflow_with_deprecated_version` | `--nextflow-version 24.10` | Builds OK, stderr "deprecated", details records "24.10" |
| `test_dx_build_nextflow_npi_with_version_warning` | NPI build with version (requires `TEST_RUN_JOBS`) | Handles both NPI-supports and NPI-unsupported cases |

### Known Gaps

- **NPI build path end-to-end**: Full NPI flow (`--repository` + `--nextflow-version`) only runs when `TEST_RUN_JOBS=true` and depends on NPI deployment state.
- **NPI asset verification**: The NPI worker uses its own dxpy's asset files, not the local dxpy's. We cannot verify the resulting applet's assets match the *local* dxpy when building via NPI — the worker's dxpy version determines what gets bundled.
- **Concurrent version resolution**: No test for thread safety of `_load_versions_manifest()` (not a current concern — CLI is single-threaded).
- **Manifest hot-reload**: No test for what happens if `versions.json` changes between calls within the same process (not a realistic scenario).
