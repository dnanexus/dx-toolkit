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
