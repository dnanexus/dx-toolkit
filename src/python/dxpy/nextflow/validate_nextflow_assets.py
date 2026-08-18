#!/usr/bin/env python3
"""
Standalone validator for Nextflow asset JSON files.

Checks that every asset file referenced in versions.json:
  - exists on disk
  - is valid JSON
  - contains only well-formed DNAnexus record IDs (^record-[A-Za-z0-9]{24}$)
  - has identical region key sets in the prod and staging variants

Also validates app_asset_projects_ids_{prod,staging}.json for project IDs.

No dxpy dependency — stdlib only. Safe to run without any DX credentials.

Exit code: 0 on success, 1 on any validation failure.
"""

import json
import os
import re
import sys

NEXTFLOW_DIR = os.path.join(os.path.dirname(__file__))
VERSIONS_JSON = os.path.join(NEXTFLOW_DIR, "versions.json")

RECORD_ID_RE = re.compile(r"^record-[A-Za-z0-9]{24}$")
PROJECT_ID_RE = re.compile(r"^project-[A-Za-z0-9]{24}$")

ASSET_KEYS = ("nextaur_assets", "nextflow_assets", "awscli_assets")


def _load_json(filepath):
    """Load and return parsed JSON. Returns (data, error_message)."""
    if not os.path.isfile(filepath):
        return None, f"File not found: {filepath}"
    try:
        with open(filepath) as f:
            return json.load(f), None
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON in {filepath}: {e}"


def validate_asset_file(filepath, id_re, id_type, errors):
    """Validate a single asset JSON file.

    Returns the set of region keys if the file is valid, else None.
    Appends error strings to `errors`.
    """
    data, err = _load_json(filepath)
    if err:
        errors.append(err)
        return None

    if not isinstance(data, dict):
        errors.append(f"{filepath}: expected a JSON object, got {type(data).__name__}")
        return None

    for region, obj_id in data.items():
        if not id_re.match(obj_id):
            errors.append(
                f"{filepath}[{region!r}]: {obj_id!r} is not a valid {id_type} ID "
                f"(expected pattern {id_re.pattern})"
            )

    return set(data.keys())


def validate_assets():
    errors = []

    # ------------------------------------------------------------------
    # 1. Load versions.json
    # ------------------------------------------------------------------
    manifest, err = _load_json(VERSIONS_JSON)
    if err:
        print(f"FAIL  {err}", file=sys.stderr)
        return False

    if "default" not in manifest or "versions" not in manifest:
        errors.append(f"{VERSIONS_JSON}: missing 'default' or 'versions' key")
    else:
        # ------------------------------------------------------------------
        # 2. Validate every version's prod and staging asset files
        # ------------------------------------------------------------------
        for ver, config in manifest["versions"].items():
            for key in ASSET_KEYS:
                prod_filename = config.get(key)
                if not prod_filename:
                    errors.append(f"versions.json[{ver!r}]: missing key {key!r}")
                    continue

                prod_path = os.path.join(NEXTFLOW_DIR, prod_filename)
                staging_filename = prod_filename.replace(".json", ".staging.json")
                staging_path = os.path.join(NEXTFLOW_DIR, staging_filename)

                prod_regions = validate_asset_file(
                    prod_path, RECORD_ID_RE, "record", errors
                )
                staging_regions = validate_asset_file(
                    staging_path, RECORD_ID_RE, "record", errors
                )

                # Region-set consistency between prod and staging
                if prod_regions is not None and staging_regions is not None:
                    only_in_prod = prod_regions - staging_regions
                    only_in_staging = staging_regions - prod_regions
                    if only_in_prod:
                        errors.append(
                            f"Region mismatch for {ver} / {key}: "
                            f"regions in prod but not staging: {sorted(only_in_prod)}"
                        )
                    if only_in_staging:
                        errors.append(
                            f"Region mismatch for {ver} / {key}: "
                            f"regions in staging but not prod: {sorted(only_in_staging)}"
                        )

    # ------------------------------------------------------------------
    # 3. Validate app_asset_projects_ids_{prod,staging}.json
    # ------------------------------------------------------------------
    for filename in (
        "app_asset_projects_ids_prod.json",
        "app_asset_projects_ids_staging.json",
    ):
        filepath = os.path.join(NEXTFLOW_DIR, filename)
        validate_asset_file(filepath, PROJECT_ID_RE, "project", errors)

    # ------------------------------------------------------------------
    # 4. Report
    # ------------------------------------------------------------------
    if errors:
        for err in errors:
            print(f"FAIL  {err}", file=sys.stderr)
        return False

    print(f"OK    All Nextflow asset JSON files passed validation.", file=sys.stdout)
    return True


if __name__ == "__main__":
    sys.exit(0 if validate_assets() else 1)
