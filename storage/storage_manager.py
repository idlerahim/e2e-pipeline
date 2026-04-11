"""
============================================================
RecoMart Data Pipeline — Data Lake Storage Manager
============================================================
Utilities for inspecting, cataloging, and managing the local
data lake.  Provides a single-command audit of every file
stored in the raw layer, including metadata, sizes, and
partition details.

Deliverables for Task 3:
  • Programmatic creation of the data lake folder hierarchy
  • Catalog / manifest generation for all stored assets
  • Storage statistics (files, sizes, partitions)

Usage:
    python -m storage.storage_manager                # Generate catalog
    python -m storage.storage_manager --verify       # Verify checksums
    python -m storage.storage_manager --tree         # Print tree view
"""

import os
import sys
import json
import argparse
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.utils import (
    load_config,
    setup_logger,
    get_project_root,
    ensure_directory,
    compute_file_checksum,
)


# ----------------------------------------------------------
# Constants for the data lake layer layout
# ----------------------------------------------------------
LAKE_LAYERS = {
    "raw":       "Unprocessed data exactly as ingested from sources",
    "staging":   "Validated and lightly cleaned data (post-quality checks)",
    "curated":   "Fully cleaned, transformed, feature-engineered data",
    "serving":   "Model-ready features and inference outputs",
}


# ----------------------------------------------------------
# Initialize data lake skeleton
# ----------------------------------------------------------
def initialize_data_lake(cfg: dict, logger) -> dict:
    """
    Create the full data lake directory skeleton.

    Creates the following hierarchy under ``data_lake_root``:
        data_lake/
        ├── raw/
        ├── staging/
        ├── curated/
        └── serving/

    Each layer is created even if no data exists yet, so the
    structure is ready for downstream pipeline stages.

    Returns
    -------
    dict
        Mapping of layer name → absolute path.
    """
    project_root = get_project_root()
    lake_root = os.path.join(project_root, cfg["paths"]["data_lake_root"])

    created = {}
    for layer, description in LAKE_LAYERS.items():
        layer_path = ensure_directory(os.path.join(lake_root, layer))
        created[layer] = layer_path
        logger.info(f"  Layer '{layer}' → {layer_path}  ({description})")

    return created


# ----------------------------------------------------------
# Catalog / manifest builder
# ----------------------------------------------------------
def build_catalog(raw_layer_path: str, logger) -> list:
    """
    Walk the raw layer and build a catalog of every stored file.

    For each file, records:
      - relative path within the data lake
      - source name, data type, partition date
      - file size in bytes
      - associated metadata (if .meta.json sidecar exists)

    Returns
    -------
    list[dict]
        List of catalog entries.
    """
    catalog = []

    for dirpath, dirnames, filenames in os.walk(raw_layer_path):
        for fname in filenames:
            full_path = os.path.join(dirpath, fname)
            rel_path = os.path.relpath(full_path, raw_layer_path)
            size_bytes = os.path.getsize(full_path)

            # Parse partition components from path
            # Expected: <source>/<type>/<date>/<file>
            parts = rel_path.replace("\\", "/").split("/")
            source_name = parts[0] if len(parts) > 0 else "unknown"
            data_type = parts[1] if len(parts) > 1 else "unknown"
            partition_date = parts[2] if len(parts) > 2 else "unknown"

            entry = {
                "file_name": fname,
                "relative_path": rel_path,
                "absolute_path": full_path,
                "source_name": source_name,
                "data_type": data_type,
                "partition_date": partition_date,
                "size_bytes": size_bytes,
                "size_human": _human_size(size_bytes),
                "is_metadata": fname.endswith(".meta.json"),
            }

            # Load sidecar metadata if present
            if fname.endswith(".meta.json"):
                try:
                    with open(full_path, "r", encoding="utf-8") as f:
                        entry["metadata"] = json.load(f)
                except Exception:
                    entry["metadata"] = None

            catalog.append(entry)

    logger.info(f"  Catalog built: {len(catalog)} files found in raw layer")
    return catalog


def _human_size(nbytes: int) -> str:
    """Convert bytes to a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


# ----------------------------------------------------------
# Verification: checksums
# ----------------------------------------------------------
def verify_checksums(catalog: list, logger) -> dict:
    """
    For every data file that has a sidecar .meta.json with a
    ``checksum_md5``, recompute the checksum and compare.

    Returns
    -------
    dict
        {"verified": int, "mismatched": int, "skipped": int, "details": list}
    """
    results = {"verified": 0, "mismatched": 0, "skipped": 0, "details": []}

    # Build a map: data_file_basename → expected checksum
    # Use basename matching to avoid path separator issues
    meta_map = {}
    for entry in catalog:
        if entry["is_metadata"] and entry.get("metadata"):
            meta = entry["metadata"]
            dest_file = meta.get("destination_file", "")
            expected_md5 = meta.get("checksum_md5", "")
            if dest_file and expected_md5:
                # Key by the directory + basename of the data file
                key = os.path.basename(os.path.normpath(dest_file))
                meta_map[key] = expected_md5

    # Verify each data file
    for entry in catalog:
        if entry["is_metadata"]:
            continue
        key = entry["file_name"]
        expected = meta_map.get(key)
        if expected is None:
            results["skipped"] += 1
            continue

        actual = compute_file_checksum(entry["absolute_path"])
        match = actual == expected
        results["details"].append({
            "file": entry["relative_path"],
            "expected_md5": expected,
            "actual_md5": actual,
            "match": match,
        })
        if match:
            results["verified"] += 1
            logger.info(f"  ✓ MATCH  {entry['relative_path']}")
        else:
            results["mismatched"] += 1
            logger.warning(f"  ✗ MISMATCH  {entry['relative_path']}  expected={expected}  actual={actual}")

    return results


# ----------------------------------------------------------
# Tree printer
# ----------------------------------------------------------
def print_tree(root_path: str, prefix: str = "", max_depth: int = 4, _depth: int = 0):
    """Print a directory tree to stdout."""
    if _depth >= max_depth:
        return
    entries = sorted(os.listdir(root_path))
    dirs = [e for e in entries if os.path.isdir(os.path.join(root_path, e))]
    files = [e for e in entries if os.path.isfile(os.path.join(root_path, e))]

    for i, d in enumerate(dirs):
        connector = "└── " if (i == len(dirs) - 1 and not files) else "├── "
        print(f"{prefix}{connector}{d}/")
        extension = "    " if connector == "└── " else "│   "
        print_tree(os.path.join(root_path, d), prefix + extension, max_depth, _depth + 1)

    for i, f in enumerate(files):
        connector = "└── " if i == len(files) - 1 else "├── "
        size = _human_size(os.path.getsize(os.path.join(root_path, f)))
        print(f"{prefix}{connector}{f}  ({size})")


# ----------------------------------------------------------
# Storage statistics
# ----------------------------------------------------------
def compute_statistics(catalog: list) -> dict:
    """Aggregate storage statistics from the catalog."""
    stats = {
        "total_files": len(catalog),
        "data_files": 0,
        "metadata_files": 0,
        "total_size_bytes": 0,
        "by_source": defaultdict(lambda: {"files": 0, "size_bytes": 0, "rows": 0}),
        "by_type": defaultdict(lambda: {"files": 0, "size_bytes": 0}),
        "partitions": set(),
    }

    for entry in catalog:
        stats["total_size_bytes"] += entry["size_bytes"]
        stats["partitions"].add(entry["partition_date"])

        src = stats["by_source"][entry["source_name"]]
        src["files"] += 1
        src["size_bytes"] += entry["size_bytes"]

        tp = stats["by_type"][entry["data_type"]]
        tp["files"] += 1
        tp["size_bytes"] += entry["size_bytes"]

        if entry["is_metadata"]:
            stats["metadata_files"] += 1
            if entry.get("metadata"):
                src["rows"] = max(src["rows"], entry["metadata"].get("row_count", 0))
        else:
            stats["data_files"] += 1

    stats["total_size_human"] = _human_size(stats["total_size_bytes"])
    stats["partitions"] = sorted(stats["partitions"])

    # Convert defaultdicts to regular dicts for JSON serialization
    stats["by_source"] = {k: dict(v) for k, v in stats["by_source"].items()}
    stats["by_type"] = {k: dict(v) for k, v in stats["by_type"].items()}

    return stats


# ----------------------------------------------------------
# Main
# ----------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="RecoMart Data Lake Storage Manager")
    parser.add_argument("--config", type=str, default=None, help="Path to pipeline_config.yaml")
    parser.add_argument("--verify", action="store_true", help="Verify MD5 checksums of all data files")
    parser.add_argument("--tree", action="store_true", help="Print a tree view of the data lake")
    args = parser.parse_args()

    cfg = load_config(args.config)
    logger = setup_logger("storage_manager", cfg, log_filename="storage_manager.log")
    project_root = get_project_root()

    print("=" * 70)
    print("  RecoMart — Data Lake Storage Manager")
    print("=" * 70)

    # 1. Initialize / ensure all layers exist
    print("\n[1] Initializing data lake layers ...")
    layers = initialize_data_lake(cfg, logger)

    # 2. Build catalog of raw layer
    raw_path = layers["raw"]
    print(f"\n[2] Building catalog for raw layer: {raw_path}")
    catalog = build_catalog(raw_path, logger)

    # 3. Compute statistics
    print("\n[3] Storage Statistics:")
    stats = compute_statistics(catalog)
    print(f"    Total files      : {stats['total_files']}")
    print(f"    Data files       : {stats['data_files']}")
    print(f"    Metadata files   : {stats['metadata_files']}")
    print(f"    Total size       : {stats['total_size_human']}")
    print(f"    Partitions       : {stats['partitions']}")
    print(f"    Sources          : {list(stats['by_source'].keys())}")
    print(f"    Types            : {list(stats['by_type'].keys())}")

    # 4. Save catalog + stats
    catalog_dir = ensure_directory(os.path.join(project_root, cfg["paths"]["logs_dir"]))
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    catalog_path = os.path.join(catalog_dir, f"data_lake_catalog_{ts}.json")
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, default=str)
    print(f"\n    Catalog saved: {catalog_path}")

    stats_path = os.path.join(catalog_dir, f"data_lake_stats_{ts}.json")
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, default=str)
    print(f"    Stats saved  : {stats_path}")

    # 5. Optional: verify checksums
    if args.verify:
        print("\n[4] Verifying checksums ...")
        vresult = verify_checksums(catalog, logger)
        print(f"    Verified   : {vresult['verified']}")
        print(f"    Mismatched : {vresult['mismatched']}")
        print(f"    Skipped    : {vresult['skipped']}")

    # 6. Optional: tree view
    if args.tree:
        lake_root = os.path.join(project_root, cfg["paths"]["data_lake_root"])
        print(f"\n[5] Data Lake Tree: {lake_root}")
        print(f"    {os.path.basename(lake_root)}/")
        print_tree(lake_root, prefix="    ")

    print("\n" + "=" * 70)
    print("  Storage Manager complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
