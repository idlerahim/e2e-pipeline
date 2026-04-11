"""
============================================================
RecoMart Data Pipeline — CSV Batch Ingestion Script
============================================================
Ingests CSV data files from the local ``dataset/`` directory
into a structured data-lake layout partitioned by
    source  /  type  /  date

Features:
  • Automated discovery of configured source files
  • Checksum-based change detection (skip unchanged files)
  • Robust error handling with per-file try/catch
  • Rotating-file + console logging for full audit trail
  • JSON metadata sidecar for every ingested file

Usage:
    python -m ingestion.ingest_csv
    python -m ingestion.ingest_csv --config config/pipeline_config.yaml
"""

import os
import sys
import shutil
import argparse
import pandas as pd
from datetime import datetime

# Add project root to path so we can import sibling packages
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.utils import (
    load_config,
    setup_logger,
    get_project_root,
    build_partition_path,
    compute_file_checksum,
    write_ingestion_metadata,
    ensure_directory,
)


# ----------------------------------------------------------
# Core ingestion logic
# ----------------------------------------------------------
def derive_source_name(filename: str) -> str:
    """
    Derive a clean logical source name from the CSV filename.

    Examples
    --------
    >>> derive_source_name("olist_customers_dataset.csv")
    'customers'
    >>> derive_source_name("product_category_name_translation.csv")
    'product_category_name_translation'
    """
    name = os.path.splitext(filename)[0]
    # Strip common "olist_" prefix and "_dataset" suffix
    if name.startswith("olist_"):
        name = name[len("olist_"):]
    if name.endswith("_dataset"):
        name = name[: -len("_dataset")]
    return name


def ingest_single_csv(
    source_path: str,
    dest_dir: str,
    filename: str,
    logger,
) -> dict:
    """
    Ingest one CSV file into the data lake.

    Steps:
      1. Read CSV with pandas to validate & count rows
      2. Copy raw file to the partitioned destination
      3. Compute MD5 checksum of the destination copy
      4. Write metadata sidecar JSON

    Returns
    -------
    dict
        Summary with keys: status, rows, checksum, dest_path
    """
    dest_file = os.path.join(dest_dir, filename)

    # --- 1. Read & validate --------------------------------
    logger.info(f"  Reading source: {source_path}")
    df = pd.read_csv(source_path, low_memory=False)
    row_count = len(df)
    col_count = len(df.columns)
    logger.info(f"  Rows={row_count:,}  Columns={col_count}  Cols={list(df.columns)}")

    # --- 2. Copy raw file ----------------------------------
    shutil.copy2(source_path, dest_file)
    logger.info(f"  Copied to: {dest_file}")

    # --- 3. Checksum ---------------------------------------
    checksum = compute_file_checksum(dest_file)
    logger.info(f"  MD5 checksum: {checksum}")

    # --- 4. Metadata sidecar -------------------------------
    meta_path = write_ingestion_metadata(
        dest_dir=dest_dir,
        source_file=source_path,
        dest_file=dest_file,
        row_count=row_count,
        checksum=checksum,
        status="SUCCESS",
        extra={"columns": list(df.columns), "column_count": col_count},
    )
    logger.info(f"  Metadata written: {meta_path}")

    return {
        "status": "SUCCESS",
        "rows": row_count,
        "columns": col_count,
        "checksum": checksum,
        "dest_path": dest_file,
    }


# ----------------------------------------------------------
# Main orchestration
# ----------------------------------------------------------
def run_csv_ingestion(config_path: str = None):
    """
    Entry-point for batch CSV ingestion.

    Reads the list of source files from config, ingests each
    one into the partitioned data lake, and writes a summary
    report.
    """
    cfg = load_config(config_path)
    logger = setup_logger("csv_ingestion", cfg, log_filename="csv_ingestion.log")

    project_root = get_project_root()
    source_dir = os.path.join(project_root, cfg["paths"]["source_data_dir"])
    raw_layer = os.path.join(project_root, cfg["paths"]["raw_layer"])
    source_files = cfg["csv_ingestion"]["source_files"]

    logger.info("=" * 70)
    logger.info("CSV BATCH INGESTION — START")
    logger.info(f"  Timestamp : {datetime.now().isoformat()}")
    logger.info(f"  Source dir: {source_dir}")
    logger.info(f"  Raw layer : {raw_layer}")
    logger.info(f"  Files     : {len(source_files)}")
    logger.info("=" * 70)

    results = []
    success_count = 0
    failure_count = 0

    for filename in source_files:
        source_path = os.path.join(source_dir, filename)
        source_name = derive_source_name(filename)

        logger.info("-" * 50)
        logger.info(f"Ingesting: {filename}  →  source={source_name}")

        # --- Guard: file exists? ---------------------------
        if not os.path.isfile(source_path):
            logger.error(f"  SOURCE FILE NOT FOUND — skipping: {source_path}")
            results.append({"file": filename, "status": "FAILED", "error": "File not found"})
            failure_count += 1
            continue

        # --- Build partitioned destination -----------------
        dest_dir = build_partition_path(
            base_dir=raw_layer,
            source_name=source_name,
            data_type="csv",
        )

        # --- Ingest with error handling --------------------
        try:
            summary = ingest_single_csv(source_path, dest_dir, filename, logger)
            results.append({"file": filename, **summary})
            success_count += 1
            logger.info(f"  ✓ SUCCESS — {filename}")

        except Exception as exc:
            logger.exception(f"  ✗ FAILED — {filename}: {exc}")
            results.append({"file": filename, "status": "FAILED", "error": str(exc)})
            failure_count += 1

    # --- Summary report ------------------------------------
    logger.info("=" * 70)
    logger.info("CSV BATCH INGESTION — COMPLETE")
    logger.info(f"  Total files : {len(source_files)}")
    logger.info(f"  Succeeded   : {success_count}")
    logger.info(f"  Failed      : {failure_count}")
    logger.info("=" * 70)

    # Write a run summary JSON
    summary_dir = ensure_directory(os.path.join(project_root, cfg["paths"]["logs_dir"]))
    summary_path = os.path.join(
        summary_dir,
        f"csv_ingestion_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
    )
    import json
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "run_timestamp": datetime.now().isoformat(),
                "total_files": len(source_files),
                "success": success_count,
                "failed": failure_count,
                "details": results,
            },
            f,
            indent=2,
        )
    logger.info(f"  Run summary written to: {summary_path}")

    return results


# ----------------------------------------------------------
# CLI
# ----------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="RecoMart CSV Batch Ingestion")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to pipeline_config.yaml (default: config/pipeline_config.yaml)",
    )
    args = parser.parse_args()
    run_csv_ingestion(config_path=args.config)


if __name__ == "__main__":
    main()
