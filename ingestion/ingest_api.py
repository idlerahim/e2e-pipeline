"""
============================================================
RecoMart Data Pipeline — REST API Ingestion Script
============================================================
Demonstrates ingesting product data from a REST API
(https://fakestoreapi.com) into the data-lake raw layer.

Features:
  • Configurable endpoint list from YAML config
  • Exponential-backoff retry with configurable max retries
  • Request timeout support
  • JSON response → CSV conversion for downstream compatibility
  • Checksum + metadata sidecar for every ingested file
  • Rotating-file + console logging for full audit trail

Usage:
    python -m ingestion.ingest_api
    python -m ingestion.ingest_api --config config/pipeline_config.yaml
"""

import os
import sys
import json
import time
import argparse
import requests
import pandas as pd
from datetime import datetime, timezone

# Add project root to path
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
# Retry-enabled HTTP GET
# ----------------------------------------------------------
def fetch_with_retry(
    url: str,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    backoff_factor: float = 2.0,
    timeout: int = 30,
    logger=None,
) -> requests.Response:
    """
    Perform an HTTP GET with exponential-backoff retry.

    Parameters
    ----------
    url : str
        Full URL to fetch.
    max_retries : int
        Maximum number of retry attempts.
    retry_delay : float
        Initial delay (seconds) between retries.
    backoff_factor : float
        Multiplier applied to the delay after each retry.
    timeout : int
        Request timeout in seconds.
    logger : logging.Logger, optional

    Returns
    -------
    requests.Response
        Successful HTTP response object.

    Raises
    ------
    requests.exceptions.RequestException
        If all retry attempts are exhausted.
    """
    delay = retry_delay
    last_exception = None

    for attempt in range(1, max_retries + 1):
        try:
            if logger:
                logger.info(f"  HTTP GET {url}  (attempt {attempt}/{max_retries})")
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            if logger:
                logger.info(f"  Response: {response.status_code} - {len(response.content)} bytes")
            return response

        except requests.exceptions.RequestException as exc:
            last_exception = exc
            if logger:
                logger.warning(
                    f"  Attempt {attempt} failed: {exc}"
                    + (f" — retrying in {delay:.1f}s" if attempt < max_retries else " — no more retries")
                )
            if attempt < max_retries:
                time.sleep(delay)
                delay *= backoff_factor

    raise last_exception


# ----------------------------------------------------------
# Ingest one API endpoint
# ----------------------------------------------------------
def ingest_api_endpoint(
    base_url: str,
    endpoint_name: str,
    endpoint_path: str,
    dest_dir: str,
    api_cfg: dict,
    logger,
) -> dict:
    """
    Fetch data from a REST API endpoint, convert to CSV,
    and store in the data lake.

    Steps:
      1. HTTP GET with retry
      2. Parse JSON response
      3. Normalize into a pandas DataFrame
      4. Save as CSV + raw JSON
      5. Compute checksum & write metadata

    Returns
    -------
    dict
        Summary with keys: status, rows, checksum, dest_path
    """
    url = base_url.rstrip("/") + endpoint_path

    # --- 1. Fetch -----------------------------------------
    response = fetch_with_retry(
        url=url,
        max_retries=api_cfg.get("max_retries", 3),
        retry_delay=api_cfg.get("retry_delay_seconds", 2),
        backoff_factor=api_cfg.get("retry_backoff_factor", 2),
        timeout=api_cfg.get("request_timeout_seconds", 30),
        logger=logger,
    )

    # --- 2. Parse JSON ------------------------------------
    data = response.json()

    # Save raw JSON
    json_filename = f"{endpoint_name}_raw.json"
    json_path = os.path.join(dest_dir, json_filename)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"  Raw JSON saved: {json_path}")

    # --- 3. Normalize to DataFrame ------------------------
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        # List of objects → flatten nested keys
        df = pd.json_normalize(data)
    elif isinstance(data, dict):
        # Single object — wrap in list and normalize
        df = pd.json_normalize([data])
    elif isinstance(data, list):
        # Scalar list (e.g. categories endpoint returns ["electronics", ...])
        df = pd.DataFrame({endpoint_name: data})
    else:
        # Fallback for unexpected types
        df = pd.DataFrame({"value": [data]})

    row_count = len(df)
    col_count = len(df.columns)
    logger.info(f"  Parsed: Rows={row_count:,}  Columns={col_count}  Cols={list(df.columns)}")

    # --- 4. Save as CSV -----------------------------------
    csv_filename = f"{endpoint_name}.csv"
    csv_path = os.path.join(dest_dir, csv_filename)
    df.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info(f"  CSV saved: {csv_path}")

    # --- 5. Checksum + metadata ---------------------------
    checksum = compute_file_checksum(csv_path)
    logger.info(f"  MD5 checksum: {checksum}")

    meta_path = write_ingestion_metadata(
        dest_dir=dest_dir,
        source_file=url,
        dest_file=csv_path,
        row_count=row_count,
        checksum=checksum,
        status="SUCCESS",
        extra={
            "columns": list(df.columns),
            "column_count": col_count,
            "http_status": response.status_code,
            "content_length": len(response.content),
            "api_endpoint": endpoint_path,
        },
    )
    logger.info(f"  Metadata written: {meta_path}")

    return {
        "status": "SUCCESS",
        "rows": row_count,
        "columns": col_count,
        "checksum": checksum,
        "dest_path": csv_path,
    }


# ----------------------------------------------------------
# Main orchestration
# ----------------------------------------------------------
def run_api_ingestion(config_path: str = None):
    """
    Entry-point for REST API data ingestion.

    Iterates over configured endpoints, fetches each with
    retry logic, converts to CSV, and stores in the data lake.
    """
    cfg = load_config(config_path)
    logger = setup_logger("api_ingestion", cfg, log_filename="api_ingestion.log")

    project_root = get_project_root()
    raw_layer = os.path.join(project_root, cfg["paths"]["raw_layer"])
    api_cfg = cfg["api_ingestion"]
    base_url = api_cfg["base_url"]
    endpoints = api_cfg["endpoints"]

    logger.info("=" * 70)
    logger.info("REST API INGESTION - START")
    logger.info(f"  Timestamp : {datetime.now().isoformat()}")
    logger.info(f"  Base URL  : {base_url}")
    logger.info(f"  Endpoints : {list(endpoints.keys())}")
    logger.info(f"  Raw layer : {raw_layer}")
    logger.info("=" * 70)

    results = []
    success_count = 0
    failure_count = 0

    for ep_name, ep_path in endpoints.items():
        logger.info("-" * 50)
        logger.info(f"Ingesting API endpoint: {ep_name}  ->  {ep_path}")

        dest_dir = build_partition_path(
            base_dir=raw_layer,
            source_name=ep_name,
            data_type="api",
        )

        try:
            summary = ingest_api_endpoint(
                base_url=base_url,
                endpoint_name=ep_name,
                endpoint_path=ep_path,
                dest_dir=dest_dir,
                api_cfg=api_cfg,
                logger=logger,
            )
            results.append({"endpoint": ep_name, **summary})
            success_count += 1
            logger.info(f"  [OK] SUCCESS - {ep_name}")

        except Exception as exc:
            logger.exception(f"  [FAIL] FAILED - {ep_name}: {exc}")
            results.append({"endpoint": ep_name, "status": "FAILED", "error": str(exc)})
            failure_count += 1

    # --- Summary report ------------------------------------
    logger.info("=" * 70)
    logger.info("REST API INGESTION - COMPLETE")
    logger.info(f"  Total endpoints : {len(endpoints)}")
    logger.info(f"  Succeeded       : {success_count}")
    logger.info(f"  Failed          : {failure_count}")
    logger.info("=" * 70)

    # Write a run summary JSON
    summary_dir = ensure_directory(os.path.join(project_root, cfg["paths"]["logs_dir"]))
    summary_path = os.path.join(
        summary_dir,
        f"api_ingestion_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
    )
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "run_timestamp": datetime.now().isoformat(),
                "base_url": base_url,
                "total_endpoints": len(endpoints),
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
    parser = argparse.ArgumentParser(description="RecoMart REST API Ingestion")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to pipeline_config.yaml (default: config/pipeline_config.yaml)",
    )
    args = parser.parse_args()
    run_api_ingestion(config_path=args.config)


if __name__ == "__main__":
    main()
