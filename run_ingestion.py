"""
============================================================
RecoMart Data Pipeline - Unified Ingestion Runner
============================================================
Orchestrates both CSV batch ingestion and REST API ingestion
in a single run.  Can also be used for periodic/scheduled
execution.

Usage:
    python run_ingestion.py                    # Run both
    python run_ingestion.py --mode csv         # CSV only
    python run_ingestion.py --mode api         # API only
    python run_ingestion.py --config <path>    # Custom config
"""

import argparse
import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingestion.ingest_csv import run_csv_ingestion
from ingestion.ingest_api import run_api_ingestion


def main():
    parser = argparse.ArgumentParser(
        description="RecoMart Unified Ingestion Runner"
    )
    parser.add_argument(
        "--mode",
        choices=["all", "csv", "api"],
        default="all",
        help="Which ingestion pipeline to run (default: all)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to pipeline_config.yaml",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("  RecoMart - Data Ingestion Pipeline")
    print(f"  Mode: {args.mode.upper()}")
    print("=" * 70)

    if args.mode in ("all", "csv"):
        print("\n>>> Running CSV Batch Ingestion ...\n")
        csv_results = run_csv_ingestion(config_path=args.config)
        csv_ok = sum(1 for r in csv_results if r.get("status") == "SUCCESS")
        csv_fail = len(csv_results) - csv_ok
        print(f"\n>>> CSV Ingestion complete: {csv_ok} succeeded, {csv_fail} failed.\n")

    if args.mode in ("all", "api"):
        print("\n>>> Running REST API Ingestion ...\n")
        api_results = run_api_ingestion(config_path=args.config)
        api_ok = sum(1 for r in api_results if r.get("status") == "SUCCESS")
        api_fail = len(api_results) - api_ok
        print(f"\n>>> API Ingestion complete: {api_ok} succeeded, {api_fail} failed.\n")

    print("=" * 70)
    print("  All ingestion pipelines finished.")
    print("=" * 70)


if __name__ == "__main__":
    main()
