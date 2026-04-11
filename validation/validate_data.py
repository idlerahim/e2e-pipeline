"""
============================================================
RecoMart Data Pipeline — Data Profiling & Validation (Task 4)
============================================================
Automated validation checks on all raw ingested datasets:
  • Missing values detection
  • Duplicate entry detection
  • Schema validation (expected columns & dtypes)
  • Range / format checks (e.g. review_score ∈ [1,5])
  • Data profiling (stats, distributions, cardinality)
  • Generates a Data Quality Report (JSON + Markdown)

Usage:
    python -m validation.validate_data
    python -m validation.validate_data --config config/pipeline_config.yaml
"""

import os
import sys
import json
import glob
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from collections import OrderedDict
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.utils import (
    load_config,
    setup_logger,
    get_project_root,
    ensure_directory,
)


def derive_source_name(filename: str) -> str:
    """Convert source filenames to raw data lake source names."""
    name = os.path.splitext(filename)[0]
    if name.startswith("olist_"):
        name = name[len("olist_") :]
    if name.endswith("_dataset"):
        name = name[: -len("_dataset")]
    return name


def find_latest_ingested_file(raw_layer: str, filename: str) -> str | None:
    """Return the latest ingested raw file path for the given source filename."""
    source_name = derive_source_name(filename)
    pattern = os.path.join(raw_layer, source_name, "csv", "*", filename)
    candidates = glob.glob(pattern)
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)

# ==============================================================
# Expected schemas per dataset
# ==============================================================
EXPECTED_SCHEMAS = {
    "olist_customers_dataset.csv": {
        "columns": ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
        "key_column": "customer_id",
        "unique_columns": ["customer_id"],
    },
    "olist_products_dataset.csv": {
        "columns": ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght",
                     "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"],
        "key_column": "product_id",
        "unique_columns": ["product_id"],
        "numeric_ranges": {
            "product_photos_qty": (0, 50),
            "product_weight_g": (0, 200000),
            "product_length_cm": (0, 200),
            "product_height_cm": (0, 200),
            "product_width_cm": (0, 200),
        },
    },
    "olist_orders_dataset.csv": {
        "columns": ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
                     "order_approved_at", "order_delivered_carrier_date",
                     "order_delivered_customer_date", "order_estimated_delivery_date"],
        "key_column": "order_id",
        "unique_columns": ["order_id"],
        "allowed_values": {
            "order_status": ["delivered", "shipped", "canceled", "unavailable", "invoiced",
                             "processing", "created", "approved"],
        },
    },
    "olist_order_items_dataset.csv": {
        "columns": ["order_id", "order_item_id", "product_id", "seller_id",
                     "shipping_limit_date", "price", "freight_value"],
        "key_column": None,
        "unique_columns": [],
        "numeric_ranges": {
            "price": (0, 100000),
            "freight_value": (0, 10000),
            "order_item_id": (1, 50),
        },
    },
    "olist_order_reviews_dataset.csv": {
        "columns": ["review_id", "order_id", "review_score", "review_comment_title",
                     "review_comment_message", "review_creation_date", "review_answer_timestamp"],
        "key_column": "review_id",
        "unique_columns": [],
        "numeric_ranges": {
            "review_score": (1, 5),
        },
    },
    "olist_order_payments_dataset.csv": {
        "columns": ["order_id", "payment_sequential", "payment_type",
                     "payment_installments", "payment_value"],
        "key_column": None,
        "unique_columns": [],
        "allowed_values": {
            "payment_type": ["credit_card", "boleto", "voucher", "debit_card", "not_defined"],
        },
        "numeric_ranges": {
            "payment_value": (0, 100000),
            "payment_installments": (0, 30),
        },
    },
    "olist_sellers_dataset.csv": {
        "columns": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
        "key_column": "seller_id",
        "unique_columns": ["seller_id"],
    },
    "olist_geolocation_dataset.csv": {
        "columns": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
                     "geolocation_city", "geolocation_state"],
        "key_column": None,
        "unique_columns": [],
        "numeric_ranges": {
            "geolocation_lat": (-60, 10),
            "geolocation_lng": (-80, -30),
        },
    },
    "product_category_name_translation.csv": {
        "columns": ["product_category_name", "product_category_name_english"],
        "key_column": "product_category_name",
        "unique_columns": ["product_category_name"],
    },
}


# ==============================================================
# Validation checks
# ==============================================================
def check_schema(df: pd.DataFrame, expected: dict, filename: str) -> list:
    """Validate column names match the expected schema."""
    issues = []
    expected_cols = set(expected["columns"])
    actual_cols = set(df.columns.tolist())

    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    if missing:
        issues.append({"check": "schema_missing_columns", "severity": "ERROR",
                       "detail": f"Missing columns: {sorted(missing)}"})
    if extra:
        issues.append({"check": "schema_extra_columns", "severity": "WARNING",
                       "detail": f"Unexpected columns: {sorted(extra)}"})
    if not missing and not extra:
        issues.append({"check": "schema_match", "severity": "PASS",
                       "detail": "All expected columns present"})
    return issues


def check_missing_values(df: pd.DataFrame) -> dict:
    """Count and percentage of missing values per column."""
    total = len(df)
    result = {}
    for col in df.columns:
        null_count = int(df[col].isnull().sum())
        result[col] = {
            "missing_count": null_count,
            "missing_pct": round(null_count / total * 100, 2) if total > 0 else 0,
            "completeness_pct": round((total - null_count) / total * 100, 2) if total > 0 else 0,
        }
    return result


def check_duplicates(df: pd.DataFrame, unique_columns: list) -> list:
    """Check for duplicate rows (full and on key columns)."""
    issues = []

    # Full row duplicates
    full_dupes = int(df.duplicated().sum())
    issues.append({
        "check": "full_row_duplicates",
        "severity": "WARNING" if full_dupes > 0 else "PASS",
        "detail": f"{full_dupes:,} full-row duplicates found",
        "count": full_dupes,
    })

    # Key column duplicates
    for col in unique_columns:
        if col in df.columns:
            key_dupes = int(df[col].duplicated().sum())
            issues.append({
                "check": f"duplicate_key_{col}",
                "severity": "ERROR" if key_dupes > 0 else "PASS",
                "detail": f"{key_dupes:,} duplicate values in '{col}'",
                "count": key_dupes,
            })

    return issues


def check_ranges(df: pd.DataFrame, ranges: dict) -> list:
    """Check numeric columns fall within expected ranges."""
    issues = []
    for col, (lo, hi) in ranges.items():
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        below = int((series < lo).sum())
        above = int((series > hi).sum())
        total_violations = below + above

        issues.append({
            "check": f"range_{col}",
            "severity": "ERROR" if total_violations > 0 else "PASS",
            "detail": f"'{col}' expected [{lo}, {hi}]: {below} below, {above} above",
            "violations": total_violations,
            "below_min": below,
            "above_max": above,
        })
    return issues


def check_allowed_values(df: pd.DataFrame, allowed: dict) -> list:
    """Check categorical columns only contain allowed values."""
    issues = []
    for col, vals in allowed.items():
        if col not in df.columns:
            continue
        actual = set(df[col].dropna().unique())
        unexpected = actual - set(vals)
        issues.append({
            "check": f"allowed_values_{col}",
            "severity": "WARNING" if unexpected else "PASS",
            "detail": f"'{col}' unexpected values: {sorted(unexpected)}" if unexpected else f"'{col}' all values valid",
            "unexpected_values": sorted(unexpected) if unexpected else [],
        })
    return issues


def profile_column(series: pd.Series) -> dict:
    """Generate profiling stats for a single column."""
    p = {"dtype": str(series.dtype), "non_null": int(series.count()), "null": int(series.isnull().sum())}

    if pd.api.types.is_numeric_dtype(series):
        desc = series.describe()
        p.update({
            "min": float(desc.get("min", 0)),
            "max": float(desc.get("max", 0)),
            "mean": round(float(desc.get("mean", 0)), 2),
            "std": round(float(desc.get("std", 0)), 2),
            "median": round(float(series.median()), 2) if series.count() > 0 else None,
        })
    else:
        p["unique_count"] = int(series.nunique())
        top_vals = series.value_counts().head(5)
        p["top_values"] = {str(k): int(v) for k, v in top_vals.items()}

    return p


# ==============================================================
# Validate one dataset
# ==============================================================
def validate_dataset(filepath: str, filename: str, schema: dict, logger) -> dict:
    """Run all validation checks on a single dataset."""
    logger.info(f"  Reading: {filepath}")
    df = pd.read_csv(filepath, low_memory=False)
    row_count = len(df)
    col_count = len(df.columns)
    logger.info(f"  Shape: {row_count:,} rows × {col_count} columns")

    report = OrderedDict()
    report["file"] = filename
    report["rows"] = row_count
    report["columns"] = col_count
    report["column_names"] = list(df.columns)

    # 1. Schema check
    schema_issues = check_schema(df, schema, filename)
    report["schema_validation"] = schema_issues
    for iss in schema_issues:
        logger.info(f"    [SCHEMA] {iss['severity']}: {iss['detail']}")

    # 2. Missing values
    missing = check_missing_values(df)
    report["missing_values"] = missing
    total_missing = sum(v["missing_count"] for v in missing.values())
    total_cells = row_count * col_count
    overall_completeness = round((total_cells - total_missing) / total_cells * 100, 2) if total_cells > 0 else 0
    report["overall_completeness_pct"] = overall_completeness
    logger.info(f"    [MISSING] Overall completeness: {overall_completeness}%  ({total_missing:,} missing cells)")

    # 3. Duplicates
    dup_issues = check_duplicates(df, schema.get("unique_columns", []))
    report["duplicate_checks"] = dup_issues
    for iss in dup_issues:
        logger.info(f"    [DUPLICATES] {iss['severity']}: {iss['detail']}")

    # 4. Range checks
    ranges = schema.get("numeric_ranges", {})
    if ranges:
        range_issues = check_ranges(df, ranges)
        report["range_checks"] = range_issues
        for iss in range_issues:
            logger.info(f"    [RANGE] {iss['severity']}: {iss['detail']}")

    # 5. Allowed values checks
    allowed = schema.get("allowed_values", {})
    if allowed:
        allowed_issues = check_allowed_values(df, allowed)
        report["allowed_value_checks"] = allowed_issues
        for iss in allowed_issues:
            logger.info(f"    [VALUES] {iss['severity']}: {iss['detail']}")

    # 6. Column profiling
    profiles = {}
    for col in df.columns:
        profiles[col] = profile_column(df[col])
    report["column_profiles"] = profiles

    # Determine overall status
    all_issues = schema_issues + dup_issues + report.get("range_checks", []) + report.get("allowed_value_checks", [])
    has_error = any(i["severity"] == "ERROR" for i in all_issues)
    has_warning = any(i["severity"] == "WARNING" for i in all_issues)
    report["overall_status"] = "ERROR" if has_error else ("WARNING" if has_warning else "PASS")

    return report


# ==============================================================
# Generate Markdown quality report
# ==============================================================
def generate_markdown_report(all_reports: list, output_path: str):
    """Write a human-readable Markdown data quality report."""
    lines = [
        "# Data Quality Report",
        "",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**Datasets validated:** {len(all_reports)}",
        "",
        "---",
        "",
        "## Summary",
        "",
        "| # | Dataset | Rows | Cols | Completeness | Duplicates | Status |",
        "|---|---------|-----:|-----:|-------------:|-----------:|--------|",
    ]

    for i, r in enumerate(all_reports, 1):
        total_missing = sum(v["missing_count"] for v in r["missing_values"].values())
        full_dupes = next((d["count"] for d in r["duplicate_checks"] if d["check"] == "full_row_duplicates"), 0)
        status_icon = "[OK]" if r["overall_status"] == "PASS" else ("[W]" if r["overall_status"] == "WARNING" else "[FAIL]")
        lines.append(
            f"| {i} | `{r['file']}` | {r['rows']:,} | {r['columns']} | "
            f"{r['overall_completeness_pct']}% | {full_dupes:,} | {status_icon} {r['overall_status']} |"
        )

    lines += ["", "---", ""]

    # Per-dataset details
    for r in all_reports:
        lines += [
            f"## {r['file']}",
            "",
            f"- **Rows:** {r['rows']:,}",
            f"- **Columns:** {r['columns']}",
            f"- **Overall Completeness:** {r['overall_completeness_pct']}%",
            f"- **Status:** {r['overall_status']}",
            "",
        ]

        # Missing values table
        lines += ["### Missing Values", "", "| Column | Missing | % Missing | % Complete |", "|--------|--------:|----------:|-----------:|"]
        for col, mv in r["missing_values"].items():
            if mv["missing_count"] > 0:
                lines.append(f"| `{col}` | {mv['missing_count']:,} | {mv['missing_pct']}% | {mv['completeness_pct']}% |")
        if all(mv["missing_count"] == 0 for mv in r["missing_values"].values()):
            lines.append("| *(none)* | 0 | 0% | 100% |")

        lines.append("")

        # Validation issues
        all_checks = r.get("schema_validation", []) + r.get("duplicate_checks", []) + r.get("range_checks", []) + r.get("allowed_value_checks", [])
        non_pass = [c for c in all_checks if c["severity"] != "PASS"]
        if non_pass:
            lines += ["### Issues Found", "", "| Check | Severity | Detail |", "|-------|----------|--------|"]
            for c in non_pass:
                lines.append(f"| {c['check']} | {c['severity']} | {c['detail']} |")
            lines.append("")

        lines += ["---", ""]

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ==============================================================
# Main
# ==============================================================
def run_validation(config_path: str = None):
    cfg = load_config(config_path)
    logger = setup_logger("validation", cfg, log_filename="validation.log")
    project_root = get_project_root()
    raw_layer = os.path.join(project_root, cfg["paths"]["raw_layer"])
    source_dir = os.path.join(project_root, cfg["paths"]["source_data_dir"])
    reports_dir = ensure_directory(os.path.join(project_root, "reports"))
    logs_dir = ensure_directory(os.path.join(project_root, cfg["paths"]["logs_dir"]))

    logger.info("=" * 70)
    logger.info("DATA PROFILING & VALIDATION - START")
    logger.info(f"  Timestamp : {datetime.now().isoformat()}")
    logger.info(f"  Raw layer  : {raw_layer}")
    logger.info(f"  Source     : {source_dir}")
    logger.info("=" * 70)

    all_reports = []

    for filename, schema in EXPECTED_SCHEMAS.items():
        raw_file = find_latest_ingested_file(raw_layer, filename)
        if raw_file:
            filepath = raw_file
            logger.info(f"Using latest ingested raw file: {filepath}")
        else:
            filepath = os.path.join(source_dir, filename)
            logger.warning(f"Falling back to source dataset file: {filepath}")

        logger.info("-" * 50)
        logger.info(f"Validating: {filename}")

        if not os.path.isfile(filepath):
            logger.error(f"  FILE NOT FOUND - skipping: {filepath}")
            all_reports.append({"file": filename, "overall_status": "ERROR", "detail": "File not found"})
            continue

        try:
            report = validate_dataset(filepath, filename, schema, logger)
            all_reports.append(report)
            logger.info(f"  -> Overall: {report['overall_status']}")
        except Exception as exc:
            logger.exception(f"  [FAIL] FAILED: {exc}")
            all_reports.append({"file": filename, "overall_status": "ERROR", "detail": str(exc)})

    # Save JSON report
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(reports_dir, f"data_quality_report_{ts}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_reports, f, indent=2, default=str)
    logger.info(f"  JSON report: {json_path}")

    # Save Markdown report
    md_path = os.path.join(reports_dir, f"data_quality_report_{ts}.md")
    generate_markdown_report(all_reports, md_path)
    logger.info(f"  Markdown report: {md_path}")

    # Summary
    pass_count = sum(1 for r in all_reports if r.get("overall_status") == "PASS")
    warn_count = sum(1 for r in all_reports if r.get("overall_status") == "WARNING")
    err_count = sum(1 for r in all_reports if r.get("overall_status") == "ERROR")

    logger.info("=" * 70)
    logger.info("DATA PROFILING & VALIDATION - COMPLETE")
    logger.info(f"  Total datasets : {len(all_reports)}")
    logger.info(f"  PASS           : {pass_count}")
    logger.info(f"  WARNING        : {warn_count}")
    logger.info(f"  ERROR          : {err_count}")
    logger.info("=" * 70)

    return all_reports, md_path


def main():
    parser = argparse.ArgumentParser(description="RecoMart Data Profiling & Validation")
    parser.add_argument("--config", type=str, default=None)
    args = parser.parse_args()
    run_validation(config_path=args.config)


if __name__ == "__main__":
    main()
