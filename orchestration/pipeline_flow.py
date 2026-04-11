#!/usr/bin/env python3
"""
RocoMart Prefect Pipeline Orchestrator

Orchestrates the complete RocoMart end-to-end pipeline:
  1. Data ingestion
  2. Data validation
  3. Data preparation
  4. Feature engineering
  5. Feature store registration + training set generation
  6. Model training

This flow can run locally or be deployed via Prefect.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path for task imports
sys.path.append(str(Path(__file__).parent.parent))

from prefect import flow, task, get_run_logger

from ingestion.ingest_csv import run_csv_ingestion
from ingestion.ingest_api import run_api_ingestion
from validation.validate_data import run_validation
from preparation.prepare_data import run_preparation
from transformation.feature_engineering import run_feature_engineering
from feature_store.feature_store_manager import FeatureStoreManager
from models.model_training import main as run_model_training


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_config_path(config_path: str | None = None) -> str:
    project_root = get_project_root()
    if config_path:
        return str(Path(config_path).expanduser())
    return str(project_root / "config" / "pipeline_config.yaml")


@task(retries=1, retry_delay_seconds=timedelta(seconds=5), name="Data Ingestion")
def run_data_ingestion(config_path: str | None = None) -> dict:
    logger = get_run_logger()
    logger.info("Starting data ingestion stage")

    cfg_path = get_config_path(config_path)
    csv_results = run_csv_ingestion(config_path=cfg_path)
    api_results = run_api_ingestion(config_path=cfg_path)

    csv_success = all(r.get("status") == "SUCCESS" for r in csv_results)
    api_success = all(r.get("status") == "SUCCESS" for r in api_results)
    status = "SUCCESS" if csv_success and api_success else "FAILED"

    logger.info(f"CSV ingestion success: {csv_success}")
    logger.info(f"API ingestion success: {api_success}")

    return {
        "stage": "Data Ingestion",
        "status": status,
        "csv_results": csv_results,
        "api_results": api_results,
    }


@task(retries=1, retry_delay_seconds=timedelta(seconds=5), name="Data Validation")
def run_data_validation(config_path: str | None = None) -> dict:
    logger = get_run_logger()
    logger.info("Starting data validation stage")

    cfg_path = get_config_path(config_path)
    reports, report_path = run_validation(config_path=cfg_path)
    validation_ok = all(r.get("overall_status") != "ERROR" for r in reports)

    logger.info(f"Validation status: {'PASS/WARNING' if validation_ok else 'ERROR'}")
    logger.info(f"Validation report saved to: {report_path}")

    return {
        "stage": "Data Validation",
        "status": "SUCCESS" if validation_ok else "FAILED",
        "report_path": report_path,
        "reports": reports,
    }


@task(retries=1, retry_delay_seconds=timedelta(seconds=5), name="Data Preparation")
def run_data_preparation(config_path: str | None = None) -> dict:
    logger = get_run_logger()
    logger.info("Starting data preparation stage")

    cfg_path = get_config_path(config_path)
    try:
        run_preparation(config_path=cfg_path)
        status = "SUCCESS"
    except Exception as exc:
        logger.error(f"Data preparation failed: {exc}")
        status = "FAILED"

    return {
        "stage": "Data Preparation",
        "status": status,
    }


@task(retries=1, retry_delay_seconds=timedelta(seconds=5), name="Feature Engineering")
def run_feature_engineering_stage(config_path: str | None = None) -> dict:
    logger = get_run_logger()
    logger.info("Starting feature engineering stage")

    cfg_path = get_config_path(config_path)
    try:
        run_feature_engineering(config_path=cfg_path)
        status = "SUCCESS"
    except Exception as exc:
        logger.error(f"Feature engineering failed: {exc}")
        status = "FAILED"

    return {
        "stage": "Feature Engineering",
        "status": status,
    }


@task(retries=1, retry_delay_seconds=timedelta(seconds=5), name="Feature Store")
def run_feature_store_stage(config_path: str | None = None) -> dict:
    logger = get_run_logger()
    logger.info("Starting feature store stage")

    cfg_path = get_config_path(config_path)
    manager = FeatureStoreManager(config_path=cfg_path)
    features_base = Path(manager.project_root) / "data_lake" / "curated" / "features"

    if not features_base.exists():
        raise FileNotFoundError(f"Feature engineering output path not found: {features_base}")

    partitions = sorted([p for p in features_base.iterdir() if p.is_dir()])
    if not partitions:
        raise FileNotFoundError("No feature partitions found. Run feature engineering first.")

    latest_partition = partitions[-1]
    features_db = latest_partition / "features.db"
    if not features_db.exists():
        raise FileNotFoundError(f"Expected features database not found: {features_db}")

    snapshot_id = manager.register_snapshot(str(features_db), str(latest_partition))
    logger.info(f"Feature store snapshot registered: {snapshot_id}")

    training_df = manager.get_training_set()
    training_dir = Path(manager.project_root) / "data_lake" / "serving" / "training_sets"
    training_dir.mkdir(parents=True, exist_ok=True)
    training_path = training_dir / f"training_set_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    training_df.to_csv(training_path, index=False)

    logger.info(f"Training set generated at: {training_path}")
    return {
        "stage": "Feature Store",
        "status": "SUCCESS",
        "snapshot_id": snapshot_id,
        "training_set_path": str(training_path),
    }


@task(retries=0, name="Model Training")
def run_model_training_stage(config_path: str | None = None) -> dict:
    logger = get_run_logger()
    logger.info("Starting model training stage")

    cfg_path = get_config_path(config_path)
    try:
        run_model_training()
        status = "SUCCESS"
    except Exception as exc:
        logger.error(f"Model training failed: {exc}")
        status = "FAILED"

    return {
        "stage": "Model Training",
        "status": status,
    }


def generate_pipeline_report(stage_results: list[dict]) -> str:
    project_root = get_project_root()
    reports_dir = project_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / f"pipeline_execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

    completed = sum(1 for r in stage_results if r["status"] == "SUCCESS")
    total = len(stage_results)
    success_rate = completed / total * 100 if total else 0

    lines = [
        "# RocoMart Pipeline Execution Report",
        "",
        f"**Timestamp:** {datetime.now().isoformat()}",
        f"**Success Rate:** {success_rate:.1f}% ({completed}/{total})",
        "",
        "## Stage Results",
        "",
    ]

    for result in stage_results:
        status_symbol = "✅" if result["status"] == "SUCCESS" else "❌"
        lines.append(f"- **{result['stage']}**: {status_symbol} {result['status']}")
        if result.get("training_set_path"):
            lines.append(f"  - Training set: `{result['training_set_path']}`")
        if result.get("snapshot_id"):
            lines.append(f"  - Snapshot: `{result['snapshot_id']}`")

    lines += [
        "",
        "## Notes",
        "",
        "- Non-critical failures are allowed for model training if the core pipeline succeeds.",
        "- Review the logs under `logs/` for detailed stage execution.",
    ]

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return str(report_path)


@flow(name="RocoMart_Data_Pipeline", log_prints=True)
def rocomart_data_pipeline(config_path: str | None = None):
    logger = get_run_logger()
    logger.info("🚀 Starting RocoMart Data Pipeline")

    ingestion_result = run_data_ingestion(config_path=config_path)
    validation_result = run_data_validation(config_path=config_path)
    preparation_result = run_data_preparation(config_path=config_path)
    engineering_result = run_feature_engineering_stage(config_path=config_path)
    store_result = run_feature_store_stage(config_path=config_path)
    training_result = run_model_training_stage(config_path=config_path)

    stage_results = [
        ingestion_result,
        validation_result,
        preparation_result,
        engineering_result,
        store_result,
        training_result,
    ]

    report_path = generate_pipeline_report(stage_results)
    logger.info(f"📄 Pipeline report saved to: {report_path}")

    critical_failures = [
        r for r in stage_results if r["stage"] != "Model Training" and r["status"] != "SUCCESS"
    ]

    if critical_failures:
        logger.error("One or more critical pipeline stages failed.")
        raise RuntimeError("Critical pipeline stages failed. See report and logs for details.")

    logger.info("🎉 RocoMart Data Pipeline completed successfully!")
    return {
        "status": "SUCCESS",
        "report": report_path,
        "timestamp": datetime.now().isoformat(),
    }


if __name__ == "__main__":
    rocomart_data_pipeline()
