"""
============================================================
RecoMart Data Pipeline - Utility Functions
============================================================
Shared helpers for configuration loading, logging setup,
directory creation, and metadata generation.
"""

import os
import sys
import yaml
import logging
import logging.handlers
import hashlib
import json
from datetime import datetime, timezone


# ----------------------------------------------------------
# Configuration
# ----------------------------------------------------------
def load_config(config_path: str = None) -> dict:
    """
    Load pipeline configuration from a YAML file.

    Parameters
    ----------
    config_path : str, optional
        Absolute or relative path to the YAML config file.
        Defaults to ``config/pipeline_config.yaml`` relative to
        the project root.

    Returns
    -------
    dict
        Parsed configuration dictionary.
    """
    if config_path is None:
        project_root = get_project_root()
        config_path = os.path.join(project_root, "config", "pipeline_config.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    return cfg


def get_project_root() -> str:
    """Return the absolute path to the project root directory."""
    # Assumes this file lives at  <root>/ingestion/utils.py
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


# ----------------------------------------------------------
# Logging
# ----------------------------------------------------------
def setup_logger(
    name: str,
    cfg: dict,
    log_filename: str = None,
) -> logging.Logger:
    """
    Create and configure a logger with both console and
    rotating-file handlers.

    Parameters
    ----------
    name : str
        Logger name (usually ``__name__``).
    cfg : dict
        Full pipeline configuration dict (must contain a
        ``logging`` section).
    log_filename : str, optional
        Override log file name.  Defaults to
        ``<prefix>_YYYYMMDD.log``.

    Returns
    -------
    logging.Logger
    """
    log_cfg = cfg.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    fmt = log_cfg.get("format", "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    datefmt = log_cfg.get("date_format", "%Y-%m-%d %H:%M:%S")
    prefix = log_cfg.get("log_file_prefix", "ingestion")
    max_bytes = log_cfg.get("max_log_size_mb", 10) * 1024 * 1024
    backup_count = log_cfg.get("backup_count", 5)

    # Ensure logs directory exists
    project_root = get_project_root()
    logs_dir = os.path.join(project_root, cfg["paths"]["logs_dir"])
    ensure_directory(logs_dir)

    if log_filename is None:
        today = datetime.now().strftime("%Y%m%d")
        log_filename = f"{prefix}_{today}.log"

    log_path = os.path.join(logs_dir, log_filename)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers on repeated calls
    if not logger.handlers:
        formatter = logging.Formatter(fmt, datefmt=datefmt)

        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # Rotating file handler
        fh = logging.handlers.RotatingFileHandler(
            log_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        )
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


# ----------------------------------------------------------
# Directory helpers
# ----------------------------------------------------------
def ensure_directory(path: str) -> str:
    """Create the directory tree if it does not exist and return the path."""
    os.makedirs(path, exist_ok=True)
    return path


def build_partition_path(
    base_dir: str,
    source_name: str,
    data_type: str = "csv",
    timestamp: datetime = None,
) -> str:
    """
    Build a partitioned storage path:
        ``<base_dir>/<source_name>/<data_type>/YYYY-MM-DD/``

    Parameters
    ----------
    base_dir : str
        Root of the data lake layer (e.g. ``data_lake/raw``).
    source_name : str
        Logical name of the data source (e.g. ``customers``).
    data_type : str
        Data format or origin type (``csv``, ``api``, etc.).
    timestamp : datetime, optional
        Partition timestamp.  Defaults to *now*.

    Returns
    -------
    str
        Fully-qualified directory path.
    """
    if timestamp is None:
        timestamp = datetime.now()
    date_str = timestamp.strftime("%Y-%m-%d")
    partition = os.path.join(base_dir, source_name, data_type, date_str)
    return ensure_directory(partition)


# ----------------------------------------------------------
# Metadata / checksums
# ----------------------------------------------------------
def compute_file_checksum(filepath: str, algorithm: str = "md5") -> str:
    """Compute a hex-digest checksum of a file."""
    h = hashlib.new(algorithm)
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def write_ingestion_metadata(
    dest_dir: str,
    source_file: str,
    dest_file: str,
    row_count: int,
    checksum: str,
    status: str = "SUCCESS",
    extra: dict = None,
) -> str:
    """
    Write a JSON metadata sidecar file next to the ingested data.

    Returns
    -------
    str
        Path to the metadata file.
    """
    meta = {
        "source_file": source_file,
        "destination_file": dest_file,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "row_count": row_count,
        "checksum_md5": checksum,
        "status": status,
    }
    if extra:
        meta.update(extra)

    meta_path = os.path.join(dest_dir, os.path.basename(dest_file) + ".meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    return meta_path
