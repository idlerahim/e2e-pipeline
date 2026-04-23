"""
============================================================
RecoMart - Lightweight Feature Store (Task 7)
============================================================
Custom lightweight feature store supporting:
  - Feature versioning (snapshot-based)
  - Point-in-time correctness (timestamp-partitioned)
  - Retrieval for training and inference

Architecture:
  - SQLite backend for feature tables
  - Snapshot versioning (each run = new version)
  - Feature registry with metadata
  - Point-in-time retrieval via snapshot timestamps
  - Training/inference APIs

Usage:
    python -m feature_store.feature_store_manager --register
    python -m feature_store.feature_store_manager --query-users <user_id1,user_id2>
    python -m feature_store.feature_store_manager --query-items <item_id1,item_id2>
    python -m feature_store.feature_store_manager --training-set --sample 1000
    python -m feature_store.feature_store_manager --status
"""

import os
import sys
import json
import sqlite3
import argparse
import shutil
import pandas as pd
import numpy as np
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.utils import (
    load_config,
    setup_logger,
    get_project_root,
    ensure_directory,
)


# ==============================================================
# Feature Store Manager
# ==============================================================
class FeatureStoreManager:
    """
    Lightweight feature store with:
      - Snapshot-based versioning
      - Point-in-time correctness
      - Training & inference retrieval
    """

    def __init__(self, config_path=None):
        self.cfg = load_config(config_path)
        self.logger = setup_logger("feature_store", self.cfg, log_filename="feature_store.log")
        self.project_root = get_project_root()
        self.store_dir = ensure_directory(
            os.path.join(self.project_root, "data_lake", "serving", "feature_store")
        )
        self.registry_path = os.path.join(self.store_dir, "store_registry.json")
        self._load_registry()

    # ----------------------------------------------------------
    # Registry management
    # ----------------------------------------------------------
    def _load_registry(self):
        """Load or initialize the store registry."""
        if os.path.exists(self.registry_path):
            with open(self.registry_path, "r") as f:
                self.registry = json.load(f)
        else:
            self.registry = {
                "created_at": datetime.now().isoformat(),
                "snapshots": [],
                "latest_snapshot": None,
                "feature_groups": {},
            }
            self._save_registry()

    def _save_registry(self):
        """Persist registry to disk."""
        with open(self.registry_path, "w") as f:
            json.dump(self.registry, f, indent=2, default=str)

    # ----------------------------------------------------------
    # Snapshot versioning
    # ----------------------------------------------------------
    def register_snapshot(self, source_db_path: str, source_features_dir: str):
        """
        Register a new feature snapshot from the curated features DB.

        Creates a versioned copy in the feature store with a timestamp.
        This supports feature versioning - each run produces a new snapshot.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        snapshot_id = f"v_{timestamp}"
        snapshot_dir = ensure_directory(os.path.join(self.store_dir, snapshot_id))

        self.logger.info(f"[REGISTER] Creating snapshot: {snapshot_id}")
        self.logger.info(f"  Source DB: {source_db_path}")

        # Copy the features DB to this snapshot
        dest_db_path = os.path.join(snapshot_dir, "features.db")
        shutil.copy2(source_db_path, dest_db_path)
        self.logger.info(f"  Copied DB -> {dest_db_path}")

        # Copy feature registry JSON
        source_registry = os.path.join(source_features_dir, "feature_registry.json")
        if os.path.exists(source_registry):
            dest_registry = os.path.join(snapshot_dir, "feature_registry.json")
            shutil.copy2(source_registry, dest_registry)
            self.logger.info(f"  Copied registry -> {dest_registry}")

        # Read feature metadata from the DB
        conn = sqlite3.connect(dest_db_path)
        feature_groups = {}
        for table in ["user_features", "item_features", "interaction_features"]:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            cursor2 = conn.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor2.fetchall()]
            feature_groups[table] = {
                "row_count": row_count,
                "column_count": len(columns),
                "columns": columns,
            }

        # Read feature registry entries
        try:
            reg_df = pd.read_sql("SELECT * FROM feature_registry", conn)
            stored_features = reg_df.to_dict(orient="records")
        except Exception:
            stored_features = []

        conn.close()

        # Update store registry
        snapshot_meta = {
            "snapshot_id": snapshot_id,
            "timestamp": datetime.now().isoformat(),
            "db_path": dest_db_path,
            "source_db": source_db_path,
            "feature_groups": feature_groups,
            "stored_features": stored_features,
        }
        self.registry["snapshots"].append(snapshot_meta)
        self.registry["latest_snapshot"] = snapshot_id
        self.registry["feature_groups"] = feature_groups
        self._save_registry()

        self.logger.info(f"  Snapshot {snapshot_id} registered successfully")
        self.logger.info(f"  Feature groups: {list(feature_groups.keys())}")
        for table, info in feature_groups.items():
            self.logger.info(f"    {table}: {info['row_count']} rows, {info['column_count']} features")

        return snapshot_id

    def list_snapshots(self):
        """List all registered snapshots."""
        return self.registry.get("snapshots", [])

    def get_latest_snapshot(self):
        """Get the latest snapshot ID."""
        return self.registry.get("latest_snapshot")

    def _get_snapshot_db(self, snapshot_id=None):
        """Get the DB path for a specific snapshot (or latest)."""
        if snapshot_id is None:
            snapshot_id = self.get_latest_snapshot()
        if snapshot_id is None:
            raise ValueError("No snapshots registered. Run --register first.")
        for snap in self.registry["snapshots"]:
            if snap["snapshot_id"] == snapshot_id:
                return snap["db_path"]
        raise ValueError(f"Snapshot '{snapshot_id}' not found.")

    # ----------------------------------------------------------
    # Point-in-time retrieval
    # ----------------------------------------------------------
    def get_snapshot_at_time(self, target_time: str):
        """
        Get the latest snapshot that was created at or before `target_time`.

        This supports point-in-time correctness - you can retrieve features
        as they were at a specific point in time, avoiding data leakage.
        """
        target_dt = datetime.fromisoformat(target_time)
        best_snapshot = None
        for snap in self.registry["snapshots"]:
            snap_dt = datetime.fromisoformat(snap["timestamp"])
            if snap_dt <= target_dt:
                best_snapshot = snap
        if best_snapshot is None:
            raise ValueError(f"No snapshot found at or before {target_time}")
        self.logger.info(f"[PIT] Point-in-time lookup for {target_time} -> {best_snapshot['snapshot_id']}")
        return best_snapshot

    # ----------------------------------------------------------
    # Feature retrieval: Training
    # ----------------------------------------------------------
    def get_training_set(self, snapshot_id=None, sample_size=None):
        """
        Retrieve a training dataset by joining user, item, and interaction features.

        Returns a DataFrame suitable for model training.
        """
        db_path = self._get_snapshot_db(snapshot_id)
        self.logger.info(f"[TRAINING] Building training set from {db_path}")

        conn = sqlite3.connect(db_path)

        query = """
        SELECT
            i.customer_unique_id,
            i.product_id,
            i.rating,
            i.purchase_count,
            i.implicit_signal,
            i.rating_normalized,
            i.user_item_affinity
        FROM interaction_features i
        """

        # query = """
        # SELECT
        #     i.customer_unique_id,
        #     i.product_id,
        #     i.rating,
        #     i.purchase_count,
        #     i.implicit_signal,
        #     i.rating_normalized,
        #     i.user_item_affinity,
        #     u.total_spending,
        #     u.avg_order_value,
        #     u.avg_rating_given,
        #     u.review_count AS user_review_count,
        #     u.distinct_products,
        #     u.distinct_categories,
        #     u.preferred_category,
        #     u.recency_days,
        #     u.purchase_freq_log,
        #     u.spending_normalized,
        #     u.aov_normalized,
        #     u.customer_state,
        #     it.category_english,
        #     it.total_sold,
        #     it.avg_rating_received,
        #     it.review_count AS item_review_count,
        #     it.avg_price,
        #     it.total_revenue,
        #     it.distinct_buyers,
        #     it.popularity_rank,
        #     it.price_percentile,
        #     it.price_normalized,
        #     it.sold_normalized,
        #     it.rating_normalized AS item_rating_normalized
        # FROM interaction_features i
        # LEFT JOIN user_features u ON i.customer_unique_id = u.customer_unique_id
        # LEFT JOIN item_features it ON i.product_id = it.product_id
        # """

        if sample_size:
            query += f" ORDER BY RANDOM() LIMIT {sample_size}"

        df = pd.read_sql(query, conn)
        conn.close()

        self.logger.info(f"  Training set: {len(df)} rows x {len(df.columns)} features")
        return df

    # ----------------------------------------------------------
    # Feature retrieval: Inference (online serving)
    # ----------------------------------------------------------
    def get_user_features(self, user_ids: list, snapshot_id=None):
        """Retrieve features for specific users (online serving)."""
        db_path = self._get_snapshot_db(snapshot_id)
        conn = sqlite3.connect(db_path)
        placeholders = ",".join(["?" for _ in user_ids])
        query = f"SELECT * FROM user_features WHERE customer_unique_id IN ({placeholders})"
        df = pd.read_sql(query, conn, params=user_ids)
        conn.close()
        self.logger.info(f"[INFERENCE] Retrieved {len(df)} user feature rows for {len(user_ids)} user IDs")
        return df

    def get_item_features(self, item_ids: list, snapshot_id=None):
        """Retrieve features for specific items (online serving)."""
        db_path = self._get_snapshot_db(snapshot_id)
        conn = sqlite3.connect(db_path)
        placeholders = ",".join(["?" for _ in item_ids])
        query = f"SELECT * FROM item_features WHERE product_id IN ({placeholders})"
        df = pd.read_sql(query, conn, params=item_ids)
        conn.close()
        self.logger.info(f"[INFERENCE] Retrieved {len(df)} item feature rows for {len(item_ids)} item IDs")
        return df

    def get_interaction_features(self, user_id: str, snapshot_id=None):
        """Retrieve all interactions for a specific user."""
        db_path = self._get_snapshot_db(snapshot_id)
        conn = sqlite3.connect(db_path)
        query = "SELECT * FROM interaction_features WHERE customer_unique_id = ?"
        df = pd.read_sql(query, conn, params=[user_id])
        conn.close()
        self.logger.info(f"[INFERENCE] Retrieved {len(df)} interactions for user {user_id[:12]}...")
        return df

    # ----------------------------------------------------------
    # Status report
    # ----------------------------------------------------------
    def print_status(self):
        """Print feature store status summary."""
        print("=" * 70)
        print("  RecoMart Feature Store - Status")
        print("=" * 70)
        print(f"  Store location : {self.store_dir}")
        print(f"  Created        : {self.registry.get('created_at', 'N/A')}")

        snapshots = self.list_snapshots()
        print(f"  Snapshots      : {len(snapshots)}")
        print(f"  Latest         : {self.get_latest_snapshot() or 'None'}")

        if snapshots:
            print("\n  Snapshot History:")
            for i, snap in enumerate(snapshots):
                print(f"    [{i+1}] {snap['snapshot_id']}  ({snap['timestamp']})")
                for table, info in snap.get("feature_groups", {}).items():
                    print(f"        {table}: {info['row_count']} rows, {info['column_count']} features")

        print("=" * 70)


# ==============================================================
# Main CLI
# ==============================================================
def main():
    parser = argparse.ArgumentParser(description="RecoMart Lightweight Feature Store")
    parser.add_argument("--config", type=str, default=None, help="Path to pipeline_config.yaml")
    parser.add_argument("--register", action="store_true", help="Register a new feature snapshot")
    parser.add_argument("--status", action="store_true", help="Print feature store status")
    parser.add_argument("--training-set", action="store_true", help="Generate a training set")
    parser.add_argument("--sample", type=int, default=None, help="Sample size for training set")
    parser.add_argument("--query-users", type=str, default=None, help="Comma-separated user IDs to query")
    parser.add_argument("--query-items", type=str, default=None, help="Comma-separated item IDs to query")
    parser.add_argument("--pit", type=str, default=None, help="Point-in-time timestamp (ISO format)")
    args = parser.parse_args()

    store = FeatureStoreManager(config_path=args.config)

    if args.register:
        # Find latest features DB from Task 6
        features_base = os.path.join(store.project_root, "data_lake", "curated", "features")
        partitions = sorted([d for d in os.listdir(features_base)
                             if os.path.isdir(os.path.join(features_base, d))])
        if not partitions:
            print("ERROR: No feature partitions found. Run Task 6 first.")
            return
        latest = partitions[-1]
        features_dir = os.path.join(features_base, latest)
        db_path = os.path.join(features_dir, "features.db")
        if not os.path.exists(db_path):
            print(f"ERROR: features.db not found at {db_path}")
            return
        snapshot_id = store.register_snapshot(db_path, features_dir)
        print(f"\n[OK] Snapshot registered: {snapshot_id}")

    if args.status:
        store.print_status()

    if args.training_set:
        snapshot_id = None
        if args.pit:
            snap = store.get_snapshot_at_time(args.pit)
            snapshot_id = snap["snapshot_id"]
        df = store.get_training_set(snapshot_id=snapshot_id, sample_size=args.sample)
        # Save training set
        out_dir = ensure_directory(os.path.join(store.project_root, "data_lake", "serving", "training_sets"))
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(out_dir, f"training_set_{ts}.csv")
        df.to_csv(out_path, index=False)
        print(f"\n[OK] Training set saved: {out_path} ({len(df)} rows x {len(df.columns)} features)")

    if args.query_users:
        user_ids = [uid.strip() for uid in args.query_users.split(",")]
        df = store.get_user_features(user_ids)
        print(f"\n--- User Features ({len(df)} rows) ---")
        print(df.to_string(index=False))

    if args.query_items:
        item_ids = [iid.strip() for iid in args.query_items.split(",")]
        df = store.get_item_features(item_ids)
        print(f"\n--- Item Features ({len(df)} rows) ---")
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
