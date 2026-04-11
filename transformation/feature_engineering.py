"""
============================================================
RecoMart — Feature Engineering & Transformation (Task 6)
============================================================
Builds engineered features for the recommendation model:
  • User-level features  (purchase behavior, preferences)
  • Item-level features  (popularity, ratings, one-hot categories)
  • Interaction features (enhanced user-item pairs)
  • Applies: normalization, log transforms, one-hot encoding
  • Stores results as CSV + SQLite database

Usage:
    python -m transformation.feature_engineering
"""

import os
import sys
import json
import sqlite3
import argparse
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
# Helper: Min-Max normalization
# ==============================================================
def min_max_normalize(series: pd.Series) -> pd.Series:
    """Scale a numeric series to [0, 1]."""
    smin, smax = series.min(), series.max()
    if smax == smin:
        return pd.Series(0.0, index=series.index)
    return (series - smin) / (smax - smin)


# ==============================================================
# Top N categories for one-hot encoding
# ==============================================================
TOP_CATEGORIES = [
    "bed_bath_table", "health_beauty", "sports_leisure",
    "furniture_decor", "computers_accessories", "housewares",
    "watches_gifts", "telephony", "garden_tools", "auto",
]


# ==============================================================
# 1. User Features
# ==============================================================
def build_user_features(txn: pd.DataFrame, logger) -> pd.DataFrame:
    """Aggregate transaction data into one row per user."""
    logger.info("  [USER] Aggregating user features...")

    ref_date = txn["order_purchase_timestamp"].max()

    uf = txn.groupby("customer_unique_id").agg(
        purchase_count=("order_id", "nunique"),
        total_spending=("price", "sum"),
        avg_order_value=("price", "mean"),
        avg_rating_given=("review_score", lambda x: x[x > 0].mean()),
        review_count=("review_score", lambda x: (x > 0).sum()),
        distinct_products=("product_id", "nunique"),
        distinct_categories=("category_english", "nunique"),
        preferred_category=("category_english", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "unknown"),
        avg_freight=("freight_value", "mean"),
        customer_state=("customer_state", "first"),
        last_purchase=("order_purchase_timestamp", "max"),
    ).reset_index()

    # Recency: days since last purchase
    uf["recency_days"] = (ref_date - uf["last_purchase"]).dt.days
    uf.drop(columns=["last_purchase"], inplace=True)

    # Log transforms (handle zeros)
    uf["purchase_freq_log"] = np.log1p(uf["purchase_count"])
    uf["spending_log"] = np.log1p(uf["total_spending"])

    # Min-Max normalization
    uf["spending_normalized"] = min_max_normalize(uf["total_spending"])
    uf["aov_normalized"] = min_max_normalize(uf["avg_order_value"])

    # Round floats
    float_cols = uf.select_dtypes(include="float").columns
    uf[float_cols] = uf[float_cols].round(4)

    logger.info(f"    -> {len(uf):,} users, {len(uf.columns)} features")
    return uf


# ==============================================================
# 2. Item Features
# ==============================================================
def build_item_features(txn: pd.DataFrame, products: pd.DataFrame, logger) -> pd.DataFrame:
    """Aggregate transaction + product data into one row per item."""
    logger.info("  [ITEM] Aggregating item features...")

    itf = txn.groupby("product_id").agg(
        category_english=("category_english", "first"),
        total_sold=("order_id", "count"),
        avg_rating_received=("review_score", lambda x: x[x > 0].mean()),
        review_count=("review_score", lambda x: (x > 0).sum()),
        avg_price=("price", "mean"),
        total_revenue=("price", "sum"),
        distinct_buyers=("customer_unique_id", "nunique"),
        avg_freight=("freight_value", "mean"),
    ).reset_index()

    # Merge product physical attributes
    prod_cols = ["product_id", "product_weight_g", "product_photos_qty",
                 "product_length_cm", "product_height_cm", "product_width_cm"]
    prod_subset = products[[c for c in prod_cols if c in products.columns]].drop_duplicates("product_id")
    itf = itf.merge(prod_subset, on="product_id", how="left")

    # Derived: product volume
    if all(c in itf.columns for c in ["product_length_cm", "product_height_cm", "product_width_cm"]):
        itf["product_volume_cm3"] = (
            itf["product_length_cm"] * itf["product_height_cm"] * itf["product_width_cm"]
        )
        itf.drop(columns=["product_length_cm", "product_height_cm", "product_width_cm"], inplace=True)

    # Popularity rank
    itf["popularity_rank"] = itf["total_sold"].rank(method="min", ascending=False).astype(int)

    # Price percentile
    itf["price_percentile"] = itf["avg_price"].rank(pct=True)

    # Min-Max normalization
    itf["price_normalized"] = min_max_normalize(itf["avg_price"])
    itf["sold_normalized"] = min_max_normalize(itf["total_sold"])
    itf["rating_normalized"] = min_max_normalize(itf["avg_rating_received"].fillna(0))

    # One-hot encoding for top categories
    for cat in TOP_CATEGORIES:
        col_name = f"is_cat_{cat.replace(' ', '_')}"
        itf[col_name] = (itf["category_english"] == cat).astype(int)
    itf["is_cat_other"] = (~itf["category_english"].isin(TOP_CATEGORIES)).astype(int)

    # Round floats
    float_cols = itf.select_dtypes(include="float").columns
    itf[float_cols] = itf[float_cols].round(4)

    logger.info(f"    -> {len(itf):,} items, {len(itf.columns)} features")
    return itf


# ==============================================================
# 3. Interaction Features
# ==============================================================
def build_interaction_features(interactions: pd.DataFrame, logger) -> pd.DataFrame:
    """Enhance the user-item interaction matrix with derived features."""
    logger.info("  [INTERACTION] Building interaction features...")

    inf = interactions.copy()
    inf.rename(columns={"implicit": "implicit_signal"}, inplace=True)

    # Normalize rating to [0,1]
    inf["rating_normalized"] = min_max_normalize(inf["rating"].fillna(0))

    # Composite affinity score: weighted combination
    inf["user_item_affinity"] = (
        0.6 * inf["rating_normalized"] +
        0.4 * min_max_normalize(inf["purchase_count"])
    ).round(4)

    float_cols = inf.select_dtypes(include="float").columns
    inf[float_cols] = inf[float_cols].round(4)

    logger.info(f"    -> {len(inf):,} interactions, {len(inf.columns)} features")
    return inf


# ==============================================================
# 4. Feature Registry
# ==============================================================
def build_feature_registry() -> list:
    """Return a list of dicts describing every engineered feature."""
    registry = [
        # --- User features ---
        ("purchase_count", "user_features", "numeric", "count", "Total number of orders placed", "order_id"),
        ("total_spending", "user_features", "numeric", "sum", "Sum of all item prices (BRL)", "price"),
        ("avg_order_value", "user_features", "numeric", "mean", "Mean price per purchased item", "price"),
        ("avg_rating_given", "user_features", "numeric", "mean", "Mean review score given by user (1-5)", "review_score"),
        ("review_count", "user_features", "numeric", "count", "Number of reviews submitted", "review_score"),
        ("distinct_products", "user_features", "numeric", "nunique", "Unique products purchased", "product_id"),
        ("distinct_categories", "user_features", "numeric", "nunique", "Unique categories purchased", "category_english"),
        ("preferred_category", "user_features", "categorical", "mode", "Most frequently purchased category", "category_english"),
        ("avg_freight", "user_features", "numeric", "mean", "Mean shipping cost per item", "freight_value"),
        ("customer_state", "user_features", "categorical", "first", "Customer state code", "customer_state"),
        ("recency_days", "user_features", "numeric", "derived", "Days since last purchase relative to dataset end", "order_purchase_timestamp"),
        ("purchase_freq_log", "user_features", "numeric", "log1p", "Log-transformed purchase count", "purchase_count"),
        ("spending_log", "user_features", "numeric", "log1p", "Log-transformed total spending", "total_spending"),
        ("spending_normalized", "user_features", "numeric", "min-max [0,1]", "Normalized total spending", "total_spending"),
        ("aov_normalized", "user_features", "numeric", "min-max [0,1]", "Normalized avg order value", "avg_order_value"),
        # --- Item features ---
        ("total_sold", "item_features", "numeric", "count", "Total units sold", "order_id"),
        ("avg_rating_received", "item_features", "numeric", "mean", "Mean review score received (1-5)", "review_score"),
        ("review_count", "item_features", "numeric", "count", "Number of reviews received", "review_score"),
        ("avg_price", "item_features", "numeric", "mean", "Mean selling price (BRL)", "price"),
        ("total_revenue", "item_features", "numeric", "sum", "Total revenue generated", "price"),
        ("distinct_buyers", "item_features", "numeric", "nunique", "Number of unique buyers", "customer_unique_id"),
        ("product_volume_cm3", "item_features", "numeric", "derived (L×H×W)", "Product volume in cm³", "product_length/height/width_cm"),
        ("popularity_rank", "item_features", "numeric", "rank", "Rank by total_sold (1 = most popular)", "total_sold"),
        ("price_percentile", "item_features", "numeric", "rank(pct)", "Price percentile [0,1]", "avg_price"),
        ("price_normalized", "item_features", "numeric", "min-max [0,1]", "Normalized average price", "avg_price"),
        ("sold_normalized", "item_features", "numeric", "min-max [0,1]", "Normalized sales volume", "total_sold"),
        ("rating_normalized", "item_features", "numeric", "min-max [0,1]", "Normalized average rating", "avg_rating_received"),
        ("is_cat_*", "item_features", "binary", "one-hot", "One-hot encoding for top 10 categories + other", "category_english"),
        # --- Interaction features ---
        ("rating", "interaction_features", "numeric", "mean", "Explicit rating (review score)", "review_score"),
        ("purchase_count", "interaction_features", "numeric", "count", "Times user bought this item", "order_id"),
        ("implicit_signal", "interaction_features", "binary", "flag", "1 if purchased, 0 otherwise", "order_id"),
        ("rating_normalized", "interaction_features", "numeric", "min-max [0,1]", "Normalized rating", "rating"),
        ("user_item_affinity", "interaction_features", "numeric", "weighted (0.6×rating + 0.4×freq)", "Composite affinity score", "rating, purchase_count"),
    ]
    return [
        {"feature_name": r[0], "feature_table": r[1], "feature_type": r[2],
         "transformation": r[3], "description": r[4], "source_columns": r[5]}
        for r in registry
    ]


# ==============================================================
# 5. Load into SQLite
# ==============================================================
def load_to_sqlite(uf: pd.DataFrame, itf: pd.DataFrame, inf: pd.DataFrame,
                   registry: list, db_path: str, schema_path: str, logger):
    """Create SQLite database and load all feature tables."""
    logger.info(f"  [SQLITE] Loading features into: {db_path}")

    # Remove existing DB to start fresh
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)

    # Load DataFrames (creates tables automatically)
    uf.to_sql("user_features", conn, if_exists="replace", index=False)
    logger.info(f"    user_features: {len(uf):,} rows")

    itf.to_sql("item_features", conn, if_exists="replace", index=False)
    logger.info(f"    item_features: {len(itf):,} rows")

    inf.to_sql("interaction_features", conn, if_exists="replace", index=False)
    logger.info(f"    interaction_features: {len(inf):,} rows")

    # Load registry
    reg_df = pd.DataFrame(registry)
    reg_df.to_sql("feature_registry", conn, if_exists="replace", index=False)
    logger.info(f"    feature_registry: {len(reg_df)} entries")

    # Add performance indexes
    cursor = conn.cursor()
    index_stmts = [
        "CREATE INDEX IF NOT EXISTS idx_user_state ON user_features(customer_state)",
        "CREATE INDEX IF NOT EXISTS idx_item_category ON item_features(category_english)",
        "CREATE INDEX IF NOT EXISTS idx_item_popularity ON item_features(popularity_rank)",
        "CREATE INDEX IF NOT EXISTS idx_interaction_user ON interaction_features(customer_unique_id)",
        "CREATE INDEX IF NOT EXISTS idx_interaction_item ON interaction_features(product_id)",
    ]
    for stmt in index_stmts:
        cursor.execute(stmt)
    logger.info("    Indexes created")

    conn.commit()
    conn.close()
    logger.info("    SQLite database saved successfully")


# ==============================================================
# 6. Generate feature logic summary (Markdown)
# ==============================================================
def generate_feature_summary(registry: list, uf: pd.DataFrame, itf: pd.DataFrame,
                              inf: pd.DataFrame, output_path: str, logger):
    """Write a Markdown summary of all feature logic."""
    lines = [
        "# Feature Logic Summary",
        "",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "---",
        "",
        "## Overview",
        "",
        f"| Feature Table | Features | Rows |",
        f"|---------------|:--------:|-----:|",
        f"| user_features | {len(uf.columns)} | {len(uf):,} |",
        f"| item_features | {len(itf.columns)} | {len(itf):,} |",
        f"| interaction_features | {len(inf.columns)} | {len(inf):,} |",
        "",
        "---",
        "",
    ]

    # Group by table
    for table_name in ["user_features", "item_features", "interaction_features"]:
        table_features = [r for r in registry if r["feature_table"] == table_name]
        lines += [
            f"## {table_name}",
            "",
            "| Feature | Type | Transformation | Description | Source |",
            "|---------|------|---------------|-------------|--------|",
        ]
        for f in table_features:
            lines.append(
                f"| `{f['feature_name']}` | {f['feature_type']} | {f['transformation']} "
                f"| {f['description']} | `{f['source_columns']}` |"
            )
        lines += ["", "---", ""]

    # Transformation notes
    lines += [
        "## Transformation Details",
        "",
        "| Transformation | Formula | Applied To |",
        "|---------------|---------|-----------|",
        "| **Min-Max Normalization** | `(x - min) / (max - min)` -> [0, 1] | spending, AOV, price, sold, rating |",
        "| **Log Transform** | `log(1 + x)` | purchase_count, total_spending |",
        "| **One-Hot Encoding** | Binary 0/1 for each category | Top 10 categories + 'other' |",
        "| **Rank** | Ascending/descending integer rank | popularity_rank, price_percentile |",
        "| **Recency** | `max_date - last_purchase_date` in days | recency_days |",
        "| **Composite Score** | `0.6 × rating_norm + 0.4 × freq_norm` | user_item_affinity |",
        "",
    ]

    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    logger.info(f"    Feature summary: {output_path}")


# ==============================================================
# Main
# ==============================================================
def run_feature_engineering(config_path: str = None):
    cfg = load_config(config_path)
    logger = setup_logger("transformation", cfg, log_filename="transformation.log")
    project_root = get_project_root()

    # Input paths (from Task 5 curated output)
    curated_base = os.path.join(project_root, "data_lake", "curated", "prepared")
    # Find the latest date partition
    partitions = sorted([d for d in os.listdir(curated_base)
                         if os.path.isdir(os.path.join(curated_base, d))])
    latest = partitions[-1] if partitions else datetime.now().strftime("%Y-%m-%d")
    input_dir = os.path.join(curated_base, latest)

    # Output paths
    features_dir = ensure_directory(os.path.join(project_root, "data_lake", "curated", "features",
                                                  datetime.now().strftime("%Y-%m-%d")))
    reports_dir = ensure_directory(os.path.join(project_root, "reports"))
    schema_path = os.path.join(project_root, "transformation", "schema.sql")

    logger.info("=" * 70)
    logger.info("FEATURE ENGINEERING - START")
    logger.info(f"  Timestamp : {datetime.now().isoformat()}")
    logger.info(f"  Input     : {input_dir}")
    logger.info(f"  Output    : {features_dir}")
    logger.info("=" * 70)

    # Load prepared data from Task 5
    logger.info("[1] Loading prepared datasets...")
    txn = pd.read_csv(os.path.join(input_dir, "transactions.csv"), low_memory=False,
                       parse_dates=["order_purchase_timestamp"])
    logger.info(f"    Transactions: {len(txn):,} rows")

    interactions = pd.read_csv(os.path.join(input_dir, "user_item_interactions.csv"))
    logger.info(f"    Interactions: {len(interactions):,} rows")

    products = pd.read_csv(os.path.join(input_dir, "products_cleaned.csv"))
    logger.info(f"    Products: {len(products):,} rows")

    # Build features
    logger.info("[2] Building user features...")
    uf = build_user_features(txn, logger)

    logger.info("[3] Building item features...")
    itf = build_item_features(txn, products, logger)

    logger.info("[4] Building interaction features...")
    inf = build_interaction_features(interactions, logger)

    # Build registry
    logger.info("[5] Building feature registry...")
    registry = build_feature_registry()
    logger.info(f"    {len(registry)} features registered")

    # Save CSVs
    logger.info("[6] Saving feature CSVs...")
    uf.to_csv(os.path.join(features_dir, "user_features.csv"), index=False)
    itf.to_csv(os.path.join(features_dir, "item_features.csv"), index=False)
    inf.to_csv(os.path.join(features_dir, "interaction_features.csv"), index=False)

    reg_path = os.path.join(features_dir, "feature_registry.json")
    with open(reg_path, "w") as f:
        json.dump(registry, f, indent=2)
    logger.info(f"    Feature registry: {reg_path}")

    # Load to SQLite
    logger.info("[7] Loading into SQLite database...")
    db_path = os.path.join(features_dir, "features.db")
    load_to_sqlite(uf, itf, inf, registry, db_path, schema_path, logger)

    # Generate feature logic summary
    logger.info("[8] Generating feature logic summary...")
    summary_path = os.path.join(reports_dir, "feature_logic_summary.md")
    generate_feature_summary(registry, uf, itf, inf, summary_path, logger)

    # Save run metadata
    meta = {
        "run_timestamp": datetime.now().isoformat(),
        "input_dir": input_dir,
        "output_dir": features_dir,
        "user_features": {"rows": len(uf), "columns": len(uf.columns)},
        "item_features": {"rows": len(itf), "columns": len(itf.columns)},
        "interaction_features": {"rows": len(inf), "columns": len(inf.columns)},
        "total_registered_features": len(registry),
        "sqlite_db": db_path,
    }
    with open(os.path.join(features_dir, "feature_summary.json"), "w") as f:
        json.dump(meta, f, indent=2)

    logger.info("=" * 70)
    logger.info("FEATURE ENGINEERING - COMPLETE")
    logger.info(f"  User features        : {len(uf):,} rows × {len(uf.columns)} cols")
    logger.info(f"  Item features        : {len(itf):,} rows × {len(itf.columns)} cols")
    logger.info(f"  Interaction features : {len(inf):,} rows × {len(inf.columns)} cols")
    logger.info(f"  SQLite DB            : {db_path}")
    logger.info("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="RecoMart Feature Engineering")
    parser.add_argument("--config", type=str, default=None)
    args = parser.parse_args()
    run_feature_engineering(config_path=args.config)


if __name__ == "__main__":
    main()
