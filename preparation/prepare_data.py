"""
============================================================
RecoMart Data Pipeline — Data Preparation & EDA (Task 5)
============================================================
Performs data cleaning, preprocessing, exploratory analysis,
and outputs a prepared dataset ready for feature engineering.

Steps:
  1. Load all raw datasets
  2. Clean & preprocess each dataset
  3. Merge into a unified transaction dataset
  4. Build user-item interaction matrix
  5. Generate EDA plots (histograms, heatmaps, distributions)
  6. Save prepared datasets to data_lake/curated/

Usage:
    python -m preparation.prepare_data
"""

import os
import sys
import glob
import json
import argparse
import warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for headless environments
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.utils import (
    load_config,
    setup_logger,
    get_project_root,
    ensure_directory,
)


def derive_source_name(filename: str) -> str:
    name = os.path.splitext(filename)[0]
    if name.startswith("olist_"):
        name = name[len("olist_") :]
    if name.endswith("_dataset"):
        name = name[: -len("_dataset")]
    return name


def find_latest_ingested_file(raw_layer: str, filename: str) -> str | None:
    source_name = derive_source_name(filename)
    pattern = os.path.join(raw_layer, source_name, "csv", "*", filename)
    candidates = glob.glob(pattern)
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)

# Plot style
plt.style.use("seaborn-v0_8-whitegrid")
COLORS = ["#2196F3", "#FF9800", "#4CAF50", "#F44336", "#9C27B0",
          "#00BCD4", "#795548", "#607D8B", "#E91E63", "#3F51B5"]


# ==============================================================
# 1. Data Loading
# ==============================================================
def load_datasets(source_dir: str, raw_layer: str, logger) -> dict:
    """Load all raw CSV datasets into a dictionary of DataFrames."""
    files = {
        "customers": "olist_customers_dataset.csv",
        "products": "olist_products_dataset.csv",
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "reviews": "olist_order_reviews_dataset.csv",
        "payments": "olist_order_payments_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv",
        "categories": "product_category_name_translation.csv",
    }
    dfs = {}
    for key, fname in files.items():
        raw_file = find_latest_ingested_file(raw_layer, fname)
        if raw_file:
            path = raw_file
            logger.info(f"  Loading {key} from ingested raw layer: {raw_file}")
        else:
            path = os.path.join(source_dir, fname)
            logger.info(f"  Loading {key} from source dataset fallback: {path}")

        if not os.path.isfile(path):
            raise FileNotFoundError(f"Required dataset file not found: {path}")

        dfs[key] = pd.read_csv(path, low_memory=False)
        logger.info(f"    -> {len(dfs[key]):,} rows x {len(dfs[key].columns)} cols")
    return dfs


# ==============================================================
# 2. Data Cleaning
# ==============================================================
def clean_customers(df: pd.DataFrame, logger) -> pd.DataFrame:
    """De-duplicate customers by customer_unique_id."""
    logger.info("  Cleaning customers...")
    df = df.drop_duplicates(subset=["customer_unique_id"], keep="first").copy()
    df["customer_city"] = df["customer_city"].str.strip().str.lower()
    df["customer_state"] = df["customer_state"].str.strip().str.upper()
    logger.info(f"    -> {len(df):,} unique customers")
    return df


def clean_products(df: pd.DataFrame, cat_df: pd.DataFrame, logger) -> pd.DataFrame:
    """Translate categories, impute missing dimensions."""
    logger.info("  Cleaning products...")
    # Translate category names
    df = df.merge(cat_df, on="product_category_name", how="left")
    df["category_english"] = df["product_category_name_english"].fillna("unknown")
    df.drop(columns=["product_category_name_english"], inplace=True)

    # Fill missing product_category_name
    df["product_category_name"] = df["product_category_name"].fillna("unknown")

    # Impute missing numeric dimensions with median
    num_cols = ["product_name_lenght", "product_description_lenght", "product_photos_qty",
                "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    for col in num_cols:
        if df[col].isnull().sum() > 0:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            logger.info(f"    Imputed {col}: {df[col].isnull().sum()} remaining nulls (median={median_val:.1f})")

    logger.info(f"    -> {len(df):,} products, {df['category_english'].nunique()} categories")
    return df


def clean_orders(df: pd.DataFrame, logger) -> pd.DataFrame:
    """Parse timestamps, filter to delivered orders."""
    logger.info("  Cleaning orders...")
    date_cols = ["order_purchase_timestamp", "order_approved_at",
                 "order_delivered_carrier_date", "order_delivered_customer_date",
                 "order_estimated_delivery_date"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Keep only delivered orders for model training
    total = len(df)
    df_delivered = df[df["order_status"] == "delivered"].copy()
    logger.info(f"    Filtered: {total:,} -> {len(df_delivered):,} delivered orders "
                f"({len(df_delivered)/total*100:.1f}%)")
    return df_delivered


def clean_reviews(df: pd.DataFrame, logger) -> pd.DataFrame:
    """Ensure review_score is int, handle missing text."""
    logger.info("  Cleaning reviews...")
    df["review_score"] = df["review_score"].astype(int)
    df["review_comment_title"] = df["review_comment_title"].fillna("")
    df["review_comment_message"] = df["review_comment_message"].fillna("")
    df["has_review_text"] = (df["review_comment_message"].str.len() > 0).astype(int)
    logger.info(f"    -> {len(df):,} reviews, {df['has_review_text'].sum():,} with text")
    return df


def clean_payments(df: pd.DataFrame, logger) -> pd.DataFrame:
    """Aggregate payments per order."""
    logger.info("  Cleaning payments...")
    agg = df.groupby("order_id").agg(
        total_payment=("payment_value", "sum"),
        num_payments=("payment_sequential", "max"),
        primary_payment_type=("payment_type", "first"),
    ).reset_index()
    logger.info(f"    -> {len(agg):,} orders with payment info")
    return agg


def clean_geolocation(df: pd.DataFrame, logger) -> pd.DataFrame:
    """De-duplicate and filter outliers."""
    logger.info("  Cleaning geolocation...")
    df = df.drop_duplicates(subset=["geolocation_zip_code_prefix"], keep="first").copy()
    # Filter to valid Brazil bounds
    df = df[(df["geolocation_lat"] >= -60) & (df["geolocation_lat"] <= 10) &
            (df["geolocation_lng"] >= -80) & (df["geolocation_lng"] <= -30)]
    logger.info(f"    -> {len(df):,} unique zip codes")
    return df


# ==============================================================
# 3. Merge into unified transaction dataset
# ==============================================================
def build_transactions(dfs: dict, logger) -> pd.DataFrame:
    """Merge orders + items + reviews + payments + customers + products."""
    logger.info("  Building unified transaction dataset...")

    # Start with orders + items
    txn = dfs["orders"].merge(dfs["order_items"], on="order_id", how="inner")
    logger.info(f"    orders × items: {len(txn):,}")

    # Add reviews
    txn = txn.merge(dfs["reviews"][["order_id", "review_score", "has_review_text"]],
                     on="order_id", how="left")
    txn["review_score"] = txn["review_score"].fillna(0).astype(int)
    logger.info(f"    + reviews: {len(txn):,}")

    # Add payments
    txn = txn.merge(dfs["payments"], on="order_id", how="left")
    logger.info(f"    + payments: {len(txn):,}")

    # Add customer info
    txn = txn.merge(dfs["customers"][["customer_id", "customer_unique_id", "customer_state"]],
                     on="customer_id", how="left")
    logger.info(f"    + customers: {len(txn):,}")

    # Add product info
    txn = txn.merge(dfs["products"][["product_id", "category_english", "product_weight_g", "product_photos_qty"]],
                     on="product_id", how="left")
    logger.info(f"    + products: {len(txn):,}")

    # Derived columns
    txn["purchase_date"] = txn["order_purchase_timestamp"].dt.date
    txn["purchase_hour"] = txn["order_purchase_timestamp"].dt.hour
    txn["purchase_dayofweek"] = txn["order_purchase_timestamp"].dt.dayofweek

    logger.info(f"    Final transaction dataset: {len(txn):,} rows × {len(txn.columns)} cols")
    return txn


# ==============================================================
# 4. User-Item Interaction Matrix
# ==============================================================
def build_interaction_matrix(txn: pd.DataFrame, logger) -> pd.DataFrame:
    """Build a user-item interaction matrix with review scores."""
    logger.info("  Building user-item interaction matrix...")
    interactions = txn.groupby(["customer_unique_id", "product_id"]).agg(
        rating=("review_score", "mean"),
        purchase_count=("order_id", "nunique"),
    ).reset_index()
    interactions["rating"] = interactions["rating"].round(1)
    interactions["implicit"] = 1  # Binary implicit feedback

    n_users = interactions["customer_unique_id"].nunique()
    n_items = interactions["product_id"].nunique()
    n_interactions = len(interactions)
    sparsity = 1 - (n_interactions / (n_users * n_items))

    logger.info(f"    Users: {n_users:,}  Items: {n_items:,}  Interactions: {n_interactions:,}")
    logger.info(f"    Sparsity: {sparsity*100:.4f}%")

    return interactions


# ==============================================================
# 5. EDA Plots
# ==============================================================
def generate_eda_plots(txn: pd.DataFrame, interactions: pd.DataFrame,
                        dfs: dict, plots_dir: str, logger):
    """Generate and save EDA summary plots."""
    logger.info("  Generating EDA plots...")

    # --- Plot 1: Review Score Distribution ---
    fig, ax = plt.subplots(figsize=(8, 5))
    scores = txn[txn["review_score"] > 0]["review_score"]
    score_counts = scores.value_counts().sort_index()
    ax.bar(score_counts.index, score_counts.values, color=COLORS[:5], edgecolor="white")
    ax.set_xlabel("Review Score", fontsize=12)
    ax.set_ylabel("Count", fontsize=12)
    ax.set_title("Distribution of Review Scores", fontsize=14, fontweight="bold")
    ax.set_xticks([1, 2, 3, 4, 5])
    for i, v in enumerate(score_counts.values):
        ax.text(score_counts.index[i], v + 500, f"{v:,}", ha="center", fontsize=9)
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "01_review_score_distribution.png"), dpi=150)
    plt.close()
    logger.info("    [OK] 01_review_score_distribution.png")

    # --- Plot 2: Top 15 Product Categories ---
    fig, ax = plt.subplots(figsize=(10, 6))
    cat_counts = txn["category_english"].value_counts().head(15)
    ax.barh(cat_counts.index[::-1], cat_counts.values[::-1], color=COLORS[0], edgecolor="white")
    ax.set_xlabel("Number of Transactions", fontsize=12)
    ax.set_title("Top 15 Product Categories by Transaction Volume", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "02_top_categories.png"), dpi=150)
    plt.close()
    logger.info("    [OK] 02_top_categories.png")

    # --- Plot 3: Item Popularity (Long Tail) ---
    fig, ax = plt.subplots(figsize=(10, 5))
    item_pop = txn["product_id"].value_counts().reset_index()
    item_pop.columns = ["product_id", "count"]
    item_pop = item_pop.sort_values("count", ascending=False).reset_index(drop=True)
    ax.plot(range(len(item_pop)), item_pop["count"], color=COLORS[3], linewidth=1)
    ax.fill_between(range(len(item_pop)), item_pop["count"], alpha=0.3, color=COLORS[3])
    ax.set_xlabel("Product Rank", fontsize=12)
    ax.set_ylabel("Number of Purchases", fontsize=12)
    ax.set_title("Item Popularity - Long Tail Distribution", fontsize=14, fontweight="bold")
    ax.set_xlim(0, len(item_pop))
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "03_item_popularity_long_tail.png"), dpi=150)
    plt.close()
    logger.info("    [OK] 03_item_popularity_long_tail.png")

    # --- Plot 4: User Activity Distribution ---
    fig, ax = plt.subplots(figsize=(8, 5))
    user_activity = interactions.groupby("customer_unique_id")["purchase_count"].sum()
    act_counts = user_activity.value_counts().sort_index().head(10)
    ax.bar(act_counts.index.astype(str), act_counts.values, color=COLORS[4], edgecolor="white")
    ax.set_xlabel("Number of Purchases per User", fontsize=12)
    ax.set_ylabel("Number of Users", fontsize=12)
    ax.set_title("User Activity Distribution", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "04_user_activity_distribution.png"), dpi=150)
    plt.close()
    logger.info("    [OK] 04_user_activity_distribution.png")

    # --- Plot 5: Purchase Volume Over Time ---
    fig, ax = plt.subplots(figsize=(12, 5))
    monthly = txn.set_index("order_purchase_timestamp").resample("ME")["order_id"].count()
    ax.plot(monthly.index, monthly.values, color=COLORS[0], linewidth=2, marker="o", markersize=4)
    ax.fill_between(monthly.index, monthly.values, alpha=0.2, color=COLORS[0])
    ax.set_xlabel("Month", fontsize=12)
    ax.set_ylabel("Number of Orders", fontsize=12)
    ax.set_title("Monthly Purchase Volume Over Time", fontsize=14, fontweight="bold")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "05_monthly_purchase_volume.png"), dpi=150)
    plt.close()
    logger.info("    [OK] 05_monthly_purchase_volume.png")

    # --- Plot 6: Purchase Hour Heatmap ---
    fig, ax = plt.subplots(figsize=(10, 5))
    hour_dow = txn.groupby(["purchase_dayofweek", "purchase_hour"])["order_id"].count().unstack(fill_value=0)
    im = ax.imshow(hour_dow.values, aspect="auto", cmap="YlOrRd", interpolation="nearest")
    ax.set_yticks(range(7))
    ax.set_yticklabels(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])
    ax.set_xlabel("Hour of Day", fontsize=12)
    ax.set_title("Purchase Heatmap: Day of Week × Hour", fontsize=14, fontweight="bold")
    plt.colorbar(im, ax=ax, label="Order Count")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "06_purchase_heatmap.png"), dpi=150)
    plt.close()
    logger.info("    [OK] 06_purchase_heatmap.png")

    # --- Plot 7: Price Distribution ---
    fig, ax = plt.subplots(figsize=(8, 5))
    prices = txn["price"].clip(upper=500)  # clip for visualization
    ax.hist(prices, bins=50, color=COLORS[1], edgecolor="white", alpha=0.8)
    ax.set_xlabel("Price (BRL, clipped at 500)", fontsize=12)
    ax.set_ylabel("Frequency", fontsize=12)
    ax.set_title("Price Distribution (Clipped at 500 BRL)", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "07_price_distribution.png"), dpi=150)
    plt.close()
    logger.info("    [OK] 07_price_distribution.png")

    # --- Plot 8: Payment Type Distribution ---
    fig, ax = plt.subplots(figsize=(8, 5))
    pt_counts = txn["primary_payment_type"].value_counts()
    ax.pie(pt_counts.values, labels=pt_counts.index, autopct="%1.1f%%",
           colors=COLORS[:len(pt_counts)], startangle=90)
    ax.set_title("Payment Type Distribution", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "08_payment_type_distribution.png"), dpi=150)
    plt.close()
    logger.info("    [OK] 08_payment_type_distribution.png")

    logger.info(f"  All plots saved to: {plots_dir}")


# ==============================================================
# Main
# ==============================================================
def run_preparation(config_path: str = None):
    cfg = load_config(config_path)
    logger = setup_logger("preparation", cfg, log_filename="preparation.log")
    project_root = get_project_root()
    source_dir = os.path.join(project_root, cfg["paths"]["source_data_dir"])
    raw_layer = os.path.join(project_root, cfg["paths"]["raw_layer"])
    curated_dir = ensure_directory(os.path.join(project_root, "data_lake", "curated", "prepared",
                                                 datetime.now().strftime("%Y-%m-%d")))
    plots_dir = ensure_directory(os.path.join(project_root, "reports", "eda_plots"))

    logger.info("=" * 70)
    logger.info("DATA PREPARATION & EDA - START")
    logger.info(f"  Timestamp : {datetime.now().isoformat()}")
    logger.info(f"  Raw layer : {raw_layer}")
    logger.info("=" * 70)

    # 1. Load
    logger.info("[1] Loading datasets...")
    dfs = load_datasets(source_dir, raw_layer, logger)

    # 2. Clean
    logger.info("[2] Cleaning datasets...")
    dfs["customers"] = clean_customers(dfs["customers"], logger)
    dfs["products"] = clean_products(dfs["products"], dfs["categories"], logger)
    dfs["orders"] = clean_orders(dfs["orders"], logger)
    dfs["reviews"] = clean_reviews(dfs["reviews"], logger)
    dfs["payments"] = clean_payments(dfs["payments"], logger)
    dfs["geolocation"] = clean_geolocation(dfs["geolocation"], logger)

    # 3. Merge
    logger.info("[3] Building transactions...")
    txn = build_transactions(dfs, logger)

    # 4. Interaction matrix
    logger.info("[4] Building interaction matrix...")
    interactions = build_interaction_matrix(txn, logger)

    # 5. EDA plots
    logger.info("[5] Generating EDA plots...")
    generate_eda_plots(txn, interactions, dfs, plots_dir, logger)

    # 6. Save prepared datasets
    logger.info("[6] Saving prepared datasets...")
    txn_path = os.path.join(curated_dir, "transactions.csv")
    txn.to_csv(txn_path, index=False)
    logger.info(f"    Transactions: {txn_path} ({len(txn):,} rows)")

    int_path = os.path.join(curated_dir, "user_item_interactions.csv")
    interactions.to_csv(int_path, index=False)
    logger.info(f"    Interactions: {int_path} ({len(interactions):,} rows)")

    prod_path = os.path.join(curated_dir, "products_cleaned.csv")
    dfs["products"].to_csv(prod_path, index=False)
    logger.info(f"    Products: {prod_path} ({len(dfs['products']):,} rows)")

    cust_path = os.path.join(curated_dir, "customers_cleaned.csv")
    dfs["customers"].to_csv(cust_path, index=False)
    logger.info(f"    Customers: {cust_path} ({len(dfs['customers']):,} rows)")

    # Save summary stats
    stats = {
        "run_timestamp": datetime.now().isoformat(),
        "transaction_rows": len(txn),
        "unique_users": int(txn["customer_unique_id"].nunique()),
        "unique_products": int(txn["product_id"].nunique()),
        "unique_categories": int(txn["category_english"].nunique()),
        "interaction_count": len(interactions),
        "sparsity_pct": round((1 - len(interactions) / (txn["customer_unique_id"].nunique() * txn["product_id"].nunique())) * 100, 4),
        "avg_review_score": round(float(txn[txn["review_score"] > 0]["review_score"].mean()), 2),
        "date_range": f"{txn['order_purchase_timestamp'].min()} to {txn['order_purchase_timestamp'].max()}",
    }
    stats_path = os.path.join(curated_dir, "preparation_summary.json")
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2, default=str)
    logger.info(f"    Summary: {stats_path}")

    logger.info("=" * 70)
    logger.info("DATA PREPARATION & EDA - COMPLETE")
    logger.info(f"  Transactions : {stats['transaction_rows']:,}")
    logger.info(f"  Users        : {stats['unique_users']:,}")
    logger.info(f"  Products     : {stats['unique_products']:,}")
    logger.info(f"  Interactions : {stats['interaction_count']:,}")
    logger.info(f"  Sparsity     : {stats['sparsity_pct']}%")
    logger.info("=" * 70)

    return txn, interactions


def main():
    parser = argparse.ArgumentParser(description="RecoMart Data Preparation & EDA")
    parser.add_argument("--config", type=str, default=None)
    args = parser.parse_args()
    run_preparation(config_path=args.config)


if __name__ == "__main__":
    main()
