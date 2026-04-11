# Task 8: Data Versioning and Lineage

## Overview

This document describes the data versioning and lineage tracking workflow implemented for RocoMart using **DVC (Data Version Control)**. DVC lets us version large raw and transformed datasets alongside code, while keeping the Git repository lightweight by storing only pointer files and metadata.

## Repository Structure and Dataset Versions

The repository tracks raw and transformed data with DVC at the top level using these files:

- `dataset.dvc` — tracks the `dataset/` directory containing raw external datasets.
- `data_lake.dvc` — tracks the `data_lake/` directory containing raw ingestion output, prepared data, feature data, and serving artifacts.

The tracked directory layout is:

- `dataset/`
  - `olist_customers_dataset.csv`
  - `olist_geolocation_dataset.csv`
  - `olist_order_items_dataset.csv`
  - `olist_order_payments_dataset.csv`
  - `olist_order_reviews_dataset.csv`
  - `olist_orders_dataset.csv`
  - `olist_products_dataset.csv`
  - `olist_sellers_dataset.csv`
  - `product_category_name_translation.csv`
- `data_lake/`
  - `raw/`
    - `categories/`
    - `customers/`
    - `geolocation/`
    - `order_items/`
    - `order_payments/`
    - `order_reviews/`
    - `orders/`
    - `product_category_name_translation/`
    - `products/`
    - `sellers/`
  - `curated/`
    - `prepared/`
    - `features/`
  - `serving/`
    - `feature_store/`
    - `training_sets/`

The raw data directories in `data_lake/raw/` also include metadata and ingestion logs that capture lineage details such as source and ingestion timestamp.

## Versioning Workflow

### 1. Track Data Additions and Updates

Whenever raw datasets or transformed outputs change, version them with DVC:

```powershell
dvc add dataset data_lake
```

This command updates the lightweight DVC tracking files `dataset.dvc` and `data_lake.dvc`. Each file stores checksums, file counts, and paths for the current state of its tracked directory.

### 2. Commit Data Version Metadata to Git

Keep the repository and data versions aligned by committing the DVC pointer files:

```powershell
git add dataset.dvc data_lake.dvc
git commit -m "Version dataset and data lake state"
```

The actual data payload is stored in DVC cache at `.dvc/cache` and is excluded from Git. The Git repository therefore remains small while preserving exact dataset versions.

### 3. View Version History

Inspect the data version history directly with Git:

```powershell
git log --oneline -- dataset.dvc
git log --oneline -- data_lake.dvc
```

This provides a complete lineage of dataset states and allows you to correlate a specific code commit with the exact data version used.

### 4. Restore a Historical Data Version

To reproduce a past state of the pipeline:

```powershell
git checkout <commit_hash>
dvc checkout
```

`dvc checkout` restores the version of `dataset/` and `data_lake/` that correspond to the checked-out Git commit.

## Data Lineage and Metadata Tracking

- **Source and ingestion metadata:** During ingestion, raw files are stored under `data_lake/raw/` and paired with metadata artifacts that document source, ingestion date, and schema.
- **Transformation history:** Transformation steps and feature creation logic are recorded in the project’s feature store logs, including the feature registry and generated training set metadata.
- **Data version lineage:** `dataset.dvc` and `data_lake.dvc` provide a direct lineage mapping between Git commits and dataset states.

## Notes on DVC Usage

- The repository already contains the DVC tracking files `dataset.dvc` and `data_lake.dvc`.
- `dataset/` and `data_lake/` are excluded from Git via `.gitignore`, so only the pointer files and metadata are tracked.
- To check if the workspace is synchronized with the DVC cache, use:

```powershell
dvc status
```

- If a remote storage backend is configured later, use:

```powershell
dvc push
dvc pull
```

This workflow ensures dataset versions and lineage metadata are retained in the repository without storing large raw files in Git.
