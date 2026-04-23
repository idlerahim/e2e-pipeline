# Task 7: Feature Store Implementation

## How to Run

```bash
# 1. Register a feature snapshot (from Task 6 output)
python -m feature_store.feature_store_manager --register

# 2. Check store status
python -m feature_store.feature_store_manager --status

# 3. Generate a training set
python -m feature_store.feature_store_manager --training-set
# With sampling:
python -m feature_store.feature_store_manager --training-set --sample 10000

# 4. Point-in-time retrieval (timestamp must be >= earliest snapshot)
python -m feature_store.feature_store_manager --training-set --pit "2026-04-12"

# 5.1 OPTIONAL: Dynamically get users using sqlite3
# Install sqlite3 on Windows
winget install SQLite.SQLite

# Get users dynamically from latest feature_store and place them next command
sqlite3 "$((Get-ChildItem data_lake/serving/feature_store -Directory -Filter 'v_*' | Sort-Object Name | Select-Object -Last 1).FullName)\features.db" "SELECT DISTINCT customer_unique_id FROM user_features LIMIT 20;"

# 5.2 Inference: query user features (use real customer_unique_id values)
python -m feature_store.feature_store_manager --query-users "0006fdc98a402fceb4eb0ee528f6a8d4, 00c04df1c94e385d57d4a33a2965217c"

# 6.1 Get items dynamically from latest feature_store and place them in next command
sqlite3 "$((Get-ChildItem data_lake/serving/feature_store -Directory -Filter 'v_*' | Sort-Object Name | Select-Object -Last 1).FullName)\features.db" "SELECT DISTINCT product_id FROM item_features LIMIT 20;"

# 6.2 Inference: query item features (use real product_id values)
python -m feature_store.feature_store_manager --query-items "0030e635639c898b323826589761cf23, 00ab8a8b9fe219511dc3f178c6d79698"
```

## Input

| Input                        | What                                        | Location                                                        |
| ---------------------------- | ------------------------------------------- | --------------------------------------------------------------- |
| Features DB from Task 6      | SQLite DB with user/item/interaction tables | `data_lake/curated/features/YYYY-MM-DD/features.db`           |
| Feature registry from Task 6 | JSON metadata about each feature            | `data_lake/curated/features/YYYY-MM-DD/feature_registry.json` |

## What It Does

### 1. Snapshot Versioning

Each time `--register` is run, a **timestamped snapshot** is created:

- Copies `features.db` into a versioned folder
- Records snapshot metadata in `store_registry.json`
- Supports multiple versions (e.g., daily refreshes)

### 2. Point-in-Time Correctness

The `--pit` flag enables **point-in-time lookups**:

- Finds the latest snapshot created **at or before** the requested timestamp
- Prevents data leakage by ensuring training uses only historically-available features

### 3. Training Set Retrieval

The `--training-set` command:

- Joins `interaction_features` and `user_features` via SQL
- Filters for users with `distinct_categories >= 2`
- Produces a flat DataFrame with 7 essential recommendation features per row
- Optional `--sample N` to randomly sample N rows
- Saves to `data_lake/serving/training_sets/`

### 4. Inference Retrieval (Online Serving)

- `--query-users`: Look up features for specific user IDs
- `--query-items`: Look up features for specific item IDs
- Returns feature vectors suitable for real-time recommendation scoring

## Architecture

```
data_lake/serving/feature_store/
├── store_registry.json              ← Store metadata, snapshot history
├── v_20260407_105054/               ← Snapshot version 1
│   ├── features.db                  ← Versioned copy of features DB
│   └── feature_registry.json       ← Versioned copy of feature metadata
├── v_20260408_090000/               ← Snapshot version 2 (next day)
│   ├── features.db
│   └── feature_registry.json
└── ...

data_lake/serving/training_sets/
├── training_set_20260407_105117.csv  ← Generated training set
└── ...
```

## Output

| Output                  | Location                                                |
| ----------------------- | ------------------------------------------------------- |
| Feature store snapshots | `data_lake/serving/feature_store/v_*/`                |
| Store registry (JSON)   | `data_lake/serving/feature_store/store_registry.json` |
| Training sets (CSV)     | `data_lake/serving/training_sets/`                    |
| Feature store log       | `logs/feature_store.log`                              |

## Deliverables Checklist

| Deliverable                                       | File                                                                    |
| ------------------------------------------------- | ----------------------------------------------------------------------- |
| ✅**Feature store setup code**              | `feature_store/feature_store_manager.py`                              |
| ✅**Sample queries for training/inference** | CLI commands (`--training-set`, `--query-users`, `--query-items`) |
| ✅**Documentation of stored features**      | This file +`reports/feature_logic_summary.md` (from Task 6)           |

## Key Design Decisions

| Decision                                       | Rationale                                                 |
| ---------------------------------------------- | --------------------------------------------------------- |
| **Custom solution** (vs Feast/Hopsworks) | Simpler for assignment scope; no infra dependencies       |
| **SQLite backend**                       | Zero-config, portable, suitable for batch workloads       |
| **Snapshot versioning**                  | Each registration creates a new immutable version         |
| **Point-in-time via timestamps**         | Prevents data leakage in time-series recommendation tasks |
| **SQL-based training set**               | Single JOIN produces flat feature matrix for model input  |

## Files Involved

```
feature_store/feature_store_manager.py  ← Feature store implementation (entry point)
transformation/schema.sql               ← SQL schema (from Task 6)
transformation/feature_engineering.py   ← Feature builder (from Task 6)
config/pipeline_config.yaml             ← Path configuration
ingestion/utils.py                      ← Shared utilities
```
