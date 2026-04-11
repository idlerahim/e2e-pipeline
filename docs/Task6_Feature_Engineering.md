# Task 6: Data Transformation and Feature Engineering

## How to Run

```bash
python -m transformation.feature_engineering
```

## Input

| Input                          | What                     | Location                                                             |
| ------------------------------ | ------------------------ | -------------------------------------------------------------------- |
| Transactions (cleaned, merged) | 947 rows from Task 5 | `data_lake/curated/prepared/2026-04-11/transactions.csv`           |
| User-item interactions         | 89 rows from Task 5  | `data_lake/curated/prepared/2026-04-11/user_item_interactions.csv` |
| Cleaned products               | 791 rows from Task 5  | `data_lake/curated/prepared/2026-04-11/products_cleaned.csv`       |

## What It Does

### User Features (86 rows × 16 cols)

Aggregates per user (purchase_count, total_spending, avg_order_value, avg_rating_given, review_count, distinct_products, distinct_categories, preferred_category, avg_freight, customer_state, recency_days) + log transforms (purchase_freq_log, spending_log) + min-max normalization (spending_normalized, aov_normalized).

### Item Features (791 rows × 28 cols)

Aggregates per product (total_sold, avg_rating_received, review_count, avg_price, total_revenue, distinct_buyers, product_volume_cm3, popularity_rank, price_percentile) + min-max normalization (price_normalized, sold_normalized, rating_normalized) + **one-hot encoding** (is_cat_* for top 10 categories + other).

### Interaction Features (89 rows × 7 cols)

Enhances user-item pairs: explicit rating (review_score), purchase_count, implicit_signal (1 if purchased), rating_normalized + **composite affinity score** (user_item_affinity = 0.6 × rating_normalized + 0.4 × frequency_normalized).

### Transformations Applied

| Transformation                  | What                                 | Applied To                         |
| ------------------------------- | ------------------------------------ | ---------------------------------- |
| **Min-Max Normalization** | Scale to [0, 1]                      | spending, AOV, price, sold, rating |
| **Log Transform**         | `log(1 + x)`                       | purchase_count, total_spending     |
| **One-Hot Encoding**      | Binary 0/1 per category              | Top 10 categories + 'other'        |
| **Rank**                  | Integer rank                         | popularity_rank, price_percentile  |
| **Recency**               | `max_date - last_purchase` in days | recency_days                       |
| **Composite Score**       | Weighted combination                 | user_item_affinity                 |

## Output

| Output                       | Location                                                           |
| ---------------------------- | ------------------------------------------------------------------ |
| User features (CSV)          | `data_lake/curated/features/2026-04-11/user_features.csv` (86 rows)       |
| Item features (CSV)          | `data_lake/curated/features/2026-04-11/item_features.csv` (791 rows)       |
| Interaction features (CSV)   | `data_lake/curated/features/2026-04-11/interaction_features.csv` (89 rows) |
| SQLite database (all tables) | `data_lake/curated/features/2026-04-11/features.db`              |
| Feature registry (JSON)      | `data_lake/curated/features/2026-04-11/feature_registry.json`    |
| Feature logic summary        | `reports/feature_logic_summary.md`                               |
| Transformation log           | `logs/transformation.log`                                        |

## Deliverables Checklist

| Deliverable                          | File                                      |
| ------------------------------------ | ----------------------------------------- |
| ✅**SQL Schema**               | `transformation/schema.sql`             |
| ✅**Transformation Script**    | `transformation/feature_engineering.py` |
| ✅**Summary of Feature Logic** | `reports/feature_logic_summary.md`      |

## Files Involved

```
transformation/feature_engineering.py  ← Main script (entry point)
transformation/schema.sql              ← SQL CREATE TABLE definitions
reports/feature_logic_summary.md       ← Feature documentation (auto-generated)
config/pipeline_config.yaml            ← Path configuration
ingestion/utils.py                     ← Shared utilities
```
