# Task 5: Data Preparation

## How to Run

```bash
# Install dependencies (one-time)
pip install pandas matplotlib

# Install numpy 1.26.4 as latest wheel conflicts with Perfect server on Windows 11 64 bit
pip install numpy==1.26.4

# Run preparation + EDA
python -m preparation.prepare_data
```

## Input

| Input                | What              | Location            |
| -------------------- | ----------------- | ------------------- |
| Raw CSV datasets (9) | Source data files | `dataset/` folder |

## What It Does

### Step 1: Data Cleaning

| Dataset               | Cleaning Applied                                                                                        |
| --------------------- | ------------------------------------------------------------------------------------------------------- |
| **Customers**   | De-duplicated by `customer_unique_id`, standardized city/state (10,000 rows)                     |
| **Products**    | Translated categories PT→EN, imputed 188 missing category names, imputed 3 missing dimensions (median) |
| **Orders**      | Parsed timestamps, filtered to delivered only (10,000 → valid delivered orders, keeping ~97%)                  |
| **Reviews**     | Filled missing text with empty string (9,462 rows), added `has_review_text` flag                                   |
| **Payments**    | Aggregated per order (total_payment, num_payments, primary_payment_type) (10,000 rows)                                |
| **Geolocation** | De-duplicated by zip (10,000 raw → ~5,000 deduplicated), removed lat/lng outliers outside Brazil                            |

### Step 2: Merge into Unified Transaction Dataset

Joins: orders × items × reviews × payments × customers × products → **947 rows × 27 cols**

### Step 3: User-Item Interaction Matrix

- **86 users** × **791 items** → **89 interactions**
- **Sparsity: 99.87%** (typical for recommendation systems)
- Contains: `customer_unique_id`, `product_id`, `rating`, `purchase_count`, `implicit`

### Step 4: EDA Plots (8 visualizations)

1. Review score distribution
2. Top 15 product categories
3. Item popularity long-tail curve
4. User activity distribution
5. Monthly purchase volume over time
6. Purchase heatmap (day-of-week × hour)
7. Price distribution
8. Payment type distribution

## Output

| Output                                 | Location                                                             |
| -------------------------------------- | -------------------------------------------------------------------- |
| Transactions dataset (cleaned, merged) | `data_lake/curated/prepared/2026-04-11/transactions.csv` (947 rows) |
| User-item interaction matrix           | `data_lake/curated/prepared/2026-04-11/user_item_interactions.csv` (89 interactions) |
| Cleaned products                       | `data_lake/curated/prepared/2026-04-11/products_cleaned.csv` (791 products) |
| Cleaned customers                      | `data_lake/curated/prepared/2026-04-11/customers_cleaned.csv` (86 unique users) |
| Summary stats (JSON)                   | `data_lake/curated/prepared/2026-04-11/preparation_summary.json` |
| EDA plots (8 PNGs)                     | `reports/eda_plots/` |
| Preparation log                        | `logs/preparation.log` |

## Files Involved

```
preparation/prepare_data.py    ← Main preparation + EDA script (entry point)
config/pipeline_config.yaml    ← Path configuration
ingestion/utils.py             ← Shared utilities
```
