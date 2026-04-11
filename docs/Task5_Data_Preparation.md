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
| **Customers**   | De-duplicated by `customer_unique_id` (99,441 → 96,096), standardized city/state                     |
| **Products**    | Translated categories PT→EN, imputed 610 missing category names, imputed 2 missing dimensions (median) |
| **Orders**      | Parsed timestamps, filtered to**delivered only** (99,441 → 96,478, keeping 97%)                  |
| **Reviews**     | Filled missing text with empty string, added `has_review_text` flag                                   |
| **Payments**    | Aggregated per order (total_payment, num_payments, primary_payment_type)                                |
| **Geolocation** | De-duplicated by zip (1M → 19,008), removed lat/lng outliers outside Brazil                            |

### Step 2: Merge into Unified Transaction Dataset

Joins: orders × items × reviews × payments × customers × products → **110,840 rows × 27 cols**

### Step 3: User-Item Interaction Matrix

- **93,263 users** × **31,439 items** → **96,739 interactions**
- **Sparsity: 99.997%** (typical for recommendation systems)
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
| Transactions dataset (cleaned, merged) | `data_lake/curated/prepared/YYYY-MM-DD/transactions.csv`           |
| User-item interaction matrix           | `data_lake/curated/prepared/YYYY-MM-DD/user_item_interactions.csv` |
| Cleaned products                       | `data_lake/curated/prepared/YYYY-MM-DD/products_cleaned.csv`       |
| Cleaned customers                      | `data_lake/curated/prepared/YYYY-MM-DD/customers_cleaned.csv`      |
| Summary stats (JSON)                   | `data_lake/curated/prepared/YYYY-MM-DD/preparation_summary.json`   |
| EDA plots (8 PNGs)                     | `reports/eda_plots/`                                               |
| Preparation log                        | `logs/preparation.log`                                             |

## Files Involved

```
preparation/prepare_data.py    ← Main preparation + EDA script (entry point)
config/pipeline_config.yaml    ← Path configuration
ingestion/utils.py             ← Shared utilities
```
