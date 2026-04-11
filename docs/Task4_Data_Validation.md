# Task 4: Data Profiling and Validation

## How to Run

```bash
python -m validation.validate_data
```

## Input

| Input                | What                                        | Location                                                |
| -------------------- | ------------------------------------------- | ------------------------------------------------------- |
| Raw CSV datasets (9) | Source data files                           | `dataset/` folder                                     |
| Expected schemas     | Column names, types, ranges, allowed values | Defined in `validate_data.py` → `EXPECTED_SCHEMAS` |
| Config               | Path settings                               | `config/pipeline_config.yaml`                         |

## What It Does

Runs **5 automated checks** on each of the 9 datasets:

| # | Check                         | What It Validates                                                                      |
| - | ----------------------------- | -------------------------------------------------------------------------------------- |
| 1 | **Schema validation**   | Expected columns present, no missing/extra columns                                     |
| 2 | **Missing values**      | Null count & % per column, overall completeness                                        |
| 3 | **Duplicate detection** | Full-row duplicates + key column uniqueness                                            |
| 4 | **Range checks**        | Numeric values within expected bounds (e.g.`review_score` ∈ [1, 5])                 |
| 5 | **Allowed values**      | Categorical columns only contain valid values (e.g.`order_status`, `payment_type`) |

Plus **column profiling** (min, max, mean, std, top values, cardinality) for every column.

## Output

| Output                         | Location                                             |
| ------------------------------ | ---------------------------------------------------- |
| Data quality report (JSON)     | `reports/data_quality_report_YYYYMMDD_HHMMSS.json` |
| Data quality report (Markdown) | `reports/data_quality_report_YYYYMMDD_HHMMSS.md`   |
| Validation log                 | `logs/validation.log`                              |

## Key Findings

| Dataset              | Completeness | Duplicates | Issues                                                    |
| -------------------- | :----------: | :--------: | --------------------------------------------------------- |
| customers            |     100%     |     0     | PASS                                                      |
| products             |    99.2%    |     0     | PASS - 610 missing category names, 2 missing dimensions  |
| orders               |    99.4%    |     0     | PASS - some missing delivery dates (undelivered orders)   |
| order_items          |     100%     |     0     | PASS                                                      |
| order_reviews        |    79.0%    |     0     | PASS - 88% missing review titles, 59% missing review text |
| order_payments       |     100%     |     0     | PASS                                                      |
| sellers              |     100%     |     0     | PASS                                                      |
| geolocation          |     100%     |  261,831  | ERROR - duplicate rows + 48 lat/lng outliers              |
| category_translation |     100%     |     0     | PASS                                                      |

## Files Involved

```
validation/validate_data.py    ← Main validation script (entry point)
config/pipeline_config.yaml    ← Path configuration
ingestion/utils.py             ← Shared utilities (logging, config)
reports/                       ← Output directory for quality reports
```
