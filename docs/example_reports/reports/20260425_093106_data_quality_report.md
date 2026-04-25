# Data Quality Report

**Generated:** 2026-04-25 09:31:06
**Datasets validated:** 9

---

## Summary

| # | Dataset | Rows | Cols | Completeness | Duplicates | Status |
|---|---------|-----:|-----:|-------------:|-----------:|--------|
| 1 | `olist_customers_dataset.csv` | 99,441 | 5 | 100.0% | 0 | [OK] PASS |
| 2 | `olist_products_dataset.csv` | 32,951 | 9 | 99.17% | 0 | [OK] PASS |
| 3 | `olist_orders_dataset.csv` | 99,441 | 8 | 99.38% | 0 | [OK] PASS |
| 4 | `olist_order_items_dataset.csv` | 112,650 | 7 | 100.0% | 0 | [OK] PASS |
| 5 | `olist_order_reviews_dataset.csv` | 99,224 | 7 | 78.99% | 0 | [OK] PASS |
| 6 | `olist_order_payments_dataset.csv` | 103,886 | 5 | 100.0% | 0 | [OK] PASS |
| 7 | `olist_sellers_dataset.csv` | 3,095 | 4 | 100.0% | 0 | [OK] PASS |
| 8 | `olist_geolocation_dataset.csv` | 1,000,163 | 5 | 100.0% | 261,831 | [W] WARNING |
| 9 | `product_category_name_translation.csv` | 71 | 2 | 100.0% | 0 | [OK] PASS |

---

## olist_customers_dataset.csv

- **Rows:** 99,441
- **Columns:** 5
- **Overall Completeness:** 100.0%
- **Status:** PASS

### Missing Values

| Column | Missing | % Missing | % Complete |
|--------|--------:|----------:|-----------:|
| *(none)* | 0 | 0% | 100% |

---

## olist_products_dataset.csv

- **Rows:** 32,951
- **Columns:** 9
- **Overall Completeness:** 99.17%
- **Status:** PASS

### Missing Values

| Column | Missing | % Missing | % Complete |
|--------|--------:|----------:|-----------:|
| `product_category_name` | 610 | 1.85% | 98.15% |
| `product_name_lenght` | 610 | 1.85% | 98.15% |
| `product_description_lenght` | 610 | 1.85% | 98.15% |
| `product_photos_qty` | 610 | 1.85% | 98.15% |
| `product_weight_g` | 2 | 0.01% | 99.99% |
| `product_length_cm` | 2 | 0.01% | 99.99% |
| `product_height_cm` | 2 | 0.01% | 99.99% |
| `product_width_cm` | 2 | 0.01% | 99.99% |

---

## olist_orders_dataset.csv

- **Rows:** 99,441
- **Columns:** 8
- **Overall Completeness:** 99.38%
- **Status:** PASS

### Missing Values

| Column | Missing | % Missing | % Complete |
|--------|--------:|----------:|-----------:|
| `order_approved_at` | 160 | 0.16% | 99.84% |
| `order_delivered_carrier_date` | 1,783 | 1.79% | 98.21% |
| `order_delivered_customer_date` | 2,965 | 2.98% | 97.02% |

---

## olist_order_items_dataset.csv

- **Rows:** 112,650
- **Columns:** 7
- **Overall Completeness:** 100.0%
- **Status:** PASS

### Missing Values

| Column | Missing | % Missing | % Complete |
|--------|--------:|----------:|-----------:|
| *(none)* | 0 | 0% | 100% |

---

## olist_order_reviews_dataset.csv

- **Rows:** 99,224
- **Columns:** 7
- **Overall Completeness:** 78.99%
- **Status:** PASS

### Missing Values

| Column | Missing | % Missing | % Complete |
|--------|--------:|----------:|-----------:|
| `review_comment_title` | 87,656 | 88.34% | 11.66% |
| `review_comment_message` | 58,247 | 58.7% | 41.3% |

---

## olist_order_payments_dataset.csv

- **Rows:** 103,886
- **Columns:** 5
- **Overall Completeness:** 100.0%
- **Status:** PASS

### Missing Values

| Column | Missing | % Missing | % Complete |
|--------|--------:|----------:|-----------:|
| *(none)* | 0 | 0% | 100% |

---

## olist_sellers_dataset.csv

- **Rows:** 3,095
- **Columns:** 4
- **Overall Completeness:** 100.0%
- **Status:** PASS

### Missing Values

| Column | Missing | % Missing | % Complete |
|--------|--------:|----------:|-----------:|
| *(none)* | 0 | 0% | 100% |

---

## olist_geolocation_dataset.csv

- **Rows:** 1,000,163
- **Columns:** 5
- **Overall Completeness:** 100.0%
- **Status:** WARNING

### Missing Values

| Column | Missing | % Missing | % Complete |
|--------|--------:|----------:|-----------:|
| *(none)* | 0 | 0% | 100% |

### Issues Found

| Check | Severity | Detail |
|-------|----------|--------|
| full_row_duplicates | WARNING | 261,831 full-row duplicates found |

---

## product_category_name_translation.csv

- **Rows:** 71
- **Columns:** 2
- **Overall Completeness:** 100.0%
- **Status:** PASS

### Missing Values

| Column | Missing | % Missing | % Complete |
|--------|--------:|----------:|-----------:|
| *(none)* | 0 | 0% | 100% |

---
