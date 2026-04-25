# Feature Logic Summary

**Generated:** 2026-04-25 09:32:16

---

## Overview

| Feature Table | Features | Rows |
|---------------|:--------:|-----:|
| user_features | 16 | 95,378 |
| item_features | 28 | 32,951 |
| interaction_features | 7 | 98,892 |

---

## user_features

| Feature | Type | Transformation | Description | Source |
|---------|------|---------------|-------------|--------|
| `purchase_count` | numeric | count | Total number of orders placed | `order_id` |
| `total_spending` | numeric | sum | Sum of all item prices (BRL) | `price` |
| `avg_order_value` | numeric | mean | Mean price per purchased item | `price` |
| `avg_rating_given` | numeric | mean | Mean review score given by user (1-5) | `review_score` |
| `review_count` | numeric | count | Number of reviews submitted | `review_score` |
| `distinct_products` | numeric | nunique | Unique products purchased | `product_id` |
| `distinct_categories` | numeric | nunique | Unique categories purchased | `category_english` |
| `preferred_category` | categorical | mode | Most frequently purchased category | `category_english` |
| `avg_freight` | numeric | mean | Mean shipping cost per item | `freight_value` |
| `customer_state` | categorical | first | Customer state code | `customer_state` |
| `recency_days` | numeric | derived | Days since last purchase relative to dataset end | `order_purchase_timestamp` |
| `purchase_freq_log` | numeric | log1p | Log-transformed purchase count | `purchase_count` |
| `spending_log` | numeric | log1p | Log-transformed total spending | `total_spending` |
| `spending_normalized` | numeric | min-max [0,1] | Normalized total spending | `total_spending` |
| `aov_normalized` | numeric | min-max [0,1] | Normalized avg order value | `avg_order_value` |

---

## item_features

| Feature | Type | Transformation | Description | Source |
|---------|------|---------------|-------------|--------|
| `total_sold` | numeric | count | Total units sold | `order_id` |
| `avg_rating_received` | numeric | mean | Mean review score received (1-5) | `review_score` |
| `review_count` | numeric | count | Number of reviews received | `review_score` |
| `avg_price` | numeric | mean | Mean selling price (BRL) | `price` |
| `total_revenue` | numeric | sum | Total revenue generated | `price` |
| `distinct_buyers` | numeric | nunique | Number of unique buyers | `customer_unique_id` |
| `product_volume_cm3` | numeric | derived (LxHxW) | Product volume in cm3 | `product_length/height/width_cm` |
| `popularity_rank` | numeric | rank | Rank by total_sold (1 = most popular) | `total_sold` |
| `price_percentile` | numeric | rank(pct) | Price percentile [0,1] | `avg_price` |
| `price_normalized` | numeric | min-max [0,1] | Normalized average price | `avg_price` |
| `sold_normalized` | numeric | min-max [0,1] | Normalized sales volume | `total_sold` |
| `rating_normalized` | numeric | min-max [0,1] | Normalized average rating | `avg_rating_received` |
| `is_cat_*` | binary | one-hot | One-hot encoding for top 10 categories + other | `category_english` |

---

## interaction_features

| Feature | Type | Transformation | Description | Source |
|---------|------|---------------|-------------|--------|
| `rating` | numeric | mean | Explicit rating (review score) | `review_score` |
| `purchase_count` | numeric | count | Times user bought this item | `order_id` |
| `implicit_signal` | binary | flag | 1 if purchased, 0 otherwise | `order_id` |
| `rating_normalized` | numeric | min-max [0,1] | Normalized rating | `rating` |
| `user_item_affinity` | numeric | weighted (0.6xrating + 0.4xfreq) | Composite affinity score | `rating, purchase_count` |

---

## Transformation Details

| Transformation | Formula | Applied To |
|---------------|---------|-----------|
| **Min-Max Normalization** | `(x - min) / (max - min)` -> [0, 1] | spending, AOV, price, sold, rating |
| **Log Transform** | `log(1 + x)` | purchase_count, total_spending |
| **One-Hot Encoding** | Binary 0/1 for each category | Top 10 categories + 'other' |
| **Rank** | Ascending/descending integer rank | popularity_rank, price_percentile |
| **Recency** | `max_date - last_purchase_date` in days | recency_days |
| **Composite Score** | `0.6 x rating_norm + 0.4 x freq_norm` | user_item_affinity |
