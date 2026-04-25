# Model Performance Report

Generated: 2026-04-25 09:32:39

## Executive Summary

This report evaluates recommendation models using ranking metrics (Precision@K, Recall@K, NDCG@K).

### Ranking Metrics (Top-K Evaluation)

These metrics evaluate the quality of ranked recommendations:

| Model | Precision@5 | Recall@5 | NDCG@5 |
|-------|:---:|:---:|:---:|
| KNN | 0.0412 | 0.2059 | 0.1616 |

## Metric Definitions

### Ranking Metrics
- **Precision@K**: Fraction of recommended items (top K) that are relevant
- **Recall@K**: Fraction of all relevant items that appear in top K recommendations
- **NDCG@K**: Normalized Discounted Cumulative Gain - rewards relevant items ranked higher

## Model Descriptions

### KNN (K-Nearest Neighbors)
- **Type**: Collaborative Filtering (Memory-based Category User-User)
- **Strengths**: Captures user affinity to specific product categories
- **Use Case**: Providing personalized item recommendations based on categories users tend to purchase from
- **Parameters**: K=10, similarity metric: cosine, user-based

## Recommendations

**Best ranking model**: KNN
  - NDCG@5: 0.1616
  - Precision@5: 0.0412

## Production Deployment

The trained models can be deployed using the inference API:

```bash
python -m inference.inference_api --port 8000
```

See `inference/inference_api.py` for API documentation and usage examples.
