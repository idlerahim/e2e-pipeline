"""
RocoMart Recommendation Evaluation Metrics

Implements ranking-based metrics for recommendation system evaluation:
- Precision@K: Fraction of recommended items that are relevant
- Recall@K: Fraction of relevant items that are recommended
- NDCG@K: Normalized Discounted Cumulative Gain (position-aware ranking metric)
- MAP@K: Mean Average Precision

These metrics are standard for evaluating recommendation systems.
"""

import numpy as np
from typing import List, Tuple, Dict


def precision_at_k(y_true: np.ndarray, y_score: np.ndarray, k: int = 10) -> float:
    """
    Compute Precision@K for ranking evaluation.
    
    Precision@K = (# of relevant items in top K) / K
    
    Args:
        y_true: Binary relevance labels (1 = relevant, 0 = not relevant)
        y_score: Predicted scores/probabilities for ranking
        k: Number of items to consider
    
    Returns:
        Precision@K score (0 to 1)
    """
    if len(y_true) == 0:
        return 0.0
    
    k = min(k, len(y_true))
    idx = np.argsort(y_score)[::-1][:k]  # Top K indices by score
    return np.mean(y_true[idx])


def recall_at_k(y_true: np.ndarray, y_score: np.ndarray, k: int = 10) -> float:
    """
    Compute Recall@K for ranking evaluation.
    
    Recall@K = (# of relevant items in top K) / (# of relevant items)
    
    Args:
        y_true: Binary relevance labels
        y_score: Predicted scores
        k: Number of items to consider
    
    Returns:
        Recall@K score (0 to 1)
    """
    if len(y_true) == 0 or np.sum(y_true) == 0:
        return 0.0
    
    k = min(k, len(y_true))
    idx = np.argsort(y_score)[::-1][:k]
    return np.sum(y_true[idx]) / np.sum(y_true)


def ndcg_at_k(y_true: np.ndarray, y_score: np.ndarray, k: int = 10) -> float:
    """
    Compute Normalized Discounted Cumulative Gain@K.
    
    NDCG@K rewards relevant items that appear earlier in the ranking.
    DCG@K = sum(rel_i / log2(i+1)) for i=1..K
    NDCG@K = DCG@K / iDCG@K (normalized by ideal ranking)
    
    Args:
        y_true: Relevance labels (can be ratings 0-5)
        y_score: Predicted scores
        k: Number of items to consider
    
    Returns:
        NDCG@K score (0 to 1)
    """
    if len(y_true) == 0 or np.sum(y_true) == 0:
        return 0.0
    
    k = min(k, len(y_true))
    
    # Predicted ranking
    idx = np.argsort(y_score)[::-1][:k]
    dcg = np.sum(y_true[idx] / np.log2(np.arange(2, k + 2)))
    
    # Ideal ranking (sorted labels)
    ideal_idx = np.argsort(y_true)[::-1][:k]
    idcg = np.sum(y_true[ideal_idx] / np.log2(np.arange(2, k + 2)))
    
    if idcg == 0:
        return 0.0
    
    return dcg / idcg


def mean_average_precision(y_true: np.ndarray, y_score: np.ndarray, k: int = 10) -> float:
    """
    Compute Mean Average Precision@K.
    
    AP@K = sum(P@i * rel_i) / min(# relevant items, K)
    where P@i is precision at position i
    
    Args:
        y_true: Binary relevance labels
        y_score: Predicted scores
        k: Number of items to consider
    
    Returns:
        MAP@K score (0 to 1)
    """
    if len(y_true) == 0 or np.sum(y_true) == 0:
        return 0.0
    
    k = min(k, len(y_true))
    idx = np.argsort(y_score)[::-1][:k]
    rel = y_true[idx]
    
    # Cumulative precisions at positions where relevant items appear
    cumsum = np.cumsum(rel)
    precisions = cumsum * rel / (np.arange(1, k + 1))
    
    return np.sum(precisions) / min(np.sum(y_true), k)


def compute_ranking_metrics(y_true: np.ndarray, y_score: np.ndarray, 
                            k_values: List[int] = [5, 10, 20]) -> Dict[str, float]:
    """
    Compute all ranking metrics for given k values.
    
    Args:
        y_true: Relevance labels
        y_score: Predicted scores
        k_values: List of K values to evaluate at
    
    Returns:
        Dictionary with metrics for each K value
    """
    metrics = {}
    
    for k in k_values:
        metrics[f'precision@{k}'] = precision_at_k(y_true, y_score, k)
        metrics[f'recall@{k}'] = recall_at_k(y_true, y_score, k)
        metrics[f'ndcg@{k}'] = ndcg_at_k(y_true, y_score, k)
        metrics[f'map@{k}'] = mean_average_precision(y_true, y_score, k)
    
    return metrics


def compute_per_user_metrics(df, k: int = 10) -> Dict[str, float]:
    """
    Compute ranking metrics per-user and aggregate.
    
    Useful for recommendation systems where each user has their own ranking.
    
    Args:
        df: DataFrame with columns ['user_id', 'item_id', 'score', 'relevant']
        k: Number of items to consider
    
    Returns:
        Dictionary with aggregated metrics
    """
    metrics = {
        'precision@k': [],
        'recall@k': [],
        'ndcg@k': [],
        'map@k': []
    }
    
    for user_id in df['user_id'].unique():
        user_df = df[df['user_id'] == user_id].copy()
        
        if len(user_df) == 0 or user_df['relevant'].sum() == 0:
            continue
        
        y_true = user_df['relevant'].values
        y_score = user_df['score'].values
        
        metrics['precision@k'].append(precision_at_k(y_true, y_score, k))
        metrics['recall@k'].append(recall_at_k(y_true, y_score, k))
        metrics['ndcg@k'].append(ndcg_at_k(y_true, y_score, k))
        metrics['map@k'].append(mean_average_precision(y_true, y_score, k))
    
    # Return mean and std across users
    result = {}
    for metric_name, values in metrics.items():
        if values:
            result[f'{metric_name}_mean'] = np.mean(values)
            result[f'{metric_name}_std'] = np.std(values)
    
    return result
