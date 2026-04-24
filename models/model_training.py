#!/usr/bin/env python3
"""
RocoMart Model Training and Evaluation Script

Trains and evaluates recommendation models using collaborative filtering.
Tracks experiments with MLflow.

Models:
1. SVD (Matrix Factorization)
2. KNN (Collaborative Filtering)
3. Content-Based Filtering

Usage:
    python model_training.py
"""

import os
import sys
import pandas as pd
import numpy as np

from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split
from sklearn.neighbors import NearestNeighbors
import mlflow

def _normalize_metric_name(metric_name):
    return metric_name.replace("@", "_").replace("/", "_").replace(" ", "_")
import mlflow.sklearn
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.evaluation_metrics import (
    precision_at_k,
    recall_at_k,
    ndcg_at_k,
    mean_average_precision,
    compute_ranking_metrics
)

def load_training_data():
    """Load training data from feature store."""
    training_sets_dir = "data_lake/serving/training_sets"
    if not os.path.exists(training_sets_dir):
        raise FileNotFoundError(f"Training sets directory not found: {training_sets_dir}")

    # Find the latest training set
    files = [f for f in os.listdir(training_sets_dir) if f.startswith("training_set_")]
    if not files:
        raise FileNotFoundError("No training set files found")

    latest_file = max(files)
    training_file = os.path.join(training_sets_dir, latest_file)

    print(f"Loading training data from: {training_file}")
    df = pd.read_csv(training_file)

    # Ensure required columns exist
    required_cols = ['customer_unique_id', 'product_id', 'rating']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Training data must contain columns: {required_cols}")

    return df

def load_product_features():
    """Load product features for content-based filtering."""
    features_dir = "data_lake/curated/features"
    if os.path.exists(features_dir):
        files = [f for f in os.listdir(features_dir) if f.endswith("item_features.csv")]
        if files:
            latest_file = max(files)
            features_file = os.path.join(features_dir, latest_file)
            print(f"Loading product features from: {features_file}")
            return pd.read_csv(features_file)

    # Fallback to the raw product dataset if curated item features are unavailable
    products_path = "dataset/olist_products_dataset.csv"
    translation_path = "dataset/product_category_name_translation.csv"
    
    if os.path.exists(products_path):
        print(f"Loading product features from fallback dataset: {products_path}")
        prod_df = pd.read_csv(products_path)
        if os.path.exists(translation_path):
            trans_df = pd.read_csv(translation_path)
            prod_df = pd.merge(prod_df, trans_df, on='product_category_name', how='left')
        return prod_df

    raise FileNotFoundError("No item features file found and no fallback dataset available")


def compute_ranking_metrics_from_predictions(predictions, k_values=[5, 10]):
    """
    Compute ranking metrics (Precision@K, Recall@K, NDCG@K, MAP@K) from prediction records.
    
    Args:
        predictions: List of dictionaries with keys 'uid', 'iid', 'true_rating', 'pred_rating'
        k_values: List of K values to evaluate at
    
    Returns:
        Dictionary with computed metrics
    """
    # Convert predictions to DataFrame
    pred_df = pd.DataFrame([
        {
            'user': pred['uid'],
            'item': pred['iid'],
            'true_rating': pred['true_rating'],
            'pred_rating': pred['pred_rating']
        }
        for pred in predictions
    ])
    
    metrics = {f'precision@{k}': [] for k in k_values}
    metrics.update({f'recall@{k}': [] for k in k_values})
    metrics.update({f'ndcg@{k}': [] for k in k_values})
    metrics.update({f'map@{k}': [] for k in k_values})
    
    # Compute metrics per user
    for user in pred_df['user'].unique():
        user_preds = pred_df[pred_df['user'] == user].sort_values('pred_rating', ascending=False)
        
        # Create binary relevance (>= 4 is considered relevant)
        y_true = (user_preds['true_rating'] >= 4).astype(int).values
        y_score = user_preds['pred_rating'].values
        
        if len(y_true) == 0 or np.sum(y_true) == 0:
            continue
        
        for k in k_values:
            metrics[f'precision@{k}'].append(precision_at_k(y_true, y_score, k))
            metrics[f'recall@{k}'].append(recall_at_k(y_true, y_score, k))
            metrics[f'ndcg@{k}'].append(ndcg_at_k(y_true, y_score, k))
            metrics[f'map@{k}'].append(mean_average_precision(y_true, y_score, k))
    
    # Compute aggregated metrics
    result = {}
    for metric_name, values in metrics.items():
        if values:
            result[metric_name] = np.mean(values)
            result[f'{metric_name}_std'] = np.std(values)

    return result


def encode_ids(df):
    users = sorted(df['customer_unique_id'].unique())
    items = sorted(df['product_id'].unique())
    user_map = {user: idx for idx, user in enumerate(users)}
    item_map = {item: idx for idx, item in enumerate(items)}
    return user_map, item_map, users, items


def build_interaction_matrix(df, user_map, item_map):
    matrix = np.zeros((len(user_map), len(item_map)), dtype=np.float32)
    for row in df.itertuples(index=False):
        if row.customer_unique_id in user_map and row.product_id in item_map:
            matrix[user_map[row.customer_unique_id], item_map[row.product_id]] = row.rating
    return matrix




def train_knn_model(all_data_df, train_df, test_df):
    """Train a KNN-based collaborative filtering model (User-Based on Categories)."""
    print("Training KNN model (User-Based on Categories)...")

    with mlflow.start_run(run_name="KNN_Collaborative_Filtering"):
        k = 10

        mlflow.log_param("model_type", "KNN")
        mlflow.log_param("k", k)
        mlflow.log_param("similarity", "cosine")
        mlflow.log_param("user_based", True)

        # Merge with product features to get categories
        products_df = load_product_features()
        if 'category_english' in products_df.columns:
            cat_col = 'category_english'
        elif 'product_category_name_english' in products_df.columns:
            cat_col = 'product_category_name_english'
        else:
            cat_col = 'product_category_name'

        # Merge all_data_df with categories
        merged_all = pd.merge(all_data_df, products_df[['product_id', cat_col]], on='product_id', how='left')
        merged_all[cat_col] = merged_all[cat_col].fillna('unknown')

        # Filter users with 2 or more distinct categories
        user_cat_counts = merged_all.groupby('customer_unique_id')[cat_col].nunique()
        multi_cat_users = user_cat_counts[user_cat_counts >= 2].index
        merged_filtered = merged_all[merged_all['customer_unique_id'].isin(multi_cat_users)]

        print(f"Users with 2+ categories: {merged_filtered['customer_unique_id'].nunique()}")

        # Pivot
        user_item_grid = merged_filtered.pivot_table(
            index='customer_unique_id', 
            columns=cat_col, 
            values='rating'
        ).fillna(0)

        nn = NearestNeighbors(n_neighbors=min(k + 1, user_item_grid.shape[0]), metric="cosine", algorithm='brute')
        nn.fit(user_item_grid)

        # Merge test_df to get categories like KNN Model.py's test_data
        test_merged = pd.merge(test_df, products_df[['product_id', cat_col]], on='product_id', how='left')
        test_merged[cat_col] = test_merged[cat_col].fillna('unknown')
        # rename columns to match KNN Model.py's expectations
        test_data = test_merged.rename(columns={
            'customer_unique_id': 'user_number', 
            'rating': 'review_score', 
            cat_col: 'product_category_name_english'
        })
        
        def get_comprehensive_metrics(model, grid, test_data_df, k=5, threshold=4):
            test_users = test_data_df['user_number'].unique()[:100] 
            precisions, recalls, ndcgs = [], [], []

            for user in test_users:
                try:
                    # 1. Get Recommendations
                    distances, indices = model.kneighbors(grid.loc[[user]], n_neighbors=k+1)
                    neighbor_indices = grid.iloc[indices[0][1:]].index
                    
                    # Get average scores for all categories from neighbors
                    recommendation_scores = grid.loc[neighbor_indices].mean()
                    top_k_items = recommendation_scores.sort_values(ascending=False).head(k).index
                    
                    # 2. Get Ground Truth (Actual scores from test set)
                    user_test_data = test_data_df[test_data_df['user_number'] == user]
                    actual_liked = user_test_data[user_test_data['review_score'] >= threshold]['product_category_name_english'].tolist()
                    
                    if not actual_liked: continue

                    # 3. Calculate Precision & Recall
                    hits = len(set(top_k_items) & set(actual_liked))
                    precisions.append(hits / k)
                    recalls.append(hits / len(actual_liked))
                    
                    from sklearn.metrics import ndcg_score
                    # 4. Calculate NDCG
                    # Create a true relevance array based on test data
                    true_relevance = np.zeros(len(grid.columns))
                    for _, row in user_test_data.iterrows():
                        if row['product_category_name_english'] in grid.columns:
                            idx = grid.columns.get_loc(row['product_category_name_english'])
                            true_relevance[idx] = row['review_score']
                    
                    # Use neighbor average scores as predicted relevance
                    pred_relevance = recommendation_scores.values
                    ndcgs.append(ndcg_score([true_relevance], [pred_relevance], k=k))
                    
                except Exception as e: 
                    continue

            return np.mean(precisions) if precisions else 0, np.mean(recalls) if recalls else 0, np.mean(ndcgs) if ndcgs else 0
            
        avg_p, avg_r, avg_n = get_comprehensive_metrics(nn, user_item_grid, test_data, k=5)
        
        mlflow.log_metric("precision_5", avg_p)
        mlflow.log_metric("recall_5", avg_r)
        mlflow.log_metric("ndcg_5", avg_n)
        
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            grid_path = os.path.join(tmpdir, "user_item_grid.csv")
            user_item_grid.to_csv(grid_path)
            mlflow.log_artifact(grid_path)

        mlflow.sklearn.log_model(nn, "model")

        ranking_metrics = {'precision@5': avg_p, 'recall@5': avg_r, 'ndcg@5': avg_n}
        print(f"KNN (Category User-Based) - Precision@5: {avg_p:.4f}, Recall@5: {avg_r:.4f}, NDCG@5: {avg_n:.4f}")

        # Return dummy RMSE/MAE
        return nn, 0.0, 0.0, ranking_metrics


def create_performance_report(results):
    """Create a comprehensive performance report with ranking metrics."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"reports/{ts}_model_performance_report.md"

    with open(report_path, 'w') as f:
        f.write("# Model Performance Report\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write("## Executive Summary\n\n")
        f.write("This report evaluates recommendation models using ranking metrics (Precision@K, Recall@K, NDCG@K).\n\n")

        f.write("### Ranking Metrics (Top-K Evaluation)\n\n")
        f.write("These metrics evaluate the quality of ranked recommendations:\n\n")
        f.write("| Model | Precision@5 | Recall@5 | NDCG@5 |\n")
        f.write("|-------|:---:|:---:|:---:|\n")

        for model_name, metrics in results.items():
            precision_5 = metrics.get('precision@5', 0)
            recall_5 = metrics.get('recall@5', 0)
            ndcg_5 = metrics.get('ndcg@5', 0)
            
            if any([precision_5, recall_5, ndcg_5]):  # Only show if ranking metrics exist
                f.write(f"| {model_name} | {precision_5:.4f} | {recall_5:.4f} | {ndcg_5:.4f} |\n")

        f.write("\n## Metric Definitions\n\n")
        f.write("### Ranking Metrics\n")
        f.write("- **Precision@K**: Fraction of recommended items (top K) that are relevant\n")
        f.write("- **Recall@K**: Fraction of all relevant items that appear in top K recommendations\n")
        f.write("- **NDCG@K**: Normalized Discounted Cumulative Gain - rewards relevant items ranked higher\n\n")

        f.write("## Model Descriptions\n\n")
        f.write("### KNN (K-Nearest Neighbors)\n")
        f.write("- **Type**: Collaborative Filtering (Memory-based Category User-User)\n")
        f.write("- **Strengths**: Captures user affinity to specific product categories\n")
        f.write("- **Use Case**: Providing personalized item recommendations based on categories users tend to purchase from\n")
        f.write("- **Parameters**: K=10, similarity metric: cosine, user-based\n\n")


        f.write("## Recommendations\n\n")
        if results:
            # Find best model by ranking metrics
            ranking_models = {k: v for k, v in results.items() if 'ndcg@5' in v}
            if ranking_models:
                best_ranking = max(ranking_models.keys(), key=lambda x: ranking_models[x].get('ndcg@5', 0))
                f.write(f"**Best ranking model**: {best_ranking}\n")
                f.write(f"  - NDCG@5: {ranking_models[best_ranking].get('ndcg@5', 0):.4f}\n")
                f.write(f"  - Precision@5: {ranking_models[best_ranking].get('precision@5', 0):.4f}\n\n")

        f.write("## Production Deployment\n\n")
        f.write("The trained models can be deployed using the inference API:\n\n")
        f.write("```bash\n")
        f.write("python -m inference.inference_api --port 8000\n")
        f.write("```\n\n")
        f.write("See `inference/inference_api.py` for API documentation and usage examples.\n")

    print(f"Performance report saved to: {report_path}")

def main():
    """Main training pipeline."""
    print("Starting RocoMart Model Training Pipeline")

    # Set MLflow experiment
    mlflow.set_experiment("RocoMart_Recommendation_Models")

    try:
        # Load data
        df = load_training_data()

        # Split data into train and test sets
        train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)

        results = {}

        # Train KNN model
        knn_model, knn_rmse, knn_mae, knn_ranking = train_knn_model(df, train_df, test_df)
        results['KNN'] = {'rmse': knn_rmse, 'mae': knn_mae, **knn_ranking}

        # Create performance report
        create_performance_report(results)

        print("Training pipeline completed successfully!")

    except Exception as e:
        print(f"Error during training: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()