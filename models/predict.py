#!/usr/bin/env python3
"""
RocoMart Model Prediction Script

Make predictions using trained recommendation models from MLflow.

Usage:
    python models/predict.py --user-id [USER_ID]
    python models/predict.py --user-id [USER_ID] --top-k 5
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
import mlflow
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from feature_store.feature_store_manager import FeatureStoreManager
from ingestion.utils import load_config, setup_logger, get_project_root


def get_latest_run_id(experiment_name: str, run_name: str):
    """Get the latest run ID for a specific model."""
    mlflow.set_experiment(experiment_name)
    experiment = mlflow.get_experiment_by_name(experiment_name)
    
    if not experiment:
        raise ValueError(f"Experiment '{experiment_name}' not found")
    
    # Get all runs for this experiment
    all_runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
    
    if all_runs.empty or 'tags.mlflow.runName' not in all_runs.columns:
        raise ValueError(f"No run found with name '{run_name}'")
    
    # Filter by run name
    matching_runs = all_runs[all_runs['tags.mlflow.runName'] == run_name]
    
    if matching_runs.empty:
        raise ValueError(f"No run found with name '{run_name}'")
    
    # Get the most recent run
    latest_run = matching_runs.sort_values('start_time', ascending=False).iloc[0]
    return latest_run['run_id']


def load_training_data_mapping():
    """Load training data to get user/item mappings."""
    training_sets_dir = "data_lake/serving/training_sets"
    files = [f for f in os.listdir(training_sets_dir) if f.startswith("training_set_")]
    if not files:
        raise FileNotFoundError("No training set files found")
    
    latest_file = max(files)
    training_file = os.path.join(training_sets_dir, latest_file)
    df = pd.read_csv(training_file)
    
    # Create ID mappings
    users = sorted(df['customer_unique_id'].unique())
    items = sorted(df['product_id'].unique())
    user_map = {user: idx for idx, user in enumerate(users)}
    item_map = {item: idx for idx, item in enumerate(items)}
    
    return df, user_map, item_map, users, items





def predict_knn(user_id: str, top_k: int, run_id: str):
    """Get top-k recommendations using KNN model."""
    print(f"Loading KNN model from run {run_id}...")
    
    # Load model
    model_uri = f"runs:///{run_id}/model"
    knn_model = mlflow.sklearn.load_model(model_uri)
    
    # Load artifacts (grid and columns)
    client = mlflow.tracking.MlflowClient()
    local_dir = client.download_artifacts(run_id, "")
    
    grid_path = os.path.join(local_dir, "user_item_grid.csv")
    if os.path.exists(grid_path):
        user_item_grid = pd.read_csv(grid_path, index_col=0)
    else:
        print("ERROR: user_item_grid not found in artifacts.")
        return []

    # Load training data
    df, user_map, item_map, users, items = load_training_data_mapping()
    
    if user_id not in user_item_grid.index:
        print(f"ERROR: User '{user_id}' not found in training grid.")
        return []
    
    user_profile = user_item_grid.loc[[user_id]]
    user_items = df[df['customer_unique_id'] == user_id]['product_id'].values
    print(f"User has {len(user_items)} past purchases.")
    
    k = 100
    distances, indices = knn_model.kneighbors(user_profile, n_neighbors=min(k, len(user_item_grid)))
    
    neighbor_indices = user_item_grid.iloc[indices[0]].index
    neighbor_data = user_item_grid.loc[neighbor_indices]
    
    purchase_counts = (neighbor_data > 0).sum()
    probabilities = (purchase_counts / len(neighbor_indices)) * 100
    
    user_bought_cats = user_profile.columns[(user_profile > 0).iloc[0]]
    recommendations = probabilities.drop(labels=user_bought_cats, errors='ignore').sort_values(ascending=False)
    
    # We need product categories to convert category predictions back to items
    from models.model_training import load_product_features
    products_df = load_product_features()
    
    if 'category_english' in products_df.columns:
        cat_col = 'category_english'
    elif 'product_category_name_english' in products_df.columns:
        cat_col = 'product_category_name_english'
    else:
        cat_col = 'product_category_name'
        
    prod_cat_map = dict(zip(products_df['product_id'], products_df[cat_col]))
    
    top_categories = recommendations.head(top_k * 2).index.tolist()
    
    final_recs = []
    for cat in top_categories:
        cat_products = [p for p in items if prod_cat_map.get(p) == cat and p not in user_items]
        
        cat_product_scores = []
        for p in cat_products:
            avg_rating = df[df['product_id'] == p]['rating'].mean()
            cat_product_scores.append((p, avg_rating + probabilities[cat]/100))
        
        cat_product_scores = sorted(cat_product_scores, key=lambda x: x[1], reverse=True)
        final_recs.extend(cat_product_scores[:2])
        
    final_recs = sorted(final_recs, key=lambda x: x[1], reverse=True)
    
    # Optional deduplication
    unique_recs = []
    seen_products = set()
    for pid, score in final_recs:
        if pid not in seen_products:
            unique_recs.append((pid, score))
            seen_products.add(pid)
            
    return unique_recs[:top_k]




def main():
    parser = argparse.ArgumentParser(description="RocoMart Model Predictions")
    parser.add_argument("--user-id", type=str, required=True, help="User ID to make predictions for")
    parser.add_argument("--top-k", type=int, default=5, help="Number of recommendations to return")
    parser.add_argument("--run-id", type=str, default=None, 
                       help="MLflow run ID (auto-detected if not provided)")
    args = parser.parse_args()
    
    # Set up MLflow
    mlflow.set_experiment("RocoMart_Recommendation_Models")
    
    # Auto-detect run ID if not provided
    run_id = args.run_id
    if not run_id:
        try:
            run_id = get_latest_run_id("RocoMart_Recommendation_Models", "KNN_Collaborative_Filtering")
            print(f"Auto-detected run ID: {run_id}\n")
        except ValueError as e:
            print(f"ERROR: {e}")
            print("Hint: Run 'mlflow ui' to see available runs")
            return
            
    print("=" * 70)
    print(f"  RocoMart Prediction Engine")
    print("=" * 70)
    print(f"  User ID: {args.user_id}")
    print(f"  Model:   KNN")
    print(f"  Run ID:  {run_id}")
    print("=" * 70)
    
    try:
        # Top-K recommendations
        print(f"\nGetting top-{args.top_k} recommendations using KNN...")
        recommendations = predict_knn(args.user_id, args.top_k, run_id)
        if recommendations:
            print(f"\nTop {len(recommendations)} Recommendations:")
            for i, (product_id, score) in enumerate(recommendations, 1):
                print(f"  {i}. Product: {product_id:20s} | Score: {score:.2f}")
        else:
            print("No recommendations available.")
        
        print("\n" + "=" * 70)
    
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
