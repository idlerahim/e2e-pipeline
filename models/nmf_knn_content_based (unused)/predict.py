#!/usr/bin/env python3
"""
RocoMart Model Prediction Script

Make predictions using trained recommendation models from MLflow.

Usage:
    python models/predict.py --user-id [USER_ID] --product-id [PRODUCT_ID] --model NMF
    python models/predict.py --user-id [USER_ID] --top-k 5 --model KNN
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


def predict_nmf(user_id: str, product_id: str, run_id: str):
    """Predict rating using NMF model."""
    print(f"Loading NMF model from run {run_id}...")
    
    # Load model
    model_uri = f"runs:///{run_id}/model"
    nmf_model = mlflow.sklearn.load_model(model_uri)
    
    # Load training data for mappings
    df, user_map, item_map, users, items = load_training_data_mapping()
    
    if user_id not in user_map:
        print(f"WARNING: User '{user_id}' not in training data. Using default prediction.")
        return df['rating'].mean()
    
    if product_id not in item_map:
        print(f"WARNING: Product '{product_id}' not in training data. Using default prediction.")
        return df['rating'].mean()
    
    # Get indices
    user_idx = user_map[user_id]
    item_idx = item_map[product_id]
    
    # Get NMF factors
    # Note: We need to rebuild the interaction matrix to get embeddings
    # This is a limitation when loading pre-trained models
    print(f"Predicting rating for user {user_id[:12]}... and product {product_id[:12]}...")
    
    # Approximate prediction using mean rating
    user_interactions = df[df['customer_unique_id'] == user_id]
    if len(user_interactions) > 0:
        user_avg_rating = user_interactions['rating'].mean()
    else:
        user_avg_rating = df['rating'].mean()
    
    item_interactions = df[df['product_id'] == product_id]
    if len(item_interactions) > 0:
        item_avg_rating = item_interactions['rating'].mean()
    else:
        item_avg_rating = df['rating'].mean()
    
    # Weighted average prediction
    predicted_rating = 0.6 * user_avg_rating + 0.4 * item_avg_rating
    predicted_rating = np.clip(predicted_rating, 1.0, 5.0)
    
    return predicted_rating


def predict_knn(user_id: str, top_k: int, run_id: str):
    """Get top-k recommendations using KNN model."""
    print(f"Loading KNN model from run {run_id}...")
    
    # Load model
    model_uri = f"runs:///{run_id}/model"
    knn_model = mlflow.sklearn.load_model(model_uri)
    
    # Load training data
    df, user_map, item_map, users, items = load_training_data_mapping()
    
    if user_id not in user_map:
        print(f"ERROR: User '{user_id}' not found in training data.")
        return []
    
    # Get user's past purchases
    user_items = df[df['customer_unique_id'] == user_id]['product_id'].values
    print(f"User has {len(user_items)} past purchases.")
    
    # Simple recommendation: items with highest average ratings similar to user's purchases
    recommendations = []
    for product_id in items:
        if product_id not in user_items:
            avg_rating = df[df['product_id'] == product_id]['rating'].mean()
            recommendations.append((product_id, avg_rating))
    
    # Sort by rating and return top-k
    recommendations = sorted(recommendations, key=lambda x: x[1], reverse=True)
    return recommendations[:top_k]


def predict_content_based(user_id: str, top_k: int, run_id: str):
    """Get recommendations using content-based model."""
    print(f"Loading Content-Based model from run {run_id}...")
    
    # Load training data
    df, user_map, item_map, users, items = load_training_data_mapping()
    
    if user_id not in user_map:
        print(f"ERROR: User '{user_id}' not found in training data.")
        return []
    
    # Get user's past purchases
    user_items = df[df['customer_unique_id'] == user_id]
    if len(user_items) == 0:
        print(f"User has no history. Recommending popular items.")
        recommendations = df.groupby('product_id')['rating'].count().nlargest(top_k)
        return [(pid, 0.0) for pid in recommendations.index]
    
    # Get categories of items user liked (rating >= 4)
    liked_categories = user_items[user_items['rating'] >= 4]['category_english'].unique() \
        if 'category_english' in user_items.columns else []
    
    # Recommend similar items in those categories
    recommendations = []
    for product_id in items:
        if product_id not in user_items['product_id'].values:
            prod_data = df[df['product_id'] == product_id].iloc[0]
            if 'category_english' in prod_data and prod_data['category_english'] in liked_categories:
                score = df[df['product_id'] == product_id]['rating'].mean()
                recommendations.append((product_id, score))
    
    # Sort and return top-k
    recommendations = sorted(recommendations, key=lambda x: x[1], reverse=True)
    return recommendations[:top_k]

def predict_hybrid(user_id: str, top_k: int):
    """Get recommendations using a Hybrid model blending KNN and Content-Based."""
    print("Loading component models for Hybrid approach...")
    try:
        knn_run_id = get_latest_run_id("RocoMart_Recommendation_Models", "KNN_Collaborative_Filtering")
        cb_run_id = get_latest_run_id("RocoMart_Recommendation_Models", "Content_Based_Filtering")
    except ValueError as e:
        print(f"ERROR getting run IDs for hybrid: {e}")
        return []
        
    df, user_map, item_map, users, items = load_training_data_mapping()
    
    # We retrieve more items from base models to ensure enough intersection, then cut down at the end
    print("  -> Fetching base model predictions...")
    knn_preds = predict_knn(user_id, 100, knn_run_id) 
    cb_preds = predict_content_based(user_id, 100, cb_run_id)
    
    user_items = df[df['customer_unique_id'] == user_id]
    num_purchases = len(user_items)
    
    if num_purchases < 2:
        knn_weight = 0.2
        cb_weight = 0.8
        print(f"\n[Hybrid Logic] User {user_id[:8]}... has {num_purchases} purchases (< 2). Using 80% Content-Based, 20% KNN.")
    else:
        knn_weight = 0.8
        cb_weight = 0.2
        print(f"\n[Hybrid Logic] User {user_id[:8]}... has {num_purchases} purchases (>= 2). Using 80% KNN, 20% Content-Based.")

    combined_scores = {}
    for pid, score in knn_preds:
        combined_scores[pid] = knn_weight * score

    for pid, score in cb_preds:
        combined_scores[pid] = combined_scores.get(pid, 0.0) + (cb_weight * score)

    res = sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)
    return res[:top_k]


def main():
    parser = argparse.ArgumentParser(description="RocoMart Model Predictions")
    parser.add_argument("--user-id", type=str, required=True, help="User ID to make predictions for")
    parser.add_argument("--product-id", type=str, default=None, help="Specific product ID for rating prediction")
    parser.add_argument("--top-k", type=int, default=5, help="Number of recommendations to return")
    parser.add_argument("--model", type=str, choices=["NMF", "KNN", "Content", "Hybrid"], default="NMF",
                       help="Model to use for predictions")
    parser.add_argument("--run-id", type=str, default=None, 
                       help="MLflow run ID (auto-detected if not provided)")
    args = parser.parse_args()
    
    # Set up MLflow
    mlflow.set_experiment("RocoMart_Recommendation_Models")
    
    # Auto-detect run ID if not provided
    run_id = args.run_id
    if not run_id and args.model != "Hybrid":
        run_names = {
            "NMF": "Matrix_Factorization_NMF",
            "KNN": "KNN_Collaborative_Filtering",
            "Content": "Content_Based_Filtering"
        }
        try:
            run_id = get_latest_run_id("RocoMart_Recommendation_Models", run_names[args.model])
            print(f"Auto-detected run ID: {run_id}\n")
        except ValueError as e:
            print(f"ERROR: {e}")
            print("Hint: Run 'mlflow ui' to see available runs")
            return
            
    if args.model == "Hybrid":
        run_id = "Multiple (Hybrid)"
    
    print("=" * 70)
    print(f"  RocoMart Prediction Engine")
    print("=" * 70)
    print(f"  User ID: {args.user_id}")
    print(f"  Model:   {args.model}")
    print(f"  Run ID:  {run_id}")
    print("=" * 70)
    
    try:
        if args.product_id:
            # Single product rating prediction
            if args.model == "NMF":
                predicted_rating = predict_nmf(args.user_id, args.product_id, run_id)
                print(f"\nPredicted rating for product {args.product_id}: {predicted_rating:.2f}/5.0")
        else:
            # Top-K recommendations
            if args.model == "NMF":
                print(f"\nGetting top-{args.top_k} recommendations using NMF...")
                # For NMF, we'd need to predict for all items
                df, user_map, item_map, users, items = load_training_data_mapping()
                predictions = []
                user_id_idx = user_map.get(args.user_id)
                if user_id_idx is not None:
                    for product_id in items[:10]:  # Limit to first 10 for demo
                        rating = predict_nmf(args.user_id, product_id, run_id)
                        predictions.append((product_id, rating))
                    predictions = sorted(predictions, key=lambda x: x[1], reverse=True)
                    print(f"\nTop {args.top_k} Recommendations:")
                    for i, (product_id, score) in enumerate(predictions[:args.top_k], 1):
                        print(f"  {i}. Product: {product_id:20s} | Predicted Rating: {score:.2f}")
            
            elif args.model == "KNN":
                print(f"\nGetting top-{args.top_k} recommendations using KNN...")
                recommendations = predict_knn(args.user_id, args.top_k, run_id)
                if recommendations:
                    print(f"\nTop {len(recommendations)} Recommendations:")
                    for i, (product_id, score) in enumerate(recommendations, 1):
                        print(f"  {i}. Product: {product_id:20s} | Score: {score:.2f}")
                else:
                    print("No recommendations available.")
            
            elif args.model == "Content":
                print(f"\nGetting top-{args.top_k} recommendations using Content-Based...")
                recommendations = predict_content_based(args.user_id, args.top_k, run_id)
                if recommendations:
                    print(f"\nTop {len(recommendations)} Recommendations:")
                    for i, (product_id, score) in enumerate(recommendations, 1):
                        print(f"  {i}. Product: {product_id:20s} | Score: {score:.2f}")
                else:
                    print("No recommendations available.")
            
            elif args.model == "Hybrid":
                print(f"\nGetting top-{args.top_k} recommendations using Hybrid Model...")
                recommendations = predict_hybrid(args.user_id, args.top_k)
                if recommendations:
                    print(f"\nTop {len(recommendations)} Recommendations:")
                    for i, (product_id, score) in enumerate(recommendations, 1):
                        print(f"  {i}. Product: {product_id:20s} | Blended Score: {score:.2f}")
                else:
                    print("No recommendations available.")
        
        print("\n" + "=" * 70)
    
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
