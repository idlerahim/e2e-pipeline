import os
import sys
import pandas as pd
import numpy as np
import mlflow
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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

def main():
    print("Initializing Static Predictor...")
    
    mlflow.set_experiment("RocoMart_Recommendation_Models")
    
    try:
        run_id = get_latest_run_id("RocoMart_Recommendation_Models", "KNN_Collaborative_Filtering")
    except ValueError as e:
        print(f"ERROR: {e}")
        return

    print(f"Loading KNN model from run {run_id}...")
    model_uri = f"runs:///{run_id}/model"
    model_knn = mlflow.sklearn.load_model(model_uri)

    client = mlflow.tracking.MlflowClient()
    local_dir = client.download_artifacts(run_id, "")
    
    grid_path = os.path.join(local_dir, "user_item_grid.csv")
    if os.path.exists(grid_path):
        user_item_grid = pd.read_csv(grid_path, index_col=0)
    else:
        print("ERROR: user_item_grid not found in artifacts.")
        return
        
    current_k = 100
    category_groups = {
        1: ["air_conditioning"],
        2: ["air_conditioning", "bed_bath_table"],
        3: ["music", "party_supplies", "toys"]
    }

    print(f"\n--- DYNAMIC AI REPORT (K={current_k}) ---")

    for group_id, user_selections in category_groups.items():
        print(f"\nGroup {group_id} Analysis:")
        print(f"Input Categories: {', '.join(user_selections)}")
        
        # Create profile for prediction (row of zeros across all categories)
        my_profile = pd.DataFrame(0, index=[0], columns=user_item_grid.columns)
        
        # Map user selections to the profile
        valid_selections = []
        for cat in user_selections:
            if cat in my_profile.columns:
                my_profile[cat] = 5  # Simulating a high-rating purchase
                valid_selections.append(cat)
            else:
                print(f"  Warning: '{cat}' not found in training grid.")

        if not valid_selections:
            print("  No valid categories found for this group. Skipping.")
            continue

        # Find the K-Nearest Neighbors using the trained model
        actual_k = min(current_k, len(user_item_grid))
        distances, indices = model_knn.kneighbors(my_profile, n_neighbors=actual_k)
        
        # Extract neighbor data to see their other purchases
        neighbor_indices = user_item_grid.iloc[indices[0]].index
        neighbor_data = user_item_grid.loc[neighbor_indices]
        
        # Calculate % of neighbors who bought other categories
        purchase_counts = (neighbor_data > 0).sum()
        probabilities = (purchase_counts / actual_k) * 100
        
        # Filter out input categories to avoid recommending what they already bought
        recommendations = probabilities.drop(labels=valid_selections, errors='ignore').sort_values(ascending=False).head(5)

        print("Top 5 Recommendations:")
        if recommendations.max() == 0:
            print("  No strong overlaps found for this specific group.")
        else:
            for category, prob in recommendations.items():
                if prob > 0:
                    print(f"  - {category:<30} | Confidence: {prob:>5.2f}%")

if __name__ == "__main__":
    main()
