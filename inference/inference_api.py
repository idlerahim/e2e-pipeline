import os
import sys
import traceback
from datetime import datetime
import pandas as pd
import numpy as np
import mlflow
from flask import Flask, request, jsonify

# Ensure the root project path is available for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from models.predict import get_latest_run_id, load_training_data_mapping
from models.model_training import load_product_features

app = Flask(__name__)

# Global state for cached model and artifacts
class ModelCache:
    model = None
    user_item_grid = None
    df = None
    users = None
    items = None
    prod_cat_map = None
    run_id = None
    loaded = False
    top_global_categories = None
    global_probabilities = None

cache = ModelCache()

def init_model():
    """Load the model and datasets into memory on startup."""
    print("Initializing Inference API and loading model artifacts...")
    try:
        mlflow.set_experiment("RocoMart_Recommendation_Models")
        run_id = get_latest_run_id("RocoMart_Recommendation_Models", "KNN_Collaborative_Filtering")
        
        print(f"Loading KNN model from run {run_id}...")
        model_uri = f"runs:///{run_id}/model"
        cache.model = mlflow.sklearn.load_model(model_uri)
        
        client = mlflow.tracking.MlflowClient()
        local_dir = client.download_artifacts(run_id, "")
        
        grid_path = os.path.join(local_dir, "user_item_grid.csv")
        if os.path.exists(grid_path):
            cache.user_item_grid = pd.read_csv(grid_path, index_col=0)
        else:
            raise FileNotFoundError("user_item_grid.csv not found in MLflow artifacts")
            
        print("Loading training data and feature mappings...")
        df, _, _, users, items = load_training_data_mapping()
        cache.df = df
        cache.users = users
        cache.items = items
        
        # Build product category map
        products_df = load_product_features()
        if 'category_english' in products_df.columns:
            cat_col = 'category_english'
        elif 'product_category_name_english' in products_df.columns:
            cat_col = 'product_category_name_english'
        else:
            cat_col = 'product_category_name'
            
        cache.prod_cat_map = dict(zip(products_df['product_id'], products_df[cat_col]))
        
        # Compute global probabilities for popularity fallback (hybrid approach)
        purchase_counts = (cache.user_item_grid > 0).sum()
        cache.global_probabilities = (purchase_counts / len(cache.user_item_grid)) * 100
        cache.top_global_categories = purchase_counts.sort_values(ascending=False).index.tolist()
        
        cache.run_id = run_id
        cache.loaded = True
        print("Initialization complete! Ready to serve predictions.")
        
    except Exception as e:
        print(f"ERROR during initialization: {str(e)}")
        traceback.print_exc()
        cache.loaded = False

def get_recommendations_for_user(user_id, n_items=10, exclude_items=None):
    """Core recommendation logic for a single user."""
    if not cache.loaded:
        raise RuntimeError("Model is not loaded.")
        
    if user_id not in cache.user_item_grid.index:
        return [] # Cold start: return empty list or fallback to popular items
        
    if exclude_items is None:
        exclude_items = []
        
    user_profile = cache.user_item_grid.loc[[user_id]]
    user_past_items = cache.df[cache.df['customer_unique_id'] == user_id]['product_id'].values
    
    # Exclude both explicitly provided items and implicitly past purchases
    items_to_exclude = set(exclude_items).union(set(user_past_items))
    
    # KNN distances
    k = 100
    distances, indices = cache.model.kneighbors(user_profile, n_neighbors=min(k, len(cache.user_item_grid)))
    
    neighbor_indices = cache.user_item_grid.iloc[indices[0]].index
    neighbor_data = cache.user_item_grid.loc[neighbor_indices]
    
    purchase_counts = (neighbor_data > 0).sum()
    probabilities = (purchase_counts / len(neighbor_indices)) * 100
    
    user_bought_cats = user_profile.columns[(user_profile > 0).iloc[0]]
    recommendations = probabilities.drop(labels=user_bought_cats, errors='ignore').sort_values(ascending=False)
    
    top_categories = recommendations.head(n_items * 2).index.tolist()
    
    final_recs = []
    for cat in top_categories:
        # Find products in this category that user hasn't seen
        cat_products = [p for p in cache.items if cache.prod_cat_map.get(p) == cat and p not in items_to_exclude]
        
        cat_product_scores = []
        for p in cat_products:
            # Score = Global avg rating + category affinity probability weight
            avg_rating = cache.df[cache.df['product_id'] == p]['rating'].mean()
            cat_product_scores.append((p, avg_rating + probabilities[cat]/100))
        
        # Take top 2 products per category to ensure diversity
        cat_product_scores = sorted(cat_product_scores, key=lambda x: x[1], reverse=True)
        final_recs.extend(cat_product_scores[:2])
        
    final_recs = sorted(final_recs, key=lambda x: x[1], reverse=True)
    
    # Deduplicate
    unique_recs = []
    seen_products = set()
    for pid, score in final_recs:
        if pid not in seen_products:
            unique_recs.append({
                "item_id": pid, 
                "category": cache.prod_cat_map.get(pid, "unknown"),
                "score": round(score, 4), 
                "rank": len(unique_recs) + 1
            })
            seen_products.add(pid)
        if len(unique_recs) >= n_items:
            break
            
    return unique_recs

def get_recommendations_for_categories(user_selections, n_items=10, exclude_items=None):
    """Core recommendation logic for a list of input categories."""
    if not cache.loaded:
        raise RuntimeError("Model is not loaded.")
        
    if exclude_items is None:
        exclude_items = []
        
    my_profile = pd.DataFrame(0, index=[0], columns=cache.user_item_grid.columns)
    
    valid_selections = []
    for cat in user_selections:
        if cat in my_profile.columns:
            my_profile[cat] = 5
            valid_selections.append(cat)
            
    if not valid_selections:
        # Hybrid Model Fallback: Use popular categories if input is completely unknown
        print("No valid input categories found, falling back to Popular Categories Hybrid...")
        top_categories = cache.top_global_categories[:n_items * 2]
        probabilities = cache.global_probabilities
    else:
        # KNN distances
        k = 100
        distances, indices = cache.model.kneighbors(my_profile, n_neighbors=min(k, len(cache.user_item_grid)))
        
        neighbor_indices = cache.user_item_grid.iloc[indices[0]].index
        neighbor_data = cache.user_item_grid.loc[neighbor_indices]
        
        purchase_counts = (neighbor_data > 0).sum()
        probabilities = (purchase_counts / len(neighbor_indices)) * 100
        
        recommendations = probabilities.drop(labels=valid_selections, errors='ignore').sort_values(ascending=False)
        
        top_categories = recommendations.head(n_items * 2).index.tolist()
    
    final_recs = []
    for cat in top_categories:
        cat_products = [p for p in cache.items if cache.prod_cat_map.get(p) == cat and p not in exclude_items]
        
        cat_product_scores = []
        for p in cat_products:
            avg_rating = cache.df[cache.df['product_id'] == p]['rating'].mean()
            cat_product_scores.append((p, avg_rating + probabilities[cat]/100))
        
        cat_product_scores = sorted(cat_product_scores, key=lambda x: x[1], reverse=True)
        final_recs.extend(cat_product_scores[:2])
        
    final_recs = sorted(final_recs, key=lambda x: x[1], reverse=True)
    
    unique_recs = []
    seen_products = set()
    for pid, score in final_recs:
        if pid not in seen_products:
            unique_recs.append({
                "item_id": pid, 
                "category": cache.prod_cat_map.get(pid, "unknown"),
                "score": round(score, 4), 
                "rank": len(unique_recs) + 1
            })
            seen_products.add(pid)
        if len(unique_recs) >= n_items:
            break
            
    return unique_recs

@app.route('/', methods=['GET'])
def index():
    """Root endpoint to verify API is running and list available routes."""
    return jsonify({
        "message": "Welcome to the RecoMart Inference API",
        "status": "online",
        "available_endpoints": [
            "GET /health",
            "GET /model-info",
            "POST /recommend",
            "POST /recommend-batch",
            "POST /recommend-categories"
        ]
    }), 200

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    if cache.loaded:
        return jsonify({"status": "healthy", "model_loaded": True}), 200
    return jsonify({"status": "unhealthy", "model_loaded": False, "reason": "Failed to load model artifacts"}), 503

@app.route('/model-info', methods=['GET'])
def model_info():
    """Returns metadata about the active model."""
    if not cache.loaded:
        return jsonify({"error": "Model not loaded"}), 503
        
    return jsonify({
        "model_type": "Category User-User KNN",
        "mlflow_run_id": cache.run_id,
        "users_in_grid": len(cache.user_item_grid),
        "categories_in_grid": len(cache.user_item_grid.columns),
        "total_items_mapped": len(cache.items)
    }), 200

@app.route('/recommend', methods=['POST'])
def recommend():
    """Single-user recommendation endpoint."""
    if not cache.loaded:
        return jsonify({"error": "Model not loaded"}), 503
        
    data = request.get_json()
    if not data or 'user_id' not in data:
        return jsonify({"error": "Missing required field: 'user_id'"}), 400
        
    user_id = data.get('user_id')
    n_items = data.get('n_items', 10)
    exclude_items = data.get('exclude_items', [])
    
    try:
        recs = get_recommendations_for_user(user_id, n_items, exclude_items)
        return jsonify({
            "user_id": user_id,
            "recommendations": recs,
            "count": len(recs),
            "timestamp": datetime.now().isoformat()
        }), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/recommend-categories', methods=['GET', 'POST'])
def recommend_categories():
    """Category-based static recommendation endpoint."""
    if not cache.loaded:
        return jsonify({"error": "Model not loaded"}), 503
        
    if request.method == 'GET':
        categories = request.args.getlist('category')
        n_items = request.args.get('n_items', 10, type=int)
        exclude_items = request.args.getlist('exclude_items')
        if not categories:
            return jsonify({"error": "Missing required query parameter: 'category' (e.g., ?category=music)"}), 400
    else:
        data = request.get_json()
        if not data or 'categories' not in data:
            return jsonify({"error": "Missing required field: 'categories'"}), 400
        categories = data.get('categories')
        n_items = data.get('n_items', 10)
        exclude_items = data.get('exclude_items', [])
        
        if not isinstance(categories, list):
            return jsonify({"error": "'categories' must be a list of strings"}), 400
    
    try:
        recs = get_recommendations_for_categories(categories, n_items, exclude_items)
        return jsonify({
            "input_categories": categories,
            "recommendations": recs,
            "count": len(recs),
            "timestamp": datetime.now().isoformat()
        }), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/recommend-batch', methods=['POST'])
def recommend_batch():
    """Batch recommendation endpoint."""
    if not cache.loaded:
        return jsonify({"error": "Model not loaded"}), 503
        
    data = request.get_json()
    if not data or 'user_ids' not in data:
        return jsonify({"error": "Missing required field: 'user_ids'"}), 400
        
    user_ids = data.get('user_ids')
    n_items = data.get('n_items', 10)
    
    if not isinstance(user_ids, list):
        return jsonify({"error": "'user_ids' must be a list of strings"}), 400
        
    results = {}
    try:
        for uid in user_ids:
            results[uid] = get_recommendations_for_user(uid, n_items)
            
        return jsonify({
            "recommendations": results,
            "user_count": len(user_ids),
            "timestamp": datetime.now().isoformat()
        }), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="RecoMart Inference API")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host IP")
    parser.add_argument("--port", type=int, default=8000, help="Port to run the API on")
    args = parser.parse_args()
    
    init_model()
    app.run(host=args.host, port=args.port)
