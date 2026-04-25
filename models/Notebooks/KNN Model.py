import pandas as pd
import numpy as np
import mlflow
from sklearn.model_selection import train_test_split
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics import mean_squared_error
from sklearn.metrics import ndcg_score

# --- STEP 1: SETTINGS & LOADING ---
pd.set_option('display.max_columns', None)
data_path = "dataset/"

reviews = pd.read_csv(data_path + 'olist_order_reviews_dataset.csv')
orders = pd.read_csv(data_path + 'olist_orders_dataset.csv')
items = pd.read_csv(data_path + 'olist_order_items_dataset.csv')
customers = pd.read_csv(data_path + 'olist_customers_dataset.csv')  
products = pd.read_csv(data_path + 'olist_products_dataset.csv')   
translation = pd.read_csv(data_path + 'product_category_name_translation.csv')

# --- STEP 2: MASTER MERGE & MULTI-CATEGORY FILTERING ---
merged_all = pd.merge(reviews, orders, on='order_id')
merged_all = pd.merge(merged_all, items, on='order_id')
merged_all = pd.merge(merged_all, customers, on='customer_id')
merged_all = pd.merge(merged_all, products, on='product_id')
merged_all = pd.merge(merged_all, translation, on='product_category_name')

# Convert long IDs to simple user numbers
merged_all['user_number'] = pd.factorize(merged_all['customer_unique_id'])[0] + 1
# print(merged_all.head())
merged_all.to_csv('dataset/merged_all.csv', index=False)
# final_table = merged_all[['user_number', 'product_id', 'product_category_name_english', 'review_score']]
final_table = merged_all[[
                'user_number', 
                'customer_unique_id',
                'product_id', 
                'product_category_name_english', 
                'review_score',
                'price',
                'product_photos_qty'
            ]]
final_table = final_table.drop_duplicates().dropna()

# CRITICAL FILTER: Keep only users with 2 or more distinct categories
user_category_counts = final_table.groupby('user_number')['product_category_name_english'].nunique()
multi_category_users = user_category_counts[user_category_counts >= 2].index
final_table = final_table[final_table['user_number'].isin(multi_category_users)]

print(f"Data Prepared. Users with 2+ categories: {final_table['user_number'].nunique()}")

# print(final_table)

# --- STEP 3: MLFLOW & DATA SPLIT ---
if mlflow.active_run():
    mlflow.end_run()

mlflow.set_experiment("Olist-KNN-Recommender")
run = mlflow.start_run(run_name="KNN_Optimized_K10")

train_data, test_data = train_test_split(final_table, test_size=0.2, random_state=42)


# --- STEP 4: TRAINING (THE GRID & KNN) ---
# TWEAK: Using a tighter neighborhood (k=10) to improve Precision
K_NEIGHBORS = 10 

user_item_grid = train_data.pivot_table(index='user_number', 
                                        columns='product_category_name_english', 
                                        values='review_score').fillna(0)

model_knn = NearestNeighbors(metric='cosine', algorithm='brute')
model_knn.fit(user_item_grid)

mlflow.log_param("k_value", K_NEIGHBORS)
mlflow.log_param("min_categories", 2)
print("Model trained on multi-category users with k=10.")

def get_comprehensive_metrics(model, grid, test_data, k=5, threshold=4):
    test_users = test_data['user_number'].unique()[:100] 
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
            user_test_data = test_data[test_data['user_number'] == user]
            actual_liked = user_test_data[user_test_data['review_score'] >= threshold]['product_category_name_english'].tolist()
            
            if not actual_liked: continue

            # 3. Calculate Precision & Recall
            hits = len(set(top_k_items) & set(actual_liked))
            precisions.append(hits / k)
            recalls.append(hits / len(actual_liked))
            
            # 4. Calculate NDCG
            # Create a true relevance array based on test data
            true_relevance = np.zeros(len(grid.columns))
            for _, row in user_test_data.iterrows():
                idx = grid.columns.get_loc(row['product_category_name_english'])
                true_relevance[idx] = row['review_score']
            
            # Use neighbor average scores as predicted relevance
            pred_relevance = recommendation_scores.values
            ndcgs.append(ndcg_score([true_relevance], [pred_relevance], k=k))
            
        except: continue

    return np.mean(precisions), np.mean(recalls), np.mean(ndcgs)

# Run and Log
avg_p, avg_r, avg_n = get_comprehensive_metrics(model_knn, user_item_grid, test_data, k=5)
print(f"Precision@5: {avg_p:.2%}, Recall@5: {avg_r:.2%}, NDCG@5: {avg_n:.4f}")
mlflow.log_metric("NDCG_at_5", avg_n)


# --- STEP 5: PREDICT (FOR SPECIFIC CATEGORY GROUPS) ---

# 1. Ensure K_NEIGHBORS is defined (Matches the 'K' used in Step 4)
K_NEIGHBORS = 100 
current_k = K_NEIGHBORS
try:
    current_k = K_NEIGHBORS
except NameError:
    current_k = 100  # Defaulting to 10 for better precision as discussed

# 2. Define the groups to test
category_groups = {
    1: ["air_conditioning"],
    2: ["air_conditioning", "bed_bath_table"],
    3: ["music", "party_supplies", "toys"]
}

print(f"--- DYNAMIC AI REPORT (K={current_k}) ---")

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

    # 3. Find the K-Nearest Neighbors using the trained model
    distances, indices = model_knn.kneighbors(my_profile, n_neighbors=current_k)
    
    # 4. Extract neighbor data to see their other purchases
    neighbor_indices = user_item_grid.iloc[indices[0]].index
    neighbor_data = user_item_grid.loc[neighbor_indices]
    
    # Calculate % of neighbors who bought other categories
    purchase_counts = (neighbor_data > 0).sum()
    probabilities = (purchase_counts / current_k) * 100
    
    # 5. Filter out input categories to avoid recommending what they already bought
    recommendations = probabilities.drop(labels=valid_selections, errors='ignore').sort_values(ascending=False).head(5)

    print("Top 5 Recommendations:")
    if recommendations.max() == 0:
        print("  No strong overlaps found for this specific group.")
    else:
        for category, prob in recommendations.items():
            if prob > 0:
                print(f"  - {category:<30} | Confidence: {prob:>5.2f}%")

# Close the MLflow run context
if mlflow.active_run():
    mlflow.end_run()
