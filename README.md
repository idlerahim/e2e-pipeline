# RecoMart End-to-End Recommendation Pipeline

RecoMart is an end-to-end e-commerce product recommendation pipeline. This project demonstrates a complete machine learning lifecycle—from raw data ingestion and validation to feature engineering, model training, and real-time REST API serving.

## Overview

The system processes 9 interconnected datasets (customers, products, orders, reviews, etc.) to build a **Memory-based Category User-User K-Nearest Neighbors (KNN)** collaborative filtering model. The model identifies users with similar categorical purchase affinities to provide highly personalized, diverse product recommendations.

## Architecture

The pipeline consists of the following automated stages:
1. **Data Ingestion (`script_2_download_extract_data.ps1`)**: Downloads the Brazilian e-commerce dataset from Kaggle and extracts it.
2. **Raw Data Storage (`storage/`)**: Manages the data lake architecture for raw incoming datasets.
3. **Data Validation (`validation/`)**: Performs automated quality checks (schema, missing values, duplicates, bounds).
4. **Data Preparation & EDA (`preparation/`)**: Cleans data, merges transactions, and generates exploratory data analysis plots.
5. **Feature Engineering (`transformation/`)**: Creates user, item, and interaction features, applying necessary log transforms and Min-Max normalizations.
6. **Feature Store (`feature_store/`)**: A custom lightweight feature store with snapshot versioning and point-in-time correctness for generating ML training matrices.
7. **Model Training (`models/`)**: Trains the KNN model and tracks ranking metrics (Precision@K, Recall@K, NDCG@K) and artifacts using **MLflow**.
8. **Inference API (`inference/`)**: A Flask-based REST API that dynamically loads the latest model and artifacts from MLflow to serve real-time recommendations.

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Full Pipeline
You can run the entire pipeline end-to-end using the provided PowerShell script:
```powershell
./script_3_run_pipeline.ps1
```

### 3. View MLflow Dashboard
View model training metrics and artifacts:
```bash
mlflow ui
# Navigate to http://localhost:5000
```

### 4. Start the Inference API
Serve real-time recommendations via the REST API:
```bash
python -m inference.inference_api --port 8000 --host 0.0.0.0
```

#### Example API Request (Category Affinity)
```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8000/recommend-categories" -Method Post -ContentType "application/json" -Body '{"categories": ["bed_bath_table", "furniture_decor"], "n_items": 5}' | ConvertTo-Json -Depth 5
```
*(You can also visit `http://127.0.0.1:8000/recommend-categories?category=bed_bath_table&n_items=5` directly in your browser!)*

## Documentation

For a detailed breakdown of each stage, please refer to the markdown reports in the `docs/` folder:
- [Task 1: Problem Formulation](docs/Task1_Problem_Formulation.md)
- [Task 3: Raw Data Storage](docs/Task3_Raw_Data_Storage.md)
- [Task 4: Data Validation](docs/Task4_Data_Validation.md)
- [Task 5: Data Preparation](docs/Task5_Data_Preparation.md)
- [Task 6: Feature Engineering](docs/Task6_Feature_Engineering.md)
- [Task 7: Feature Store](docs/Task7_Feature_Store.md)
- [Task 9: Model Training & Prediction](docs/Task9_Model_Training_Prediction.md)

---
*Built for RecoMart e-commerce personalization.*
