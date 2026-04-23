# Task 9: Model Training and Evaluation

## How to Run

### Prerequisites

Ensure training data is available from Task 8:

```bash
python -m feature_store.feature_store_manager --training-set
```

### Train Models

Run the training pipeline:

```bash
python -m models.model_training
```

This will:

- Load the latest training set from `data_lake/serving/training_sets/`
- Train the Category User-Based KNN model
- Log metrics and artifacts to MLflow
- Generate performance report at `reports/model_performance_report.md`

### View Training Results with MLflow

#### Start MLflow UI (in a new terminal):

```bash
mlflow ui
```

Then navigate to `http://localhost:5000` in your browser.

#### View Specific Experiment:

```bash
mlflow experiments list
mlflow run -n RocoMart_Recommendation_Models
```

#### Query Metrics from Command Line:

```bash
mlflow experiments stat --experiment-name RocoMart_Recommendation_Models
```

---

## Overview

This task implements model training and evaluation for the RocoMart recommendation system. We train recommendation models using collaborative filtering techniques, evaluate their performance, and track metadata using MLflow.

## Strategy

**Recommendation Strategy:**

- Focus on collaborative filtering as the primary approach, since it leverages user-item interaction patterns effectively for e-commerce recommendations.
- Use scikit-learn for memory-based Category User-User collaborative filtering (KNN).
- Evaluate models using standard ranking metrics such as Precision@K, Recall@K, and NDCG@K.
- Track all experiments with MLflow to maintain reproducibility and compare model versions.

**Data Sources:**

- Training data: User-item interaction matrix from `data_lake/serving/training_sets/`
- Features: Product metadata from `data_lake/curated/features/`

**Evaluation Approach:**

- Train-validation split with a held-out test set for evaluation
- Compare models using ranking metrics (Precision@K, Recall@K, NDCG@K)

## Target Models

We target one primary recommendation model:

1. **KNN (K-Nearest Neighbors)** - Memory-based Category User-User collaborative filtering

   - Strengths: Interpretable, leverages user affinity to specific product categories, handles zero-shot recommendations well across item spaces.
   - Use case: Category-level personalized recommendations based on neighbor interactions.

## Implementation

### Training Script

The training script `models/model_training.py` implements:

- Data loading from the feature store (`data_lake/serving/training_sets/`)
- Model training using scikit-learn's NearestNeighbors
- Evaluation using Top-K ranking
- MLflow tracking for parameters, metrics, and artifacts

### Model Performance Report

See `reports/model_performance_report.md` for detailed evaluation results including ranking metrics such as Precision@K, Recall@K, and NDCG@K.

### MLflow Tracking

Models are tracked with:

- **Experiment**: `RocoMart_Recommendation_Models`
- **Run IDs**: `KNN_Collaborative_Filtering`
- **Parameters**: Model hyperparameters (e.g., `k_value=10`, `min_categories=2`)
- **Metrics**:
  - Ranking metrics: Precision_5, Recall_5, NDCG_5
- **Artifacts**: Serialized model (`model/`) and the user_item_grid (`user_item_grid.csv`)

#### View Experiment Metrics

- **Launch MLflow UI**: `mlflow ui` → Navigate to http://localhost:5000
- **Navigate to Experiment**: Click "RocoMart_Recommendation_Models" in the UI
- **Compare Runs**: Select multiple runs to compare side-by-side (Precision, NDCG, etc.)
- **View Run Details**: Click individual run to see parameters, metrics, and logged artifacts

#### MLflow Artifact Storage

All artifacts are stored in `mlruns/` directory:

```
mlruns/
  └── [experiment_id]/
      ├── [run_id]/
      │   ├── artifacts/
      │   │   ├── model/
      │   │   └── user_item_grid.csv
      │   ├── metrics/
      │   │   ├── precision_5
      │   │   ├── recall_5
      │   │   ├── ndcg_5
      │   │   └── ...
      │   └── params/
```

## Deliverables

- `models/model_training.py` - Training and evaluation script (entry point)
- `models/evaluation_metrics.py` - Ranking metrics implementation
- `reports/model_performance_report.md` - Performance report with metrics and recommendations
- MLflow experiment runs at `mlruns/` with tracked metadata (run IDs, parameters, metrics, artifacts)
- Trained models serialized and logged to MLflow for reproducibility

## Expected Output

After running `python -m models.model_training`, you should see:

```
Starting RocoMart Model Training Pipeline
Loading training data from: data_lake/serving/training_sets/training_set_YYYYMMDD_HHMMSS.csv
Training KNN model (User-Based on Categories)...
Loading product features from fallback dataset: dataset/olist_products_dataset.csv
Users with 2+ categories: 728
KNN (Category User-Based) - Precision@5: 0.2256, Recall@5: 0.9487, NDCG@5: 0.8100
Performance report saved to: reports/model_performance_report.md
Training pipeline completed successfully!
```

## MLflow Workflow Example

### Step 1: Run Training

```bash
# Install packages
pip install scikit-learn mlflow seaborn

# Start training
python -m models.model_training
```

### Step 2: Start MLflow UI (new terminal)

```bash
mlflow ui
```

### Step 3: View Experiments in Browser

```
Open http://localhost:5000/
├── Click "RocoMart_Recommendation_Models" experiment
├── View the latest KNN_Collaborative_Filtering run
├── Compare metrics across historical runs
└── Download model artifacts (model, user_item_grid.csv) for deployment
```

### Step 4: Check Performance Report

```bash
cat reports/model_performance_report.md
```

## How to Make Predictions

### Quick Start (Easiest Approach) 🚀

After training models, get static category recommendations using the testing script:

```bash
# Get Dynamic AI Report for Category Groups
python models/predict_static.py
```

**Expected Output:**

```
--- DYNAMIC AI REPORT (K=100) ---

Group 1 Analysis:
Input Categories: air_conditioning
Top 5 Recommendations:
  - furniture_decor                | Confidence: 25.00%
  - bed_bath_table                 | Confidence: 21.00%
  - computers_accessories          | Confidence: 16.00%
  - baby                           | Confidence: 13.00%
  - garden_tools                   | Confidence: 13.00%
```

## Files Involved

```
models/model_training.py         ← Main training script (entry point)
models/predict.py                ← Prediction script for making recommendations
models/evaluation_metrics.py     ← Ranking metrics (precision, recall, NDCG, MAP)
feature_store/feature_store_manager.py  ← Load user/item features
config/pipeline_config.yaml      ← Path configuration
ingestion/utils.py               ← Shared utilities
reports/model_performance_report.md ← Auto-generated performance report
```
