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
- Train all three models (Matrix Factorization, KNN, Content-Based)
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
- Use scikit-learn for collaborative filtering implementations: NMF for matrix factorization and KNN for item similarity.
- Evaluate models using standard metrics: RMSE (Root Mean Square Error), MAE (Mean Absolute Error), and ranking metrics such as Precision@K, Recall@K, and NDCG@K.
- Track all experiments with MLflow to maintain reproducibility and compare model versions.

**Data Sources:**

- Training data: User-item interaction matrix from `data_lake/serving/training_sets/`
- Features: Product metadata from `data_lake/curated/features/`

**Evaluation Approach:**

- Train-validation split (80/20) with a held-out test set for evaluation
- Compare models using rating prediction metrics (RMSE, MAE) and ranking metrics (Precision@K, Recall@K, NDCG@K)

## Target Models

We target three recommendation models:

1. **Matrix Factorization (NMF)** - Non-negative Matrix Factorization collaborative filtering

   - Strengths: Handles sparse interactions, captures latent factors, meaningful non-negative embeddings
   - Use case: General user-item recommendations
2. **KNN (K-Nearest Neighbors)** - Item-based memory-based collaborative filtering

   - Strengths: Interpretable, leverages item similarity, easy to extend
   - Use case: Item similarity recommendations
3. **Content-Based Filtering** - Using product features

   - Strengths: Works with new users, uses item metadata
   - Use case: Recommendations based on product descriptions/categories

## Implementation

### Training Script

The training script `models/model_training.py` implements:

- Data loading from the feature store (`data_lake/serving/training_sets/`)
- Model training for all three targets using scikit-learn for collaborative filtering and content-based scoring
- Evaluation with train/test split
- MLflow tracking for parameters, metrics, and artifacts

### Model Performance Report

See `reports/model_performance_report.md` for detailed evaluation results including RMSE, MAE, and ranking metrics such as Precision@K, Recall@K, NDCG@K, and MAP@K.

### MLflow Tracking

Models are tracked with:

- **Experiment**: `RocoMart_Recommendation_Models`
- **Run IDs**: Each model gets a unique run (Matrix Factorization, KNN, Content-Based)
- **Parameters**: Model hyperparameters (e.g., `n_components=20` for NMF, `k=40` for KNN, `max_features=1000` for TF-IDF)
- **Metrics**:
  - Rating prediction: RMSE, MAE
  - Ranking metrics: Precision@5, Precision@10, Recall@5, Recall@10, NDCG@5, NDCG@10, MAP@5, MAP@10
- **Artifacts**: Serialized models, vectorizers, similarity matrices

#### View Experiment Metrics

- **Launch MLflow UI**: `mlflow ui` → Navigate to http://localhost:5000
- **Navigate to Experiment**: Click "RocoMart_Recommendation_Models" in the UI
- **Compare Runs**: Select multiple runs to compare side-by-side (RMSE, Precision@10, etc.)
- **View Run Details**: Click individual run to see parameters, metrics, and logged artifacts

#### MLflow Artifact Storage

All artifacts are stored in `mlruns/` directory:

```
mlruns/
  └── [experiment_id]/
      ├── [run_id]/
      │   ├── artifacts/
      │   │   ├── model/
      │   │   └── cosine_sim.npy  (for Content-Based model)
      │   ├── metrics/
      │   │   ├── rmse
      │   │   ├── mae
      │   │   ├── precision_10
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
Training Matrix Factorization model...
Matrix Factorization - RMSE: 1.2345, MAE: 0.9876
Matrix Factorization - Precision@10: 0.5500, Recall@10: 0.6200, NDCG@10: 0.7100
Training KNN model...
KNN - RMSE: 1.1234, MAE: 0.8765
KNN - Precision@10: 0.5800, Recall@10: 0.6400, NDCG@10: 0.7300
Training Content-Based model...
Content-Based - accuracy: 0.7500
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
├── View 3 runs: Matrix Factorization, KNN, Content-Based
├── Compare metrics across runs
└── Download model artifacts for deployment
```

### Step 4: Check Performance Report

```bash
cat reports/model_performance_report.md
```

## Files Involved

```
models/model_training.py         ← Main training script (entry point)
models/evaluation_metrics.py     ← Ranking metrics (precision, recall, NDCG, MAP)
config/pipeline_config.yaml      ← Path configuration
ingestion/utils.py               ← Shared utilities
reports/model_performance_report.md ← Auto-generated performance report
```
