<p align="center">
  <img src="docs/supporting_files/RecoMart-Banner.png" alt="RecoMart Banner" width="100%">
</p>

# RecoMart End-to-End Recommendation Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9+-blue.svg" alt="Python Version">
  <img src="https://img.shields.io/badge/ML-KNN%20Collaborative%20Filtering-brightgreen.svg" alt="Model Type">
  <img src="https://img.shields.io/badge/Tracking-MLflow-orange.svg" alt="MLflow">
  <img src="https://img.shields.io/badge/Framework-Flask-lightgrey.svg" alt="Flask">
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License">
</p>

RecoMart is a production-grade e-commerce recommendation engine built on the Brazilian E-Commerce Public Dataset. It automates the full ML lifecycle—from data ingestion to real-time inference.

## 🏗️ Pipeline Architecture

```mermaid
graph LR
    %% Custom Styling Definitions
    classDef greenStore fill:#10B981,stroke:#065F46,stroke-width:2px,color:#fff;
    classDef orangeProcess fill:#F59E0B,stroke:#92400E,stroke-width:2px,color:#fff;
    classDef purpleTask fill:#8B5CF6,stroke:#5B21B6,stroke-width:2px,color:#fff;
    classDef blueOutput fill:#3B82F6,stroke:#1E40AF,stroke-width:2px,color:#fff;
    classDef whiteAlert fill:#FFFFFF,stroke:#000000,stroke-width:2px,color:#000;

    %% Data Sources & Ingestion
    subgraph Data_Ingestion [ ]
        style Data_Ingestion fill:none,stroke:none;
        API[(API)]:::greenStore
        CSV[(CSV)]:::greenStore
        Ingestion[Ingestion]:::orangeProcess
    end

    %% Core Data Flow
    DataLake[(Data Lake)]:::greenStore
    Validation{Validation}:::orangeProcess
    Preparation[Preparation]:::orangeProcess
    Alert[Alert]:::whiteAlert

    Feature[Feature]:::orangeProcess
    FeatureStore[(Feature Store)]:::greenStore
    Training[Training]:::purpleTask

    MLflow[(MLflow)]:::greenStore
    Evaluation[Evaluation]:::purpleTask

    Flask[Flask]:::blueOutput

    %% Output Deliverables
    subgraph Outputs [ ]
        style Outputs fill:none,stroke:none;
        Batch([Batch Recommendations]):::blueOutput
        Live([Live Inferences]):::blueOutput
        Rank([Rank Based for Unseen Data]):::blueOutput
    end

    %% Connections
    API --> Ingestion
    CSV --> Ingestion
    Ingestion --> DataLake
    DataLake --> Validation

    %% Validation Paths
    Validation -- Pass --> Preparation
    Validation -- Fail --> Alert
    Preparation --> Feature
    Feature --> FeatureStore

    %% Feature Store & Training
    FeatureStore --> Training
    FeatureStore --> MLflow
    MLflow --> Evaluation

    %% Deployment & Serving
    MLflow --> Flask
    Flask --> Batch
    Flask --> Live
    Flask --> Rank
```
_Pipeline Architecture_

The system identifies users with similar categorical purchase affinities using a **Memory-based User-User KNN** model.

1.  **Ingestion & Validation**: Automated data retrieval and schema/quality checks.
2.  **Feature Store**: Custom versioned store for point-in-time interaction features.
3.  **MLflow Tracking**: Model versioning, hyperparameter tuning, and metric logging.
4.  **Inference API**: High-performance Flask REST API for real-time recommendations.

## 🚀 Quick Start

### 1. Setup
```bash
pip install -r requirements.txt
./script_3_run_pipeline.ps1  # Runs full E2E pipeline
```

### 2. Monitoring & Serving
*   **MLflow Dashboard:** Run `mlflow ui` and visit `http://localhost:5000`
*   **Start API:** `python -m inference.inference_api --port 8000`

### 3. Example Request
```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8000/recommend-categories" -Method Post -ContentType "application/json" -Body '{"categories": ["bed_bath_table"], "n_items": 5}'
```

## 📖 Documentation
Detailed technical reports for each stage:
*   [Phase 1: Problem & Ingestion](docs/Task1_Problem_Formulation.md)
*   [Phase 2: Validation & Prep](docs/Task4_Data_Validation.md)
*   [Phase 3: Feature Engineering & Store](docs/Task7_Feature_Store.md)
*   [Phase 4: Model Training (MLflow)](docs/Task9_Model_Training_Prediction.md)

---
*Built for RecoMart e-commerce personalization.*

