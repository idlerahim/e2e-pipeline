# Task 10: Pipeline Orchestration

## Overview

This task implements end-to-end data pipeline orchestration. A custom Python-based orchestrator handles data ingestion, validation, preparation, feature engineering, and model training with automatic retries and comprehensive logging.

## Manual Task Execution (.py)

Run individual pipeline stages manually:

```bash
python run_ingestion.py
python -m validation.validate_data
python -m preparation.prepare_data
python -m transformation.feature_engineering
python -m feature_store.feature_store_manager --register
python models/model_training.py
```

## Running Prefect Deployment

To run Prefect locally for development and testing, you can use the memory messaging broker. The memory broker doesn't require a database and works perfectly for local development.

### 1. Set the Messaging Broker and Start the Server

You can configure the messaging broker via environment variables or Prefect config:

```powershell
# Option A: Set via environment variable [IMPORTANT for Windows users]
$env:PREFECT_MESSAGING_BROKER="prefect.server.utilities.messaging.memory"
prefect server start

# Option B: Set via Prefect config
prefect config set PREFECT_MESSAGING_BROKER='prefect.server.utilities.messaging.memory'
prefect server start
```

### 2. Deploy the Pipeline Flow

In a new terminal (keeping the server running) and select **NO** when asked about **remote storage** & **schedule**:

```powershell
prefect work-pool create -t process "default-work-pool"
prefect deploy orchestration/pipeline_flow.py:rocomart_data_pipeline -n "RocoMart Pipeline" -p default-work-pool
```

Or use the [prefect.yaml](vscode-file://vscode-app/c:/Users/ar/AppData/Local/Programs/Microsoft%20VS%20Code/41dd792b5e/resources/app/out/vs/code/electron-browser/workbench/workbench.html) configuration file:

```powershell
prefect deploy --prefect-file prefect.yaml
```

### 3. Start the Worker

Once the server is running successfully (you'll see the dashboard URL), open a **new terminal** and start the worker using the default work-pool. You can configure the messaging broker via environment variables or Prefect config:

```powershell
python -m prefect worker start -p "default-work-pool"

# Option A: Set via environment variable [IMPORTANT for Windows users]
$env:PREFECT_MESSAGING_BROKER="prefect.server.utilities.messaging.memory"
$env:PYTHONIOENCODING="utf-8"
python -Xutf8 -m prefect worker start -p "default-work-pool"

# Option B: Set via Prefect config
prefect config set PREFECT_MESSAGING_BROKER='prefect.server.utilities.messaging.memory'
$env:PYTHONIOENCODING="utf-8"
python -Xutf8 -m prefect worker start -p "default-work-pool"
```

### 4. Access the UI

Open the Prefect UI at **http://127.0.0.1:4200** -> Flow Runs to trigger and monitor your pipeline! 🚀

## Files Involved

```
orchestration/pipeline_flow.py         ← Main orchestrator class and execution logic
prefect.yaml                           ← Prefect deployment configuration
run_ingestion.py                       ← Data ingestion entry point
validation/validate_data.py            ← Data quality validation
preparation/prepare_data.py            ← Data cleaning and preparation
transformation/feature_engineering.py  ← Feature extraction and transformation
feature_store/feature_store_manager.py ← Feature registration and serving
models/model_training.py               ← Model training script
config/pipeline_config.yaml            ← Pipeline configuration
logs/pipeline_orchestration.log        ← Orchestration execution logs
reports/pipeline_execution_*.md        ← Timestamped execution reports
```
