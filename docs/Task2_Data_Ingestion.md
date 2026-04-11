# Task 2: Data Collection and Ingestion

## How to Run

```powershell
# Prequisite
# Virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies (one-time)
pip install pandas pyyaml requests dvc

# Git
git init
git add .
git add .dvcignore .gitignore
git commit -m "initial commit"

# DVC
dvc init
git add .dvc .dvcignore
git commit -m "initialize dvc"
dvc add data_lake dataset models mlruns

# Optional: Setup a local remote storage for dvc
dvc remote add -d local_storage /tmp/dvc-storage
dvc push
dvc pull
```

```bash
# Run both CSV + API ingestion
python run_ingestion.py

# Run only CSV ingestion
python run_ingestion.py --mode csv

# Run only API ingestion
python run_ingestion.py --mode api

```

## Input

| Source                 | What                                                                                                    | Location            |
| ---------------------- | ------------------------------------------------------------------------------------------------------- | ------------------- |
| CSV files (9)          | Customers, Products, Orders, Order Items, Reviews, Payments, Sellers, Geolocation, Category Translation | `dataset/` folder |
| REST API (2 endpoints) | Products + Categories from fakestoreapi.com                                                             | Internet (live API) |

## What It Does

1. **CSV Ingestion** (`ingestion/ingest_csv.py`)

   - Reads each CSV from `dataset/` using pandas
   - Validates row/column counts
   - Copies file to partitioned data lake: `data_lake/raw/<source>/csv/<date>/`
   - Computes MD5 checksum
   - Writes `.meta.json` sidecar (checksum, row count, columns, timestamp)
2. **API Ingestion** (`ingestion/ingest_api.py`)

   - Fetches each endpoint with **exponential-backoff retry** (3 attempts: 2s → 4s → 8s)
   - Saves raw JSON response
   - Converts JSON → CSV using pandas
   - Stores in partitioned data lake: `data_lake/raw/<source>/api/<date>/`
   - Computes MD5 checksum + writes `.meta.json`
3. **Error handling**: Each file/endpoint has its own try/catch — one failure won't stop the rest.

## Output

| Output            | Location                                                                     |
| ----------------- | ---------------------------------------------------------------------------- |
| Ingested CSV data | `data_lake/raw/<source>/csv/YYYY-MM-DD/`                                   |
| Ingested API data | `data_lake/raw/<source>/api/YYYY-MM-DD/`                                   |
| Metadata sidecars | `data_lake/raw/<source>/<type>/YYYY-MM-DD/*.meta.json`                     |
| CSV ingestion log | `logs/csv_ingestion.log`                                                   |
| API ingestion log | `logs/api_ingestion.log`                                                   |
| Run summary JSONs | `logs/csv_ingestion_summary_*.json`, `logs/api_ingestion_summary_*.json` |

## Files Involved

```
config/pipeline_config.yaml   ← Configuration (file list, API URLs, retry settings)
ingestion/utils.py             ← Shared utilities (logging, checksums, paths)
ingestion/ingest_csv.py        ← CSV batch ingestion script
ingestion/ingest_api.py        ← REST API ingestion script
run_ingestion.py               ← Unified runner (entry point)
```
