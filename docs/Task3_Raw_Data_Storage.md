# Task 3: Raw Data Storage

## How to Run

```bash
# Generate catalog + stats
python -m storage.storage_manager

# Generate catalog + verify checksums
python -m storage.storage_manager --verify

# Generate catalog + print tree view
python -m storage.storage_manager --tree

# Both verify + tree
python -m storage.storage_manager --verify --tree
```

## Input

| Input         | What                     | Location                        |
| ------------- | ------------------------ | ------------------------------- |
| Raw data lake | Files ingested by Task 2 | `data_lake/raw/`              |
| Config        | Data lake path settings  | `config/pipeline_config.yaml` |

## What It Does

1. **Initializes data lake skeleton** - creates all 4 layers if they don't exist:

   - `data_lake/raw/` - unprocessed ingested data
   - `data_lake/staging/` - post-validation (used by Task 4)
   - `data_lake/curated/` - transformed features (used by Tasks 5–6)
   - `data_lake/serving/` - model-ready data (used by Task 7)
2. **Builds file catalog** - walks the raw layer, records every file with:

   - Source name, data type, partition date
   - File size (bytes + human-readable)
   - Linked metadata from `.meta.json` sidecars
3. **Computes storage statistics** - total files, sizes, sources, partitions
4. **Verifies checksums** (`--verify`) - recomputes MD5 for each data file and compares against the checksum stored in its `.meta.json`
5. **Prints tree view** (`--tree`) - visual directory tree of the entire data lake

## Output

| Output              | Location                                                     |
| ------------------- | ------------------------------------------------------------ |
| Data lake catalog   | `logs/data_lake_catalog_YYYYMMDD_HHMMSS.json`              |
| Storage statistics  | `logs/data_lake_stats_YYYYMMDD_HHMMSS.json`                |
| Storage manager log | `logs/storage_manager.log`                                 |
| Data lake skeleton  | `data_lake/raw/`, `staging/`, `curated/`, `serving/` |

## Data Lake Folder Structure

```
data_lake/
├── raw/                                    ← Populated by Task 2
│   ├── <source_name>/                      ← e.g., customers, products, orders
│   │   └── <type>/                         ← csv or api
│   │       └── <YYYY-MM-DD>/               ← date partition
│   │           ├── <data_file>.csv         ← actual data
│   │           └── <data_file>.csv.meta.json  ← metadata sidecar
│   └── ...
├── staging/                                ← Future: Task 4
├── curated/                                ← Future: Tasks 5–6
└── serving/                                ← Future: Task 7
```

## Files Involved

```
storage/storage_manager.py     ← Main storage management script (entry point)
config/pipeline_config.yaml    ← Paths configuration
ingestion/utils.py             ← Shared helpers (used by storage_manager)
```
