---
inclusion: always
---

# InfluxDB Downsampling Manager

## Purpose
Automated tool that creates and manages downsampling tasks for InfluxDB time-series databases. It detects measurements/fields in source buckets, creates downsampled copies at configurable intervals and retention periods, and generates Flux query tasks with intelligent offset scheduling.

## Architecture

### Components
- `main.py` — Entry point. Configures source buckets, downsampling rules, credentials, and runs the manager.
- `manager/downsample_manager.py` — `DownsampleManager` class. Orchestrates bucket/task/label creation, cleanup, and InfluxDB API interactions.
- `manager/query_generator.py` — `BaseQueryGenerator` ABC with two concrete variants:
  - `SourceQueryGenerator` — reads from the raw source bucket; aggregates with mean/last.
  - `ChainedQueryGenerator` — reads from a pre-aggregated upstream bucket; applies mean-of-means for numeric fields. Safe to toggle on/off without data migration.
- `manager/model.py` — Data structures: `FieldData`, `LabelDef`, `DownsampleConfiguration`, `Mapping` type alias.
- `manager/utils.py` — Helpers: deterministic hashing for offset spreading, timedelta-to-Flux-duration conversion.
- `manager/__init__.py` — Public API: exports `DownsampleManager`, `DownsampleConfiguration`, `BaseQueryGenerator`, `SourceQueryGenerator`, `ChainedQueryGenerator`.

### Workflow
1. Connect to InfluxDB (org/token/url)
2. For each source bucket, query measurements and field types
3. Sort downsampling configs by interval (finest first)
4. Create target buckets with retention policies
5. For each tier, select `SourceQueryGenerator` or `ChainedQueryGenerator` based on the `chained` config flag
6. Generate and create/update downsampling tasks per measurement
7. Label all resources for tracking
8. Clean up orphaned tasks and labels

## Dependencies
- `influxdb-client` — InfluxDB Python SDK
- `pytimeparse` — Parse duration strings (e.g. "1d", "15m")
- `coloredlogs` — Colored logging
- `requests` — HTTP (transitive via influxdb-client)

## Running Locally
- Uses a Python venv at `venv/` in the project root
- Run with: `venv/Scripts/python main.py` (Windows) or `venv/bin/python main.py` (Linux/macOS)
- Install deps: `venv/Scripts/pip install -r requirements.txt`

## Deployment
- Docker: `python:3` base, installs deps, runs `python3 /app/main.py`
