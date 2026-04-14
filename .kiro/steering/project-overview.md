---
inclusion: always
---

# InfluxDB Downsampling Manager

## Purpose
Automated tool that creates and manages downsampling tasks for InfluxDB time-series databases. It detects measurements/fields in source buckets, creates downsampled copies at configurable intervals and retention periods, and generates Flux query tasks with intelligent offset scheduling.

## Architecture

### Components
- `manager/__main__.py` — Entry point (`python -m manager`). Loads config, parses credentials, and runs the manager.
- `manager/config.py` — Configuration loading (`load_config`), downsample config parsing (`build_bucket_configs`), and source bucket parsing (`parse_source_buckets`) with per-measurement field filtering.
- `manager/downsample_manager.py` — `DownsampleManager` class. Orchestrates bucket/task/label creation, cleanup, and InfluxDB API interactions.
- `manager/query_generator.py` — `BaseQueryGenerator` ABC with two concrete variants:
  - `SourceQueryGenerator` — reads from the raw source bucket; aggregates with mean/last.
  - `ChainedQueryGenerator` — reads from a pre-aggregated upstream bucket; applies mean-of-means for numeric fields. Safe to toggle on/off without data migration.
- `manager/model.py` — Data structures: `FieldData`, `LabelDef`, `DownsampleConfiguration`, `MeasurementConfig`, `SourceBucketConfig`, `Mapping` type alias.
- `manager/utils.py` — Helpers: deterministic hashing for offset spreading, timedelta-to-Flux-duration conversion, field filtering with fnmatch patterns.
- `manager/__init__.py` — Public API: exports `DownsampleManager`, `DownsampleConfiguration`, `MeasurementConfig`, `SourceBucketConfig`, `BaseQueryGenerator`, `SourceQueryGenerator`, `ChainedQueryGenerator`.

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
- `pyyaml` — YAML config file parsing
- `coloredlogs` — Colored logging
- `requests` — HTTP (transitive via influxdb-client)

## Running Locally
- Uses a Python venv at `venv/` in the project root
- Install runtime deps: `pip install -r requirements.txt`
- Install runtime + test deps: `pip install -r requirements-dev.txt`
- Run with: `python -m manager` (or `venv/Scripts/python -m manager` on Windows / `venv/bin/python -m manager` on Linux/macOS)
- Run tests: `pytest tests/ -v`

## Deployment
- Docker image published to `ghcr.io/xyaren/influx-downsample-manager` via the tag-based Release workflow (#[[file:.github/workflows/release.yml]])
- `python:3.14` base, installs deps, runs `python3 -m manager` with cron scheduling via entrypoint script
- Pull with: `docker pull ghcr.io/xyaren/influx-downsample-manager:latest`

## CI/CD
- `.github/workflows/ci.yml` — Lint (ruff), unit tests, integration tests, and Docker build on push/PR to main
- `.github/workflows/release.yml` — Tag-based (`v*`) Docker build and push to GHCR with semver tags

## GitHub Config
- `.github/ISSUE_TEMPLATE/bug_report.yml` — Structured bug report form
- `.github/ISSUE_TEMPLATE/feature_request.yml` — Feature request form
- `.github/dependabot.yml` — Weekly dependency updates for pip, Docker, and GitHub Actions
