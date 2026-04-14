# InfluxDB Downsampling Manager

## Project Overview

Automated tool that creates and manages downsampling tasks for InfluxDB time-series databases. It detects measurements/fields in source buckets, creates downsampled copies at configurable intervals and retention periods, and generates Flux query tasks with intelligent offset scheduling.

## Architecture

### Components
- `manager/__main__.py` — Entry point (`python -m manager`). Loads config, parses credentials, and runs the manager.
- `manager/config.py` — Configuration loading (`load_config`), downsample config parsing (`build_bucket_configs`), and source bucket parsing (`parse_source_buckets`) with per-measurement field filtering.
- `manager/downsample_manager.py` — `DownsampleManager` class. Orchestrates bucket/task/label creation, cleanup, and InfluxDB API interactions.
- `manager/query_generator.py` — `BaseQueryGenerator` ABC with two concrete variants:
  - `SourceQueryGenerator` — reads from the raw source bucket; aggregates with mean/last.
  - `ChainedQueryGenerator` — reads from a pre-aggregated upstream bucket; applies mean-of-means for numeric fields.
- `manager/model.py` — Data structures: `FieldData`, `LabelDef`, `DownsampleConfiguration`, `MeasurementConfig`, `SourceBucketConfig`, `Mapping` type alias.
- `manager/utils.py` — Helpers: deterministic hashing for offset spreading, timedelta-to-Flux-duration conversion, field filtering with fnmatch patterns.
- `manager/__init__.py` — Public API exports.

### Workflow
1. Connect to InfluxDB (org/token/url)
2. For each source bucket, query measurements and field types
3. Sort downsampling configs by interval (finest first)
4. Create target buckets with retention policies
5. For each tier, select `SourceQueryGenerator` or `ChainedQueryGenerator` based on the `chained` config flag
6. Generate and create/update downsampling tasks per measurement
7. Label all resources for tracking
8. Clean up orphaned tasks and labels

## Build & Run

- Python 3.14+
- Uses a Python venv at `venv/` in the project root
- Install runtime deps: `pip install -r requirements.txt`
- Install runtime + test deps: `pip install -r requirements-dev.txt`
- Run: `python -m manager` (or `venv/Scripts/python -m manager` on Windows / `venv/bin/python -m manager` on Linux/macOS)
- Run tests: `pytest tests/ -v`
- Lint check: `ruff check .`
- Format check: `ruff format --check .`
- Auto-fix: `ruff check --fix .` and `ruff format .`

## Dependencies

- `requirements.txt` — runtime deps only (installed in Docker image)
- `requirements-dev.txt` — includes runtime deps via `-r requirements.txt` plus test deps (pytest, ruff, testcontainers)
- When adding a new dependency, pin it to a specific version (e.g. `package==1.2.3`)
- Put test-only packages in `requirements-dev.txt`, not `requirements.txt`

## Code Conventions

### Type Hints
- Use modern Python type annotations everywhere (no `typing.Dict`, `typing.List` — use built-in `dict`, `list`, `set`)
- Use `TypedDict` with `NotRequired` for config objects with optional fields
- Use `@dataclass` for immutable value objects

### Naming
- Private instance attributes prefixed with `_` (e.g. `self._client`)
- Module-level logger: `logger = logging.getLogger(__name__)`
- Task prefix convention: `"gen_"`

### Patterns
- Idempotent operations: `create_or_get_*`, `create_or_update_*` — safe to run repeatedly
- Label-based resource tracking with `"creator": "influx-downsample-manager"` metadata
- Deterministic hashing (SHA-256) for predictable task offset spreading
- Separation of concerns: query generation, InfluxDB operations, and utilities in separate modules

### Flux Queries
- Use `json.dumps()` for field lists in generated Flux
- Numeric fields aggregated with `mean`, non-numeric with `last`
- Task offsets spread via `hash_to_integer` to avoid thundering herd

### Error Handling
- Raise `Exception` with descriptive messages for parse failures and ownership conflicts
- Log operations at INFO level (created, updated, deleted resources)

## Testing

- Framework: **pytest**
- Tests live in `tests/` and are discovered automatically
- Use plain `assert` statements, not `self.assertEqual` / `self.assertTrue`
- Use `@pytest.fixture` for shared setup (e.g. mocked `DownsampleManager`)
- Use `@pytest.mark.parametrize` for data-driven tests instead of repeating test methods
- Use plain classes (no `unittest.TestCase` inheritance) to group related tests
- Mock InfluxDB client interactions with `unittest.mock.MagicMock` / `patch`

## Commit Conventions

This project uses [Conventional Commits](https://www.conventionalcommits.org/).

### Format
```
<type>(<scope>): <short summary>
```

### Types
| Type | Purpose |
|---|---|
| `feat` | New feature or capability |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `test` | Adding or updating tests |
| `ci` | CI/CD pipeline changes |
| `chore` | Maintenance, deps, tooling |
| `perf` | Performance improvement |

### Scopes (optional)
`manager`, `query`, `config`, `model`, `docker`, `ci`

### Breaking Changes
Append `!` after the type/scope or add a `BREAKING CHANGE:` footer.

### AI Co-authoring
Always include the following trailer on AI-assisted commits:
```
Co-authored-by: Kiro <kiro@amazon.com>
```

## README Maintenance

When making changes that affect user-facing behavior, keep `README.md` in sync:
- Adding/removing/renaming a module under `manager/` → update the **Project structure** tree
- Changing CLI usage, entry points, or environment variables → update **Usage** and **Environment variables** sections
- Changing the Dockerfile or Docker Compose setup → update **Docker** installation and usage examples
- Adding/removing Python dependencies → verify the **Requirements** section still matches
- Changing config schema → update **Config file structure**, **Downsample config fields** table, and the inline YAML example
- Changing the minimum Python version → update the **Requirements** section

Do not add sections or badges to the README unless explicitly asked. Keep it concise.

## Deployment

- Docker image published to `ghcr.io/xyaren/influx-downsample-manager` via tag-based Release workflow
- `python:3.14` base, installs deps, runs `python3 -m manager` with cron scheduling via entrypoint script

## Release Process

Releases are tag-driven. Pushing a semver tag (`v*`) triggers the Release workflow which builds and publishes a Docker image to GHCR.

1. Ensure `main` is green (CI passes)
2. `git tag -a v<MAJOR>.<MINOR>.<PATCH> -m "Release v<MAJOR>.<MINOR>.<PATCH>"`
3. `git push origin main --tags`

## CI/CD

- `.github/workflows/ci.yml` — Lint (ruff), unit tests, integration tests, and Docker build on push/PR to main
- `.github/workflows/release.yml` — Tag-based (`v*`) Docker build and push to GHCR with semver tags

## Important Boundaries

- Never commit `config.yaml` with real credentials
- Do not modify files in `venv/` or `__pycache__/`
