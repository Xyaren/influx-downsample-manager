---
inclusion: always
---

# Coding Standards

## Python Version
- Python 3.14+ (uses `X | Y` union syntax, `dict[str, ...]` built-in generics, `dataclass`, `TypedDict` with `NotRequired`)

## Type Hints
- Use modern Python type annotations everywhere (no `typing.Dict`, `typing.List` — use built-in `dict`, `list`, `set`)
- Use `TypedDict` with `NotRequired` for config objects with optional fields
- Use `@dataclass` for immutable value objects

## Naming
- Private instance attributes prefixed with `_` (e.g. `self._client`)
- Module-level logger: `logger = logging.getLogger(__name__)`
- Task prefix convention: `"gen_"`

## Patterns
- Idempotent operations: `create_or_get_*`, `create_or_update_*` — safe to run repeatedly
- Label-based resource tracking with `"creator": "influx-downsample-manager"` metadata
- Deterministic hashing (SHA-256) for predictable task offset spreading
- Separation of concerns: query generation, InfluxDB operations, and utilities in separate modules

## Flux Queries
- Use `json.dumps()` for field lists in generated Flux
- Numeric fields aggregated with `mean`, non-numeric with `last`
- Task offsets spread via `hash_to_integer` to avoid thundering herd

## Error Handling
- Raise `Exception` with descriptive messages for parse failures and ownership conflicts
- Log operations at INFO level (created, updated, deleted resources)

## Git Commits
- Follow the conventions in the commit-conventions steering file (Conventional Commits format)

## Linting & Formatting
- Linter/formatter: **ruff** (version pinned in #[[file:requirements-dev.txt]])
- Config lives in `ruff.toml` at the project root
- Lint check: `ruff check .`
- Format check: `ruff format --check .`
- Auto-fix: `ruff check --fix .` and `ruff format .`
- CI runs both `ruff check` and `ruff format --check` before tests

## Testing
- Test framework: **pytest** (version pinned in #[[file:requirements-dev.txt]])
- Tests live in `tests/` and are discovered automatically by pytest
- Use plain `assert` statements, not `self.assertEqual` / `self.assertTrue`
- Use `@pytest.fixture` for shared setup (e.g. mocked `DownsampleManager`)
- Use `@pytest.mark.parametrize` for data-driven tests instead of repeating test methods
- Use plain classes (no `unittest.TestCase` inheritance) to group related tests
- Mock InfluxDB client interactions with `unittest.mock.MagicMock` / `patch`
- Run tests locally: `venv/Scripts/pytest tests/ -v` (Windows) or `venv/bin/pytest tests/ -v` (Linux/macOS)

## Dependencies
- `requirements.txt` — runtime deps only (installed in Docker image)
- `requirements-dev.txt` — includes runtime deps via `-r requirements.txt` plus test deps (pytest, ruff, testcontainers)
- When adding a new dependency, pin it to a specific version (e.g. `package==1.2.3`)
- Put test-only packages in `requirements-dev.txt`, not `requirements.txt`
- Local dev setup: `pip install -r requirements-dev.txt`
