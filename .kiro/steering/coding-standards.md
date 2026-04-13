---
inclusion: always
---

# Coding Standards

## Python Version
- Python 3.10+ (uses `X | Y` union syntax, `dict[str, ...]` built-in generics, `dataclass`, `TypedDict` with `NotRequired`)

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
- When committing changes, add Kiro as co-author using the trailer: `Co-authored-by: Kiro <kiro@amazon.com>`
