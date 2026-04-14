---
inclusion: always
---

# Commit Conventions

This project uses [Conventional Commits](https://www.conventionalcommits.org/) to produce structured commit messages that feed into auto-generated release notes.

## Format

```
<type>(<scope>): <short summary>

<optional body>

<optional footers>
```

## Types

| Type | Purpose | Release notes section |
|---|---|---|
| `feat` | New feature or capability | 🚀 Features |
| `fix` | Bug fix | 🐛 Bug Fixes |
| `docs` | Documentation only | 📚 Documentation |
| `refactor` | Code change that neither fixes a bug nor adds a feature | 🔧 Refactoring |
| `test` | Adding or updating tests | — (excluded) |
| `ci` | CI/CD pipeline changes | — (excluded) |
| `chore` | Maintenance, deps, tooling | — (excluded) |
| `perf` | Performance improvement | ⚡ Performance |

## Scopes (optional)

Use a scope to indicate the area of the codebase affected:

- `manager` — core manager module
- `query` — query generator
- `config` — configuration loading/parsing
- `model` — data structures
- `docker` — Dockerfile or container setup
- `ci` — GitHub Actions workflows

## Breaking Changes

Append `!` after the type/scope or add a `BREAKING CHANGE:` footer:

```
feat(config)!: rename source_buckets to buckets

BREAKING CHANGE: The `source_buckets` config key has been renamed to `buckets`.
```

## Examples

```
feat(query): add support for median aggregation
fix(manager): prevent duplicate task creation on retry
docs: update chained aggregation trade-offs in README
refactor(model): extract MeasurementConfig to separate TypedDict
ci: add CodeQL analysis workflow
chore: bump influxdb-client to 1.51.0
```

## Footers

Always include the Kiro co-author trailer when commits are AI-assisted:

```
Co-authored-by: Kiro <kiro@amazon.com>
```
