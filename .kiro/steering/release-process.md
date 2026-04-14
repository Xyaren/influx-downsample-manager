---
inclusion: manual
---

# Release Process

## Overview

Releases are tag-driven. Pushing a semver tag (`v*`) to GitHub triggers the **Release** workflow (#[[file:.github/workflows/release.yml]]) which builds and publishes a Docker image to GHCR.

## Steps

1. Ensure `main` is green (CI passes).
2. Create an annotated tag:
   ```bash
   git tag -a v<MAJOR>.<MINOR>.<PATCH> -m "Release v<MAJOR>.<MINOR>.<PATCH>"
   ```
3. Push the tag:
   ```bash
   git push origin main --tags
   ```
4. The Release workflow will:
   - Build the Docker image from the `Dockerfile`
   - Push to `ghcr.io/xyaren/influx-downsample-manager` with tags: `<MAJOR>.<MINOR>.<PATCH>`, `<MAJOR>.<MINOR>`, `<MAJOR>`, and `latest`

## Docker Image Tags

| Tag pattern | Example | Description |
|---|---|---|
| `<version>` | `0.1.0` | Exact release version |
| `<major>.<minor>` | `0.1` | Tracks latest patch in that minor |
| `<major>` | `0` | Tracks latest minor+patch in that major |
| `latest` | — | Always the most recent release |

## Notes

- There is no GitHub Release / changelog automation yet — tags only trigger the Docker build.
- The `latest` tag is only applied when the tag is on the default branch.
- Versions follow [Semantic Versioning](https://semver.org/).
