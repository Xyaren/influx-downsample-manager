---
inclusion: auto
---

# README Maintenance

When making changes that affect user-facing behavior, keep `README.md` in sync. Specifically:

- Adding/removing/renaming a module under `manager/` → update the **Project structure** tree
- Changing CLI usage, entry points, or environment variables → update **Usage** and **Environment variables** sections
- Changing the Dockerfile or Docker Compose setup → update **Docker** installation and usage examples
- Adding/removing Python dependencies → verify the **Requirements** section still matches
- Changing config schema (new keys, removed keys, changed defaults) → update **Config file structure**, **Downsample config fields** table, and the inline YAML example
- Changing the minimum Python version → update the **Requirements** section

Do not add sections or badges to the README unless explicitly asked. Keep it concise.
