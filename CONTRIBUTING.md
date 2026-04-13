# Contributing

Thanks for your interest in contributing to InfluxDB Downsampling Manager!

## Getting Started

1. Fork the repository and clone your fork
2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # or venv\Scripts\activate on Windows
   pip install -r requirements.txt
   ```
3. Create a feature branch from `master`

## Code Style

- Python 3.10+ with modern type annotations (`dict`, `list`, `X | Y` unions)
- Use `@dataclass` for value objects and `TypedDict` for config structures
- Private instance attributes prefixed with `_`
- Module-level logger: `logger = logging.getLogger(__name__)`

## Commit Guidelines

- Write clear, concise commit messages
- Keep commits focused on a single change

### AI-Assisted Contributions

If any part of your commit was written or substantially assisted by an AI tool (e.g. Kiro, Copilot, ChatGPT), you must add a `Co-authored-by` trailer identifying the AI tool used:

```
git commit -m "Your commit message" -m "Co-authored-by: ToolName <tool@example.com>"
```

For example:

```
git commit -m "Add retry logic to task creation" -m "Co-authored-by: Kiro <kiro@amazon.com>"
```

This ensures transparency about AI involvement in the project's history.

## Pull Requests

1. Keep PRs small and focused
2. Describe what changed and why
3. Ensure the manager runs successfully against a test InfluxDB instance before submitting
4. Reference any related issues

## Reporting Issues

Open an issue with:
- A clear description of the problem
- Steps to reproduce
- Expected vs. actual behavior
- Your InfluxDB version and Python version
