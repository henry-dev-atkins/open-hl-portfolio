# Contributing

## Development Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e .[dev]
```

Run the local checks before opening a PR:

```powershell
python -m ruff check .
python -m pytest -q
```

## Data Safety

- Do not commit files from `data/raw/`, `data/staging/`, or `data/marts/`.
- Do not attach real HL exports, PDFs, screenshots, or account data to issues or pull requests.
- Use the sanitized demo dataset in `examples/demo_data/` when you need a reproducible example.

## Pull Requests

- Keep changes focused and describe any user-visible behavior change.
- Add or update tests when parser, metric, or mart behavior changes.
- If a change depends on new config or sample data, document it in `README.md`.

## Project Scope

The automation flows are PowerShell-first and oriented around HL exports, but the core
Python package should remain understandable and testable without broker credentials.
