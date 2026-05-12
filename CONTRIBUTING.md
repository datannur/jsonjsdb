# Contributing to jsonjsdb

Thanks for contributing.

This repository currently maintains two active packages:

- `jsonjsdb/` (TypeScript core library)
- `jsonjsdb-py/` (Python library)

`jsonjsdb-builder/` is deprecated. Please avoid feature work there unless a maintainer asks for a critical fix.

## Quick Start

1. Create a branch.

```bash
git checkout -b your-change
```

2. Install dependencies for the area you edit.

```bash
# TypeScript workspace
npm ci

# Python package
cd jsonjsdb-py && uv sync
```

3. Run checks.

```bash
# TypeScript core
npm run test:core
npm run build:core
npm run lint:core

# Python package
cd jsonjsdb-py
uv run pytest
uv run ruff check .
uv run pyright src/jsonjsdb tests
```

4. Commit and open a PR to `main`.

## Contribution Rules

- Keep PRs focused: one feature, fix, or refactor per PR.
- Add or update tests for behavior changes.
- Update docs when user-facing behavior changes.
- Keep compatibility with existing JSONJS formats unless explicitly discussed.

## Project Layout

- `jsonjsdb/`: TypeScript source and tests
- `jsonjsdb-py/`: Python source and tests
- `jsonjsdb-builder/`: deprecated package (maintenance only)

## Support

Use GitHub Issues for bug reports and feature requests. For bug reports, include reproduction steps, expected behavior, and actual behavior.
