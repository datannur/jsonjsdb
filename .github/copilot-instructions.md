# GitHub Copilot Instructions - jsonjsdb Monorepo

## Project Structure

This is a monorepo containing three workspaces/packages:

### 1. `jsonjsdb/` - Core Library

- TypeScript library for JSONJS database loading
- Modular architecture: Jsonjsdb (interface), Loader, DBrowser, IntegrityChecker
- Browser and Node.js support via Vite
- Testing with Vitest on the browser

### 2. `jsonjsdb-builder/` - Builder Tools (DEPRECATED !!!)

- Deprecated package kept in the repository for compatibility/history
- Do not include it in default build, lint, type-check, test, verify, CI, or release work
- Only touch or run builder-specific commands when the user explicitly asks for builder maintenance

### 3. `jsonjsdb-py/` - Python Library

- Python implementation for JSONJS database loading and writing
- Source lives under `jsonjsdb-py/src/jsonjsdb/`
- Tests live under `jsonjsdb-py/tests/`
- Tooling uses `uv`, `pytest`, `ruff`, `pyright`, and `diff-cover`

## Development Context

- **Monorepo with npm Workspaces**: npm workspaces manage the TypeScript packages, but default root commands now target the core package only
- **Deprecated Builder**: `jsonjsdb-builder/` is intentionally ignored by default checks and CI/CD
- **CI/CD Pipeline**: GitHub Actions validate/release the core TypeScript package and the Python package; builder jobs and releases are disabled
- **Path-based Triggers**: CI jobs decide whether to run based on changed package paths
- **Root Commands**: `npm run verify` runs formatting, lint, type-check, and tests for the core TypeScript package only
- **Python Commands**: run Python checks from `jsonjsdb-py/` with `uv`/`make`, for example `make check`, `make test`, `make lint`, and `make typecheck`
- **Shared Tooling**: Root ESLint/Prettier config applies to active TypeScript code and ignores deprecated builder and Python-specific files where configured

## Package-Specific Commands

- Core TypeScript package: run root `npm run verify` for the standard validation path
- Core build: run `npm run build:core` from the repository root
- Python package: run `make check` from `jsonjsdb-py/`; prefer `uv run ...` through the Makefile over calling Python tools directly
- Deprecated builder: avoid `build:builder`, `test:builder`, `lint:builder`, and `type-check:builder` unless explicitly requested

## Code Comments Policy

- Add comments ONLY when absolutely necessary
- All comments must be written in English ONLY
- Never use any other language in comments
- Focus on explaining "why", not "what"
- Document only non-obvious business logic or complex algorithms

## Code Quality Standards

- **Clean & Professional**: Write clean, maintainable code that follows industry best practices
- **Standard Compliance**: Adhere to TypeScript and JavaScript standards consistently
- **Concise & Clear**: Keep code concise while maintaining readability and clarity
- **No Duplication**: Avoid code duplication - extract common logic into reusable functions
- **Simple & Readable**: Prioritize simplicity and readability over complex abstractions
- **Type Safety**: Use proper TypeScript types - avoid `any` type
- **Consistent Naming**: Use clear, descriptive names for variables, functions, and classes
- **Single Responsibility**: Each function and class should have a single, well-defined purpose
