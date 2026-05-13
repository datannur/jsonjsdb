# Changelog

This monorepo contains two packages with independent versioning:

- **[jsonjsdb](./jsonjsdb/)** - Core JSONJS database loading library
- **[jsonjsdb-builder](./jsonjsdb-builder/)** - Tools and utilities for building JSONJS databases

For detailed changes, see individual package changelogs:

- [jsonjsdb/CHANGELOG.md](./jsonjsdb/CHANGELOG.md)
- [jsonjsdb-builder/CHANGELOG.md](./jsonjsdb-builder/CHANGELOG.md)

## Project Overview

### 2026-05-13

- add: dependency release cooldowns for npm and uv

### 2025-12-14

- refactor: migrate to organisation datannur and use npm trusted publisher

### 2025-10-02

- refactor: enforce camelCase for property with eslint rule

### 2025-09-27

- add: prettier and ts check in ci
- refactor: enforce camelCase for property with eslint rule

### 2025-09-20

- add: release.yml for automated releases
- change: restructured CI with unified workflow in ci.yml
- fix: remove git tag creation and push from release workflow
