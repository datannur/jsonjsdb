# jsonjsdb-builder

## 0.6.10 (2026-04-04)

- fix: prevent `__table__.json` rewrite on every dev server start

## 0.6.9 (2026-03-05)

- fix: bump version (0.6.8 was unpublished from npm)

## 0.6.8 (2026-03-04)

- fix: update dependencies

## 0.6.7 (2026-01-04)

- fix: update dependencies

## 0.6.6 (2025-12-14)

- refactor: migrate to organisation datannur

## 0.6.5 (2025-11-30)

- fix: update dependencies and fix glob security issue

## 0.6.4 (2025-10-31)

- fix: prevent unnecessary file rewrites when data hasn't changed

## 0.6.3 (2025-10-30)

- fix: ensure index table is saved in snake_case

## 0.6.2 (2025-10-30)

- fix: use writeJsonjs() for updateMdDir()

## 0.6.1 (2025-10-29)

- fix: `__table__` loading in json format

## 0.6.0 (2025-10-29)

- add: both json and jsonjs file output

## 0.5.7 (2025-10-20)

- fix: make dbSourcePath optional and handle its absence in initJsonjsdbBuilder

## 0.5.6 (2025-10-20)

- feat: enhance Vite integration with new builder initialization and plugin support

## 0.5.5 (2025-10-02)

- change: evolution with compositeId get name and standardized entityId

## 0.5.4 (2025-10-02)

- refactor: enforce camelCase for property with eslint rule

## 0.5.3 (2025-10-02)

- fix: ensure test isolation by copying all Excel files (including evolution.xlsx) to temporary directories
- fix: updateDbTimestamp to be in seconds (10 digits) instead of milliseconds (13 digits)

## 0.5.2 (2025-09-20)

- fix: remove git tag creation and push from release workflow

## 0.5.1 (2025-09-20)

- add: ci/cd github action for automated test and releases

## 0.5.0 (2025-09-16)

- add: export of low-level utilities
- add: more tests and type safety
- remove `JsonjsdbWatcher`

## 0.4.3 (2025-09-16)

- fix: `__table__` property last_modif in snake_case

## 0.4.2 (2025-09-16)

- fix: jsonjsdbWatcher case in readme

## 0.4.1 (2025-09-16)

- fix: missing types

## 0.4.0 (2025-09-15)

- change: package name, apply eslint and add test

## 0.3.4 (2025-09-14)

- change: start using eslint, rename jsonjsdbAddConfig function and add correct type annotations

## 0.3.3 (2025-09-12)

- fix: missing dependencies for write-excel-file

## 0.3.2 (2025-09-04)

- change: default to non compact mode

## 0.3.1 (2025-09-02)

- remove: badge for bundle size from readme

## 0.3.0 (2025-09-02)

- change: rename `__meta__` to `__table__`
- change: improve documentation

## 0.2.10 (2025-01-29)

- fixed: jsonjsdb_editor: dont stringify null values in history

## 0.2.9 (2025-01-26)

- fixed: jsonjsdb_editor: again dont update evolution timestamp if no change

## 0.2.8 (2025-01-21)

- fixed: jsonjsdb_editor: dont update evolution timestamp if no change

## 0.2.7 (2025-01-18)

- changed: make jsonjsdb_editor use xlsx state for history so we can edit it manually

## 0.2.6 (2025-01-18)

- changed: rename history to evolution

## 0.2.5 (2025-01-15)

- fixed: prevent adding entry when old and new value are both null or empty

## 0.2.4 (2025-01-12)

- fixed: history file was not included if no change

## 0.2.3 (2025-01-08)

- changed: replace column parent_ids by parent_entity_id by getting only the first column with suffix "\_id"

## 0.2.2 (2025-01-07)

- added: name and parent_ids to history entries of type delete

## 0.2.1 (2024-12-28)

- fixed: compare_datasets method skip entity starting with "\_\_"
- refactored: use vite to bundle the lib and move compare_datasets in a separate file

## 0.2.0 (2024-12-27)

- added: compare_datasets method to add history of changes

## 0.1.18 (2024-11-31)

- changed: improve update_md_files

## 0.1.17 (2024-10-31)

- added: update_md_files method to update md files into the db

## 0.1.16 (2024-10-17)

- changed: update main last modification timestamp only if change has been made

## 0.1.15 (2024-10-16)

- added: **meta** row in **meta** table to have the global timestamp of last modification
