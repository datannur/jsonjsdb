# jsonjsdb

## 0.8.10 (2026-03-04)

- fix: update dependencies

## 0.8.9 (2026-01-16)

- fix: parentId not defined

## 0.8.8 (2026-01-16)

- remove: error log for missing table in aliases processing

## 0.8.7 (2026-01-04)

- fix: update dependencies

## 0.8.6 (2025-12-14)

- refactor: migrate to organisation datannur

## 0.8.5 (2025-11-30)

- fix: update dependencies

## 0.8.4 (2025-10-31)

- fix: skip `__table__` in addDbSchema() and correct case for var name

## 0.8.3 (2025-10-31)

- change: addDbSchema use JSON Schema

## 0.8.2 (2025-10-30)

- fix: cache url invalidation for files without version

## 0.8.1 (2025-10-30)

- fix: isHttpProtocol based on window.location.protocol

## 0.8.0 (2025-10-30)

- add: both json and jsonjs file input support depending on environment

## 0.7.8 (2025-10-23)

- add: sourcemap generation in Vite build configuration

## 0.7.7 (2025-10-02)

- fix: metaVariable storageKey

## 0.7.6 (2025-10-02)

- add: metaVariable storageKey

## 0.7.5 (2025-10-02)

- change: transform variable names to camelCase on data load

## 0.7.4 (2025-10-02)

- change: update validIdChars configuration to allow spaces

## 0.7.3 (2025-10-02)

- add: standardizeId method to clean up IDs by removing invalid characters
- remove: escapeHtml

## 0.7.2 (2025-09-30)

- add: escapeHtml to prevent XSS attacks when rendering data in HTML context
- fix: tables type and and checkIntegrity method in Jsonjsdb class
- refactor: use idSuffix variable to improve flexibility and maintainability

## 0.7.1 (2025-09-29)

- add: support for undefined values in ForeignTableObj interface
- add: comprehensive tests for countRelated() methods
- change: make DatabaseRow.id optional to support tables without id
- change: rename `tableHasId()` to `exists()` for better API clarity
- change: rename `hasNb()` to `countRelated()` for professional naming

### 0.7.0 (2025-09-28)

- add: generic type, getSchema method and "use" prop for existing entities
- refactor: use metadata property for metadata tables so the tables property is kept for actual data tables

## 0.6.6 (2025-09-20)

- fix: remove git tag creation and push from release workflow

## 0.6.5 (2025-09-20)

- add: ci/cd github action for automated test and releases

## 0.6.4 (2025-09-16)

- fix: IntegrityChecker empty ID detection logic (build issue)

## 0.6.3 (2025-09-16)

- fix: IntegrityChecker empty ID detection logic

## 0.6.2 (2025-09-16)

- fix: DBrowser set data without stringify

## 0.6.1 (2025-09-16)

- fix: aliases format

## 0.6.0 (2025-09-16)

- add: test for DBrowser and IntegrityChecker
- change: apply strict type checking and standard naming conventions

## 0.5.0 (2025-09-11)

- change: improve alias creation with initial aliases
- change: move from js to ts and from rollup to vite

## 0.4.1 (2025-09-04)

- fix: add_meta second parameter

## 0.4.0 (2025-09-02)

- change: rename `__meta__` to `__table__`
- change: improve documentation

## 0.3.14 (2025-05-15)

- add: param to set db_schema in add_meta()

## 0.3.13 (2025-05-14)

- fix: typo in add_meta()

## 0.3.12 (2025-05-14)

- fix: remove from schema only metaDataset from user_data

## 0.3.11 (2025-05-14)

- fix: metaDataset duplicate entry in schema

## 0.3.10 (2025-05-14)

- add: metadata only in schema but not in data

## 0.3.9 (2024-10-29)

- add: show error and filter for duplicate table name in meta

## 0.3.8 (2024-10-16)

- add: get alias from config file

## 0.3.7 (2024-10-16)

- add: get_last_modif_timestamp() type

## 0.3.6 (2024-10-16)

- add: timestamp last modification from **meta** file

## 0.3.5 (2024-10-15)

- add: one **meta_schema**.json.js file to replace other meta schema files

## 0.3.4 (2024-10-14)

- fix: again Loader.\_normalize_schema() with no ids found

## 0.3.3 (2024-10-13)

- fix: Loader.\_normalize_schema() with no ids found

## 0.3.2 (2024-10-13)

- add: last_update_timestamp to metaDataset

## 0.3.1 (2024-10-13)

- remove: metaDataset virtual (created at load time) from meta schema

## 0.3.0 (2024-10-11)

- add: Loader \_normalize_schema() method to normalize schema
