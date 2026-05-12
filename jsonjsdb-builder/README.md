[![NPM Version](https://img.shields.io/npm/v/jsonjsdb-builder)](https://www.npmjs.com/package/jsonjsdb-builder)
[![NPM License](https://img.shields.io/npm/l/jsonjsdb-builder)](LICENSE)
[![CI](https://github.com/datannur/jsonjsdb/workflows/CI/badge.svg)](https://github.com/datannur/jsonjsdb/actions/workflows/ci.yml)

# Jsonjsdb Builder

## Deprecation Notice

jsonjsdb-builder is deprecated and no longer maintained.

- Existing projects can continue using current versions.
- No new features, bug fixes, or security updates are planned.
- New projects should not adopt this package.
- There is currently no official replacement package.
- If you depend on this tool, keep using a pinned version or maintain a fork.

A development tool for converting relational database tables into jsonjs format compatible with [jsonjsdb](../jsonjsdb).

Currently supports Excel (.xlsx) files as source, where each file represents one database table.

**Output formats:** The builder generates two file formats for each table:

- `.json.js` - Compact matrix format (array of arrays) for browser loading without server (on file://)
- `.json` - Standard JSON format (array of objects) for readability and tooling

## Installation

```bash
npm install jsonjsdb-builder
```

## Table of Contents

- [Deprecation Notice](#deprecation-notice)
- [Basic Usage](#basic-usage)
- [Markdown Import](#markdown-import)
- [Preview Generation](#preview-generation)
- [Vite Integration](#vite-integration)
- [Low-level Utilities](#low-level-utilities)
- [API Reference](#api-reference)
- [File Structure](#file-structure)
- [License](#license)

## Basic Usage

### Simple Database Update

Convert Excel files to jsonjs format:

```js
import JsonjsdbBuilder from 'jsonjsdb-builder'

const builder = new JsonjsdbBuilder()
await builder.setOutputDb('app_db') // Output directory
await builder.updateDb('db') // Source Excel files directory
```

**Parameters:**

- `app_db`: Target directory for generated files (creates both .json and .json.js files)
- `db`: Source directory containing .xlsx files

**Output:** For each Excel table, two files are generated:

- `<table>.json.js` - Compact matrix format for the browser
- `<table>.json` - Standard JSON format for editing/inspection

## Markdown Import

Import a folder of Markdown files and expose them as jsonjs tables:

```js
import { JsonjsdbBuilder } from 'jsonjsdb-builder'

const builder = new JsonjsdbBuilder()
await builder.setOutputDb('app_db')
await builder.updateMdDir('markdown', 'content_md')
// Generates app_db/markdown/<file>.json.js and <file>.json
```

The `.json.js` format is: `jsonjs.data["<name>"] = [["content"], ["..."]]` (matrix).  
The `.json` format is: `[{ "content": "..." }]` (objects).

## Preview Generation

Generate a lightweight preview (simple read of Excel files into a subfolder):

```js
await builder.updatePreview('preview', 'db')
// Reads each .xlsx from /db and writes to /app_db/preview
```

## Vite Integration

Configure a simple watcher and auto-reload in your Vite setup:

```js
import { defineConfig } from 'vite'
import FullReload from 'vite-plugin-full-reload'
import { initJsonjsdbBuilder } from 'jsonjsdb-builder'

const builder = await initJsonjsdbBuilder(
  {
    dbPath: 'public/data/db',
    dbSourcePath: 'public/data/db-source',
    previewPath: 'public/data/dataset',
    mdPath: 'public/data/md',
    configPath: 'public/data/jsonjsdb-config.html',
  },
  { isDevelopment: process.env.NODE_ENV === 'development' },
)

export default defineConfig({
  plugins: builder.getVitePlugins(FullReload),
})
```

**Install required dependencies:**

```bash
npm install -D vite-plugin-full-reload
```

This includes:

- Database watching in development mode
- Config injection plugin
- Auto-reload on database changes

## Low-level Utilities

Low-level utility functions are exported for advanced use:

```js
import {
  jsonjsdbToObjects,
  jsonjsdbToMatrix,
  jsonjsdbRead,
  jsonjsdbWrite,
} from 'jsonjsdb-builder'

// Convert 2D matrix -> array of objects
const objects = jsonjsdbToObjects([
  ['id', 'name'],
  [1, 'Alice'],
  [2, 'Bob'],
])

// Directly write a jsonjs file
await jsonjsdbWrite('app_db', 'users', [
  ['id', 'name'],
  [1, 'Alice'],
])
```

## API Reference

### Class: JsonjsdbBuilder

Methods:

- `setOutputDb(dir: string)`: Ensure/create and set the output directory.
- `updateDb(inputDir: string)`: Convert all `.xlsx` files into jsonjs tables (generates both `.json` and `.json.js` files) and update metadata / evolution log.
- `updateMdDir(subdir: string, sourceDir: string)`: Import a markdown directory as jsonjs tables (generates both formats).
- `updatePreview(subfolder: string, sourceDir: string)`: Perform a simple read of source Excel files into a subfolder (no metadata changes).
- `getOutputDb(): string`: Absolute path of the output directory.
- `getTableIndexFile(): string`: Path of the `__table__.json` index file (standard JSON format).

Utilities:

- `jsonjsdbToObjects(matrix)`
- `jsonjsdbToMatrix(objects)`
- `jsonjsdbRead(filePath)`
- `jsonjsdbWrite(dir, name, data, options?)`

## File Structure

Typical generated structure inside `outputDb`:

```
app_db/
  __table__.json.js         # Table index + metadata (matrix format)
  __table__.json            # Table index + metadata (objects format)
  user.json.js              # User table (matrix format)
  user.json                 # User table (objects format)
  tag.json.js               # Tag table (matrix format)
  tag.json                  # Tag table (objects format)
  evolution.json.js         # Evolution log (only if changes, matrix format)
  evolution.json            # Evolution log (objects format)
  markdown/
    intro.json.js           # Markdown content (matrix format)
    intro.json              # Markdown content (objects format)
  preview/
    user.json.js            # Preview copy (via updatePreview)
```

**Format details:**

- `.json.js` files contain compact matrix data: `jsonjs.data['name'] = [["col1","col2"],[val1,val2]]`
- `.json` files contain standard JSON objects: `[{"col1":val1,"col2":val2}]`

## License

MIT License - see [LICENSE](LICENSE) for details.
