[![NPM Version](https://img.shields.io/npm/v/jsonjsdb)](https://www.npmjs.com/package/jsonjsdb)
![npm bundle size](https://img.shields.io/bundlephobia/minzip/jsonjsdb)
[![NPM License](https://img.shields.io/npm/l/jsonjsdb)](LICENSE)
[![CI](https://github.com/datannur/jsonjsdb/workflows/CI/badge.svg)](https://github.com/datannur/jsonjsdb/actions/workflows/ci.yml)

# Jsonjsdb

A comprehensive client-side relational database solution for static Single Page Applications (SPA). Jsonjsdb enables offline data storage and querying capabilities when running applications locally (file://) or over HTTP/HTTPS (localhost or production servers).

## Table of Contents

- [🚀 Quick Start](#-quick-start)
- [📦 Packages](#-packages)
  - [jsonjsdb - Core Library](#jsonjsdb---core-library)
  - [jsonjsdb-py - Python Library](#jsonjsdb-py---python-library)
- [🎯 Use Cases & Limitations](#-use-cases--limitations)
  - [✅ Perfect for](#-perfect-for)
  - [⚠️ Limitations](#️-limitations)
- [🤝 Contributing](#-contributing)
- [📄 License](#-license)

## 🚀 Quick Start

```bash
npm install jsonjsdb
```

```js
import Jsonjsdb from 'jsonjsdb'

const db = new Jsonjsdb()
await db.init()
const users = db.getAll('user')
```

## 📦 Packages

This monorepo contains two actively maintained packages:

### [jsonjsdb](./jsonjsdb) - Core Library

The main client-side database library for frontend applications.

- ✅ Client-side relational database
- ✅ File system (file://) and HTTP/HTTPS support
- ✅ Runs locally or on web servers
- ✅ Memory-based fast queries
- ✅ Supports 100-200MB databases

**[📖 View Documentation](./jsonjsdb/README.md)**

### [jsonjsdb-py](./jsonjsdb-py) - Python Library

Python library for JSONJS databases with full CRUD support and relational queries.

- ✅ Full CRUD operations
- ✅ Typed API support
- ✅ Relational queries and filtering
- ✅ Compatible with TypeScript jsonjsdb file format

**[📖 View Documentation](./jsonjsdb-py/README.md)**

## 🎯 Use Cases & Limitations

### ✅ Perfect for:

**Offline Applications**

- Applications that need to function without network connectivity
- Local-first applications with occasional sync capabilities
- Embedded applications in restricted environments

**Corporate Deployments**

- Enterprise environments with strict security policies
- Applications deployed on shared drives or local networks
- Solutions requiring minimal IT infrastructure approval

**Portable Solutions**

- Cross-platform compatibility (server, desktop, shared storage)
- Simple drag-and-drop deployment without installation
- Applications that need to run from USB drives or isolated systems

**Static Sites with Data**

- Documentation sites with searchable content
- Catalogs and directories with filtering capabilities
- Educational resources with interactive data exploration

### ⚠️ Limitations

**Data Interactivity**

- Not designed for real-time collaborative features
- Users cannot interact with each other through the database
- Similar limitations to static website architectures

**Update Strategy**

- Optimized for batch data updates rather than real-time modifications
- Database updates require regenerating the entire dataset
- Best suited for periodically updated, relatively stable data

**Memory Constraints**

- Entire database loads into memory during initialization
- Performance testing shows optimal range: 100-200MB on modern systems
- Larger datasets may cause performance degradation or memory issues

**Browser Compatibility**

- Requires modern browser support for local file system access
- Some features may be limited in older browser versions

## 🤝 Contributing

Contributions are welcome! Whether it's bug reports, new features, or improvements, your input is appreciated.

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.
