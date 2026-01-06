# DuckDB ODBC Bridge Extension

A DuckDB extension that queries remote ODBC data sources via the ODBC Bridge service.

## Overview

This extension provides two ways to query DBISAM data:

1. **Virtual Tables** (recommended) - Attach DBISAM as a catalog, use standard SQL with automatic filter pushdown
2. **Table Functions** (legacy) - Explicit function calls for queries

## Virtual Tables (Recommended)

Attach DBISAM as a database and query tables directly:

```sql
-- Load extension
LOAD 'odbcbridge';

-- Configure connection
SET odbcbridge_host = '192.168.1.100';
SET odbcbridge_port = 50051;

-- Attach DBISAM catalog
ATTACH '' AS dbisam (TYPE dbisam);

-- Query with automatic filter/projection pushdown!
SELECT code, name, price
FROM dbisam.products
WHERE code = 'ABC123';
-- Only fetches matching rows from server, only requested columns

-- Join with local tables
SELECT p.name, o.quantity
FROM dbisam.products p
JOIN local_orders o ON p.code = o.product_code;

-- List all DBISAM tables
SELECT * FROM information_schema.tables WHERE table_catalog = 'dbisam';
```

### Filter Pushdown

The extension automatically pushes supported filters to the server:

| Filter Type | Pushed to Server |
|-------------|------------------|
| `=`, `<>`, `<`, `>`, `<=`, `>=` | Yes |
| `IS NULL`, `IS NOT NULL` | Yes |
| `AND` combinations | Yes |
| `OR` combinations | Yes |
| `LIKE`, `IN` | No (filtered locally) |

### Projection Pushdown

Only requested columns are fetched from the server:

```sql
-- Only fetches 'code' and 'name' columns
SELECT code, name FROM dbisam.products;
```

## Table Functions (Legacy)

For direct SQL control:

```sql
-- List available tables
SELECT * FROM dbisam_tables();

-- Show table schema
SELECT * FROM dbisam_describe('products');

-- Execute a query
SELECT * FROM dbisam_query('SELECT * FROM products');

-- Execute with row limit
SELECT * FROM dbisam_query('SELECT * FROM products', 1000);
```

## Configuration

```sql
SET odbcbridge_host = '192.168.1.100';  -- Service host
SET odbcbridge_port = 50051;             -- Service port (default: 50051)
```

Or via ATTACH options:

```sql
ATTACH '' AS dbisam (TYPE dbisam, HOST '192.168.1.100', PORT 50051);
```

## Building

### Prerequisites

- CMake 3.21+
- C++17 compiler (GCC 11+, Clang 13+)
- gRPC and Protocol Buffers
- Ninja (recommended)

### Build Commands

```bash
# Using vcpkg for dependencies
export CMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build release
make release

# Build debug
make debug

# Output: build/release/odbcbridge.duckdb_extension
```

## Architecture

```
Linux                              Windows
┌─────────────────────┐           ┌─────────────────────┐
│  DuckDB             │           │  ODBC Bridge        │
│    └─ Extension ────┼── gRPC ──►│  Service            │
│       (C++)         │  TCP:50051│       │             │
└─────────────────────┘           │       ▼ ODBC        │
                                  │    DBISAM           │
                                  └─────────────────────┘
```

## Requirements

This extension requires the ODBC Bridge Windows service to be running and accessible.
See the `OdbcBridge.Service` project for the Windows service component.
