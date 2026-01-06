# DuckDB ODBC Bridge Extension

A DuckDB extension that queries remote ODBC data sources via the ODBC Bridge service.

## Overview

This extension provides table functions to query ODBC data sources (specifically DBISAM) running on a Windows machine from DuckDB running on Linux.

## Functions

```sql
-- List available tables
SELECT * FROM dbisam_tables();

-- Show table schema
SELECT * FROM dbisam_describe('tablename');

-- Execute a query
SELECT * FROM dbisam_query('SELECT * FROM tablename');

-- Execute with row limit
SELECT * FROM dbisam_query('SELECT * FROM tablename', 1000);
```

## Configuration

```sql
SET odbcbridge_host = '192.168.1.100';
SET odbcbridge_port = 50051;
SET odbcbridge_catalog_name = 'em';  -- Optional: customize catalog name (default: 'dbisam')
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

## Usage

### Table Functions

```sql
-- Load extension
LOAD 'path/to/odbcbridge.duckdb_extension';

-- Configure connection
SET odbcbridge_host = '192.168.1.100';
SET odbcbridge_port = 50051;

-- Query data using table functions
SELECT * FROM dbisam_tables();
SELECT * FROM dbisam_query('SELECT * FROM customers WHERE active = 1');
```

### Virtual Tables (Direct SQL Access)

```sql
-- Load extension
LOAD 'path/to/odbcbridge.duckdb_extension';

-- Configure connection
SET odbcbridge_host = '192.168.1.100';
SET odbcbridge_port = 50051;
SET odbcbridge_catalog_name = 'em';  -- Optional: use 'em' instead of 'dbisam'

-- Query tables directly!
SELECT * FROM em.products WHERE price > 100;
SELECT * FROM em.customers LIMIT 10;

-- Join remote tables
SELECT p.name, c.company_name
FROM em.products p
JOIN em.customers c ON p.customer_id = c.id;

-- Mix with local DuckDB tables
SELECT local.*, remote.status
FROM local_table local
LEFT JOIN em.orders remote ON local.order_id = remote.id;
```

## Requirements

This extension requires the ODBC Bridge Windows service to be running and accessible.
See the `OdbcBridge.Service` project for the Windows service component.
