# ODBC Bridge for DuckDB

A DuckDB extension that queries remote ODBC data sources via a Windows service.

## Purpose

Access legacy Windows-only ODBC data sources (specifically DBISAM) from DuckDB running on Linux, without dealing with WAL/locking issues of direct file access.

## Architecture

```
┌─────────────────────┐         gRPC          ┌─────────────────────┐
│  Linux              │                       │  Windows            │
│                     │                       │                     │
│  DuckDB             │                       │  ODBC Bridge        │
│    └─ Extension ────┼───────────────────────┼──► Service          │
│       (C++)         │      TCP/50051        │       │             │
│                     │                       │       ▼             │
└─────────────────────┘                       │     ODBC            │
                                              │       │             │
                                              │       ▼             │
                                              │    DBISAM           │
                                              └─────────────────────┘
```

## Contents

| File | Description |
|------|-------------|
| CONTENTS.md | This file - start here |
| PROTOCOL.md | gRPC/Protobuf protocol definition and type mappings |
| SERVICE.md | Windows C# service implementation details |
| EXTENSION.md | DuckDB C++ extension implementation details |
| BUILD.md | Build instructions for both components |

## Reading Order

1. **PROTOCOL.md** - Understand the contract between the two components
2. **SERVICE.md** - The Windows side that talks to ODBC
3. **EXTENSION.md** - The DuckDB side that users interact with
4. **BUILD.md** - When you're ready to build

## Quick Reference

### DuckDB Functions Provided

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

### Configuration

```sql
SET odbcbridge_host = '192.168.1.100';
SET odbcbridge_port = 50051;
```

## Scope

This is intentionally minimal:

- ✅ Single configured DSN
- ✅ List tables
- ✅ Describe tables
- ✅ Execute queries with optional limit
- ✅ Streaming results for large datasets
- ❌ Multiple DSNs
- ❌ Authentication
- ❌ Write operations
- ❌ Transactions

## Tech Stack

- **Service**: C# / .NET 8 / gRPC / System.Data.Odbc
- **Extension**: C++ / DuckDB Extension API / gRPC
- **Protocol**: Protocol Buffers 3
