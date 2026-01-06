# Serious DBI Sam

ODBC Bridge for DuckDB - A DuckDB extension that queries remote ODBC data sources via a Windows service.

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

## Components

This repository contains three main components:

1. **duckdb-odbcbridge/** - DuckDB C++ extension
2. **OdbcBridge.Service/** - Windows C# gRPC service
3. **odbcbridge/** - Documentation and specifications

## Quick Start

### DuckDB Usage

```sql
-- Configure the bridge
SET odbcbridge_host = '192.168.1.100';
SET odbcbridge_port = 50051;

-- List available tables
SELECT * FROM dbisam_tables();

-- Show table schema
SELECT * FROM dbisam_describe('tablename');

-- Execute a query
SELECT * FROM dbisam_query('SELECT * FROM tablename');

-- Execute with row limit
SELECT * FROM dbisam_query('SELECT * FROM tablename', 1000);
```

## Documentation

For detailed information, see the [odbcbridge documentation](odbcbridge/CONTENTS.md):

- [CONTENTS.md](odbcbridge/CONTENTS.md) - Project overview and quick reference
- [PROTOCOL.md](odbcbridge/PROTOCOL.md) - gRPC/Protobuf protocol definition and type mappings
- [SERVICE.md](odbcbridge/SERVICE.md) - Windows C# service implementation details
- [EXTENSION.md](odbcbridge/EXTENSION.md) - DuckDB C++ extension implementation details
- [BUILD.md](odbcbridge/BUILD.md) - Build instructions for both components

## Features

- ✅ Single configured DSN
- ✅ List tables
- ✅ Describe tables
- ✅ Execute queries with optional limit
- ✅ Streaming results for large datasets

## Scope

This is intentionally minimal. The following are **not** supported:

- ❌ Multiple DSNs
- ❌ Authentication
- ❌ Write operations
- ❌ Transactions

## Tech Stack

- **Service**: C# / .NET 8 / gRPC / System.Data.Odbc
- **Extension**: C++ / DuckDB Extension API / gRPC
- **Protocol**: Protocol Buffers 3

## License

[Specify your license here]
