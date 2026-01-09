# Serious DBI Sam

ODBC Bridge for DuckDB - Query legacy DBISAM databases from DuckDB with full SQL support, filter pushdown, and window functions.

## Purpose

Access legacy Windows-only ODBC data sources (specifically DBISAM) from DuckDB running on Linux. The bridge handles WAL/locking issues and provides a clean gRPC interface.

## Features

- **Virtual Catalog**: Query DBISAM tables with standard SQL via `ATTACH`
- **Filter Pushdown**: WHERE clauses sent to DBISAM, minimizing data transfer
- **Column Projection**: Only requested columns fetched from remote database
- **Window Functions**: Full DuckDB analytical capabilities on remote data
- **Streaming**: Large result sets streamed efficiently via gRPC

## Architecture

```
┌─────────────────────┐         gRPC/HTTP2         ┌─────────────────────┐
│  Linux              │                            │  Windows            │
│                     │                            │                     │
│  DuckDB             │                            │  OdbcBridge.Service │
│    └─ odbcbridge ───┼────────────────────────────┼──►  (C# / .NET 8)   │
│       extension     │       Port 50051           │        │            │
│                     │                            │        ▼ ODBC       │
└─────────────────────┘                            │     DBISAM          │
                                                   │     Database        │
                                                   └─────────────────────┘
```

## Quick Start

```sql
-- Load extension
LOAD 'odbcbridge';

-- Configure connection
SET odbcbridge_host = '192.168.1.100';
SET odbcbridge_port = 50051;

-- Attach remote database as virtual catalog
ATTACH '' AS em (TYPE odbcbridge);

-- Query with filter pushdown
SELECT CODE, CPYNAME, BALANCE
FROM em.main.Customer
WHERE BALANCE > 1000;
```

## Example: Complex Analytics Query

```sql
ATTACH '' AS em (TYPE odbcbridge);

-- Top invoice per customer by revenue (window functions + filter pushdown)
WITH invoice_totals AS (
    SELECT
        SACUST, SAINV,
        SUM(SAVALNET) AS total_revenue,
        COUNT(*) AS line_count,
        MIN(SADATE) AS invoice_date
    FROM em.main.Analysis
    WHERE SADATE > '2025-01-01'  -- Pushed to DBISAM
    GROUP BY SACUST, SAINV
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY SACUST ORDER BY total_revenue DESC) AS rank,
        SUM(total_revenue) OVER (PARTITION BY SACUST) AS customer_total
    FROM invoice_totals
)
SELECT SACUST, SAINV, invoice_date, total_revenue, customer_total
FROM ranked WHERE rank = 1
ORDER BY customer_total DESC
LIMIT 20;
```

## Components

| Directory | Description |
|-----------|-------------|
| `duckdb-odbcbridge/` | DuckDB C++ extension with virtual catalog |
| `OdbcBridge.Service/` | Windows gRPC service (C# / .NET 8) |
| `odbcbridge/` | Protocol specifications and documentation |

## Building

### DuckDB Extension (Linux)

```bash
cd duckdb-odbcbridge

# Set vcpkg toolchain
export CMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build
make release

# Install
cp build/release/odbcbridge.duckdb_extension ~/.duckdb/extensions/v1.2.1/linux_amd64/
```

### Windows Service

```bash
cd OdbcBridge.Service

# Build single-file executable
dotnet publish -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true

# Install as Windows service
sc create OdbcBridgeService binPath= "C:\path\to\OdbcBridge.Service.exe"
sc start OdbcBridgeService
```

## Configuration

### DuckDB Settings

```sql
SET odbcbridge_host = '192.168.1.100';  -- Windows server IP
SET odbcbridge_port = 50051;             -- gRPC port
```

### Service Configuration (appsettings.json)

```json
{
  "OdbcBridge": {
    "Dsn": "Exportmaster",
    "Port": 50051,
    "BatchSize": 1000,
    "CommandTimeout": 300,
    "LogDir": "C:\\RI Services\\WinServices\\Serious-DBI-Sam\\Logs"
  }
}
```

## Table Functions

For direct control without virtual catalog:

```sql
-- List tables
SELECT * FROM dbisam_tables();

-- Describe schema
SELECT * FROM dbisam_describe('Customer');

-- Execute raw SQL
SELECT * FROM dbisam_query('SELECT * FROM Customer WHERE CODE = ''123''');
```

## Filter Pushdown Support

Filters are pushed to DBISAM when possible; otherwise DuckDB filters locally:

| Filter Type | Pushed to DBISAM |
|-------------|------------------|
| `=`, `!=`, `<`, `>`, `<=`, `>=` | Yes |
| `IS NULL`, `IS NOT NULL` | Yes |
| `AND`, `OR` | Yes |
| `LIKE` | No (works, filtered locally) |
| Functions | No (works, filtered locally) |
| Subqueries | No (works, executed by DuckDB) |

## Limitations

- Read-only (no INSERT, UPDATE, DELETE)
- Single DSN per service instance
- DBISAM-specific SQL dialect (TOP at end of query)
- Requires Windows for ODBC connectivity

## Tech Stack

- **Extension**: C++17 / DuckDB 1.2.1 / gRPC / Protocol Buffers
- **Service**: C# / .NET 8 / ASP.NET Core / gRPC / System.Data.Odbc
- **Protocol**: Protocol Buffers 3 over HTTP/2

## Documentation

- [duckdb-odbcbridge/README.md](duckdb-odbcbridge/README.md) - Extension details
- [odbcbridge/PROTOCOL.md](odbcbridge/PROTOCOL.md) - gRPC protocol specification
- [odbcbridge/BUILD.md](odbcbridge/BUILD.md) - Build instructions
