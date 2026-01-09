# DuckDB ODBC Bridge Extension

A DuckDB extension that provides seamless access to remote ODBC data sources (specifically DBISAM databases) via a gRPC bridge service.

## Features

- **Virtual Catalog**: Attach remote DBISAM databases and query tables with standard SQL
- **Filter Pushdown**: WHERE clauses are pushed to DBISAM, minimizing data transfer
- **Column Projection**: Only requested columns are fetched from the remote database
- **Window Functions**: Full DuckDB analytical capabilities on remote data
- **Mixed Queries**: Join remote DBISAM tables with local DuckDB tables

## Quick Start

```sql
-- Load the extension
LOAD 'odbcbridge';

-- Configure connection (optional - defaults to localhost:50051)
SET odbcbridge_host = '192.168.1.100';
SET odbcbridge_port = 50051;

-- Attach the remote database as a virtual catalog
ATTACH '' AS em (TYPE odbcbridge);

-- Query remote tables directly
SELECT * FROM em.main.Customer WHERE CODE = '12345';
```

## Virtual Catalog Usage

The virtual catalog allows you to query DBISAM tables as if they were native DuckDB tables:

```sql
-- Attach the remote database
ATTACH '' AS em (TYPE odbcbridge);

-- List tables
SELECT table_name FROM information_schema.tables WHERE table_catalog = 'em';

-- Query with filter pushdown (only matching rows transferred)
SELECT CODE, CPYNAME, BALANCE
FROM em.main.Customer
WHERE BALANCE > 1000;

-- Aggregate queries
SELECT COUNT(*) FROM em.main.Analysis WHERE SADATE > '2025-01-01';
```

### Filter Pushdown

Filters are automatically pushed to DBISAM when possible:

| Operator | Pushed | Example |
|----------|--------|---------|
| `=`, `!=` | Yes | `WHERE CODE = '123'` |
| `<`, `>`, `<=`, `>=` | Yes | `WHERE BALANCE > 1000` |
| `IS NULL`, `IS NOT NULL` | Yes | `WHERE EMAIL IS NOT NULL` |
| `AND` | Yes | `WHERE A = 1 AND B = 2` |
| `OR` | Yes | `WHERE A = 1 OR A = 2` |
| `LIKE` | No | Works, filtered locally |
| Functions | No | Works, filtered locally |
| Subqueries | No | Works, executed by DuckDB |

### Column Projection

Only the columns you SELECT are fetched from DBISAM:

```sql
-- Only fetches CODE and CPYNAME columns, not all 134 columns
SELECT CODE, CPYNAME FROM em.main.Customer;
```

## Complex Query Example

This query demonstrates window functions with filter pushdown on a remote DBISAM table:

```sql
LOAD 'odbcbridge';
ATTACH '' AS em (TYPE odbcbridge);

-- Top invoice per customer by revenue (since 2025-01-01)
-- Filter "SADATE > '2025-01-01'" is pushed to DBISAM
WITH invoice_totals AS (
    SELECT
        SACUST,
        SAINV,
        SUM(SAVALNET) AS total_revenue,
        COUNT(*) AS line_count,
        MIN(SADATE) AS invoice_date
    FROM em.main.Analysis
    WHERE SADATE > '2025-01-01'
    GROUP BY SACUST, SAINV
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY SACUST ORDER BY total_revenue DESC) AS revenue_rank,
        SUM(total_revenue) OVER (PARTITION BY SACUST) AS customer_total,
        ROUND(100.0 * total_revenue / SUM(total_revenue) OVER (PARTITION BY SACUST), 2) AS pct_of_customer
    FROM invoice_totals
)
SELECT
    SACUST AS customer,
    SAINV AS top_invoice,
    invoice_date,
    line_count,
    ROUND(total_revenue, 2) AS invoice_revenue,
    ROUND(customer_total, 2) AS customer_total_revenue,
    pct_of_customer AS pct_of_total
FROM ranked
WHERE revenue_rank = 1
ORDER BY customer_total DESC
LIMIT 20;
```

Output:
```
┌──────────┬─────────────┬──────────────┬────────────┬─────────────────┬────────────────────────┬──────────────┐
│ customer │ top_invoice │ invoice_date │ line_count │ invoice_revenue │ customer_total_revenue │ pct_of_total │
├──────────┼─────────────┼──────────────┼────────────┼─────────────────┼────────────────────────┼──────────────┤
│ 993203   │ 1205223     │ 2025-12-10   │          2 │        138669.3 │              2245220.9 │         6.18 │
│ 425320   │ 1199896     │ 2025-04-22   │        186 │       111983.22 │             1549982.99 │         7.22 │
│ 992722   │ 1203103     │ 2025-09-25   │        259 │        53084.46 │             1288757.88 │         4.12 │
│ ...      │ ...         │ ...          │        ... │             ... │                    ... │          ... │
└──────────┴─────────────┴──────────────┴────────────┴─────────────────┴────────────────────────┴──────────────┘
```

## Table Functions

For more control, use the table functions directly:

```sql
-- List available tables
SELECT * FROM dbisam_tables();

-- Describe table schema
SELECT * FROM dbisam_describe('Customer');

-- Execute arbitrary SQL (no filter pushdown optimization)
SELECT * FROM dbisam_query('SELECT * FROM Customer WHERE CODE = ''123''');

-- With row limit
SELECT * FROM dbisam_query('SELECT * FROM Analysis', 1000);
```

## Configuration

```sql
-- Connection settings
SET odbcbridge_host = '192.168.1.100';  -- Default: localhost
SET odbcbridge_port = 50051;             -- Default: 50051

-- Attach with custom settings
ATTACH '' AS mydb (TYPE odbcbridge, HOST '10.0.0.1', PORT 50052);
```

## Building

### Prerequisites

- CMake 3.21+
- C++17 compiler (GCC 11+, Clang 13+)
- gRPC and Protocol Buffers (via vcpkg)
- Ninja (recommended)

### Build Commands

```bash
# Set vcpkg toolchain
export CMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build release
make release

# Output: build/release/odbcbridge.duckdb_extension
```

### Installation

```bash
# Copy to DuckDB extensions directory
cp build/release/odbcbridge.duckdb_extension ~/.duckdb/extensions/v1.2.1/linux_amd64/

# Load in DuckDB (use -unsigned flag for unsigned extensions)
duckdb -unsigned -c "LOAD 'odbcbridge';"
```

## Architecture

```
┌─────────────────┐         gRPC/HTTP2         ┌─────────────────────┐
│  DuckDB         │ ◄─────────────────────────► │  OdbcBridge.Service │
│  + odbcbridge   │        Port 50051          │  (Windows)          │
│  (Linux)        │                            │                     │
└─────────────────┘                            └──────────┬──────────┘
                                                          │ ODBC
                                                          ▼
                                               ┌─────────────────────┐
                                               │  DBISAM Database    │
                                               │  (Exportmaster)     │
                                               └─────────────────────┘
```

## Requirements

- **ODBC Bridge Service**: Windows service providing gRPC access to DBISAM
- **Network Access**: Port 50051 (configurable) between DuckDB client and Windows service
- **DuckDB 1.2.1+**: Extension built against DuckDB 1.2.1 API

## Table Name Case Sensitivity

DBISAM table names are case-sensitive in the catalog listing but case-insensitive in SQL queries:

```sql
-- Table might be listed as "Analysis" (mixed case)
SELECT * FROM dbisam_tables() WHERE table_name ILIKE '%analysis%';

-- But queries work with any case
SELECT * FROM em.main.Analysis ...
SELECT * FROM em.main.ANALYSIS ...  -- Also works
```

## Limitations

- Read-only access (no INSERT, UPDATE, DELETE)
- Single schema (`main`) - DBISAM has no schema concept
- Some complex filters cannot be pushed down (LIKE, functions, subqueries)
- Requires Windows service for ODBC connectivity
