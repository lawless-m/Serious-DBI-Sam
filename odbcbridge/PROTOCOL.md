# ODBC Bridge Protocol

gRPC protocol definition for communication between the DuckDB extension (client) and the Windows ODBC service (server).

## Protocol Buffer Definition

```protobuf
syntax = "proto3";

package odbcbridge;

option csharp_namespace = "OdbcBridge";

// The ODBC Bridge service
service OdbcBridge {
  // List all tables in the configured DSN
  rpc ListTables(ListTablesRequest) returns (ListTablesResponse);
  
  // Describe a table's schema
  rpc DescribeTable(DescribeTableRequest) returns (DescribeTableResponse);
  
  // Execute a query and stream results
  rpc Query(QueryRequest) returns (stream QueryResponse);
}

// Empty request - the DSN is configured server-side
message ListTablesRequest {}

message ListTablesResponse {
  repeated string table_names = 1;
}

message DescribeTableRequest {
  string table_name = 1;
}

message ColumnInfo {
  string name = 1;
  string type_name = 2;      // ODBC type name (e.g., "VARCHAR", "INTEGER")
  int32 odbc_type = 3;       // ODBC SQL type constant
  int32 size = 4;            // Column size
  int32 decimal_digits = 5;  // For numeric types
  bool nullable = 6;
}

message DescribeTableResponse {
  string table_name = 1;
  repeated ColumnInfo columns = 2;
}

message QueryRequest {
  string sql = 1;
  optional int32 limit = 2;  // If set, service will enforce this limit
}

// Streamed response - first message contains schema, subsequent messages contain row batches
message QueryResponse {
  oneof payload {
    QuerySchema schema = 1;
    RowBatch rows = 2;
  }
}

message QuerySchema {
  repeated ColumnInfo columns = 1;
}

message RowBatch {
  repeated Row rows = 1;
}

message Row {
  repeated Value values = 1;
}

// Value can be null or one of several types
message Value {
  oneof value {
    bool is_null = 1;
    int64 int_value = 2;
    double double_value = 3;
    string string_value = 4;
    bytes bytes_value = 5;
    bool bool_value = 6;
  }
}
```

## Type Mapping

### ODBC to Protocol Buffer

| ODBC Type | Protobuf Field |
|-----------|----------------|
| SQL_CHAR, SQL_VARCHAR, SQL_LONGVARCHAR | string_value |
| SQL_WCHAR, SQL_WVARCHAR, SQL_WLONGVARCHAR | string_value |
| SQL_TINYINT, SQL_SMALLINT, SQL_INTEGER, SQL_BIGINT | int_value |
| SQL_REAL, SQL_FLOAT, SQL_DOUBLE | double_value |
| SQL_DECIMAL, SQL_NUMERIC | string_value (preserve precision) |
| SQL_BIT | bool_value |
| SQL_BINARY, SQL_VARBINARY, SQL_LONGVARBINARY | bytes_value |
| SQL_DATE, SQL_TIME, SQL_TIMESTAMP | string_value (ISO 8601) |
| NULL | is_null = true |

### Protocol Buffer to DuckDB

| Protobuf Field | DuckDB Type |
|----------------|-------------|
| int_value | BIGINT |
| double_value | DOUBLE |
| string_value | VARCHAR |
| bytes_value | BLOB |
| bool_value | BOOLEAN |
| is_null | NULL |

## Streaming Behaviour

The `Query` RPC uses server-side streaming:

1. Client sends `QueryRequest` with SQL and optional limit
2. Server responds with first `QueryResponse` containing `QuerySchema`
3. Server sends subsequent `QueryResponse` messages containing `RowBatch`
4. Each batch contains up to 1000 rows (configurable)
5. Stream ends when all rows are sent

This allows efficient transfer of large result sets without loading everything into memory.

## Error Handling

gRPC status codes used:

| Situation | Status Code |
|-----------|-------------|
| Invalid SQL syntax | INVALID_ARGUMENT |
| Table not found | NOT_FOUND |
| ODBC connection error | UNAVAILABLE |
| Query execution error | INTERNAL |
| Limit must be positive | INVALID_ARGUMENT |

Error details are provided in the status message.

## Limit Handling

When `limit` is specified in `QueryRequest`:

1. If the SQL already contains a LIMIT clause, the service uses the smaller of the two values
2. If no LIMIT clause exists, the service appends one
3. This prevents pulling excessive data over the network

The service does simple SQL parsing to detect existing LIMIT clauses. For complex queries where this might fail, it wraps the query:

```sql
SELECT * FROM (original_query) AS q LIMIT n
```
