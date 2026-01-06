#pragma once

#include "duckdb.hpp"
#include "grpc_client.hpp"

// Forward declare proto types
namespace odbcbridge {
class Value;
}

namespace duckdb {

// Map ODBC type constant to DuckDB logical type
LogicalType MapOdbcTypeToDuckDB(int32_t odbc_type);

// Map protobuf Value to DuckDB Value
Value MapProtoValueToDuckDB(const odbcbridge::Value &proto_value, const ColumnInfo &column_info);

} // namespace duckdb
