# DuckDB ODBC Bridge Extension

A DuckDB extension that queries a remote ODBC data source via the ODBC Bridge service.

## Overview

The extension provides three table functions:

- `dbisam_tables()` - List available tables
- `dbisam_describe(table_name)` - Show table schema
- `dbisam_query(sql, [limit])` - Execute a query

## Project Structure

Based on the official DuckDB extension template:

```
duckdb-odbcbridge/
├── CMakeLists.txt
├── Makefile
├── src/
│   ├── odbcbridge_extension.cpp    # Extension entry point
│   ├── include/
│   │   ├── odbcbridge_extension.hpp
│   │   ├── grpc_client.hpp
│   │   └── type_mapping.hpp
│   ├── grpc_client.cpp             # gRPC client implementation
│   ├── table_functions.cpp         # DuckDB table function implementations
│   └── type_mapping.cpp            # Protobuf to DuckDB type conversion
├── third_party/
│   └── grpc/                       # gRPC as submodule or find_package
├── proto/
│   └── odbcbridge.proto            # Protocol definition
└── test/
    └── sql/
        └── odbcbridge.test
```

## Dependencies

- DuckDB source (for building extensions)
- gRPC and Protocol Buffers
- CMake 3.21+

## CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.21)
project(odbcbridge_extension)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find or fetch gRPC
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

# Generate protobuf/grpc code
set(PROTO_FILES ${CMAKE_SOURCE_DIR}/proto/odbcbridge.proto)

add_library(odbcbridge_proto STATIC)
target_link_libraries(odbcbridge_proto
    PUBLIC
    protobuf::libprotobuf
    gRPC::grpc++
)

protobuf_generate(
    TARGET odbcbridge_proto
    PROTOS ${PROTO_FILES}
    LANGUAGE cpp
    OUT_VAR PROTO_SRCS
)

protobuf_generate(
    TARGET odbcbridge_proto
    PROTOS ${PROTO_FILES}
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=\$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    OUT_VAR GRPC_SRCS
)

# Extension sources
set(EXTENSION_SOURCES
    src/odbcbridge_extension.cpp
    src/grpc_client.cpp
    src/table_functions.cpp
    src/type_mapping.cpp
)

# Build loadable extension
build_loadable_extension(
    odbcbridge
    ${EXTENSION_SOURCES}
)

target_link_libraries(odbcbridge_loadable_extension
    PRIVATE
    odbcbridge_proto
    gRPC::grpc++
)

target_include_directories(odbcbridge_loadable_extension
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_CURRENT_BINARY_DIR}  # For generated proto headers
)
```

## Key Implementation Details

### Extension Entry Point (odbcbridge_extension.cpp)

```cpp
#define DUCKDB_EXTENSION_MAIN

#include "odbcbridge_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

// Global configuration - set via SET commands
struct OdbcBridgeConfig {
    std::string host = "localhost";
    int port = 50051;
};

static OdbcBridgeConfig global_config;

void SetOdbcBridgeHost(DatabaseInstance &db, const std::string &host) {
    global_config.host = host;
}

void SetOdbcBridgePort(DatabaseInstance &db, int port) {
    global_config.port = port;
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register configuration settings
    auto &config = DBConfig::GetConfig(instance);
    
    config.AddExtensionOption(
        "odbcbridge_host",
        "Host address of the ODBC Bridge service",
        LogicalType::VARCHAR,
        Value("localhost"));
    
    config.AddExtensionOption(
        "odbcbridge_port", 
        "Port of the ODBC Bridge service",
        LogicalType::INTEGER,
        Value(50051));
    
    // Register table functions
    RegisterDbiasmTablesFunction(instance);
    RegisterDbiasmDescribeFunction(instance);
    RegisterDbiasmQueryFunction(instance);
}

void OdbcbridgeExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string OdbcbridgeExtension::Name() {
    return "odbcbridge";
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void odbcbridge_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::OdbcbridgeExtension>();
}

DUCKDB_EXTENSION_API const char *odbcbridge_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}
```

### gRPC Client (grpc_client.cpp)

```cpp
#include "grpc_client.hpp"
#include <grpcpp/grpcpp.h>
#include "odbcbridge.grpc.pb.h"

namespace duckdb {

class OdbcBridgeClient {
public:
    OdbcBridgeClient(const std::string &host, int port) {
        auto channel = grpc::CreateChannel(
            host + ":" + std::to_string(port),
            grpc::InsecureChannelCredentials());
        stub_ = odbcbridge::OdbcBridge::NewStub(channel);
    }
    
    std::vector<std::string> ListTables() {
        odbcbridge::ListTablesRequest request;
        odbcbridge::ListTablesResponse response;
        grpc::ClientContext context;
        
        auto status = stub_->ListTables(&context, request, &response);
        if (!status.ok()) {
            throw IOException("ListTables failed: " + status.error_message());
        }
        
        std::vector<std::string> tables;
        for (const auto &name : response.table_names()) {
            tables.push_back(name);
        }
        return tables;
    }
    
    std::vector<ColumnInfo> DescribeTable(const std::string &table_name) {
        odbcbridge::DescribeTableRequest request;
        request.set_table_name(table_name);
        
        odbcbridge::DescribeTableResponse response;
        grpc::ClientContext context;
        
        auto status = stub_->DescribeTable(&context, request, &response);
        if (!status.ok()) {
            throw IOException("DescribeTable failed: " + status.error_message());
        }
        
        std::vector<ColumnInfo> columns;
        for (const auto &col : response.columns()) {
            columns.push_back({
                col.name(),
                col.type_name(),
                col.odbc_type(),
                col.size(),
                col.decimal_digits(),
                col.nullable()
            });
        }
        return columns;
    }
    
    // Returns a reader for streaming query results
    std::unique_ptr<QueryReader> Query(const std::string &sql, 
                                       std::optional<int32_t> limit) {
        return std::make_unique<QueryReader>(stub_.get(), sql, limit);
    }
    
private:
    std::unique_ptr<odbcbridge::OdbcBridge::Stub> stub_;
};

// Streaming reader for query results
class QueryReader {
public:
    QueryReader(odbcbridge::OdbcBridge::Stub *stub,
                const std::string &sql,
                std::optional<int32_t> limit) {
        odbcbridge::QueryRequest request;
        request.set_sql(sql);
        if (limit.has_value()) {
            request.set_limit(limit.value());
        }
        
        reader_ = stub->Query(&context_, request);
        
        // First message should be schema
        odbcbridge::QueryResponse response;
        if (reader_->Read(&response) && response.has_schema()) {
            for (const auto &col : response.schema().columns()) {
                schema_.push_back({
                    col.name(),
                    col.type_name(), 
                    col.odbc_type(),
                    col.size(),
                    col.decimal_digits(),
                    col.nullable()
                });
            }
        }
    }
    
    const std::vector<ColumnInfo> &GetSchema() const { return schema_; }
    
    // Read next batch of rows, returns false when done
    bool ReadBatch(std::vector<std::vector<Value>> &rows) {
        odbcbridge::QueryResponse response;
        if (!reader_->Read(&response)) {
            auto status = reader_->Finish();
            if (!status.ok()) {
                throw IOException("Query failed: " + status.error_message());
            }
            return false;
        }
        
        if (!response.has_rows()) {
            return ReadBatch(rows); // Skip non-row messages
        }
        
        rows.clear();
        for (const auto &row : response.rows().rows()) {
            std::vector<Value> values;
            for (int i = 0; i < row.values_size(); i++) {
                values.push_back(MapValue(row.values(i), schema_[i]));
            }
            rows.push_back(std::move(values));
        }
        return true;
    }
    
private:
    grpc::ClientContext context_;
    std::unique_ptr<grpc::ClientReader<odbcbridge::QueryResponse>> reader_;
    std::vector<ColumnInfo> schema_;
};

} // namespace duckdb
```

### Table Functions (table_functions.cpp)

```cpp
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "grpc_client.hpp"

namespace duckdb {

// ============================================
// dbisam_tables() - List available tables
// ============================================

struct DbiasmTablesData : public TableFunctionData {
    std::vector<std::string> tables;
    idx_t offset = 0;
};

static unique_ptr<FunctionData> DbiasmTablesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {
    
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("table_name");
    
    auto result = make_uniq<DbiasmTablesData>();
    
    // Connect and fetch table list
    auto &config = DBConfig::GetConfig(context);
    auto host = config.GetOption("odbcbridge_host").GetValue<string>();
    auto port = config.GetOption("odbcbridge_port").GetValue<int32_t>();
    
    OdbcBridgeClient client(host, port);
    result->tables = client.ListTables();
    
    return std::move(result);
}

static void DbiasmTablesExecute(
    ClientContext &context,
    TableFunctionInput &input,
    DataChunk &output) {
    
    auto &data = input.bind_data->Cast<DbiasmTablesData>();
    
    idx_t count = 0;
    while (data.offset < data.tables.size() && count < STANDARD_VECTOR_SIZE) {
        output.SetValue(0, count, Value(data.tables[data.offset]));
        data.offset++;
        count++;
    }
    output.SetCardinality(count);
}

void RegisterDbiasmTablesFunction(DatabaseInstance &db) {
    TableFunction func("dbisam_tables", {}, DbiasmTablesExecute, DbiasmTablesBind);
    ExtensionUtil::RegisterFunction(db, func);
}

// ============================================
// dbisam_describe(table_name) - Table schema
// ============================================

struct DbiasmDescribeData : public TableFunctionData {
    std::vector<ColumnInfo> columns;
    idx_t offset = 0;
};

static unique_ptr<FunctionData> DbiasmDescribeBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {
    
    return_types.push_back(LogicalType::VARCHAR);  // column_name
    return_types.push_back(LogicalType::VARCHAR);  // type_name
    return_types.push_back(LogicalType::INTEGER);  // size
    return_types.push_back(LogicalType::INTEGER);  // decimal_digits
    return_types.push_back(LogicalType::BOOLEAN);  // nullable
    
    names = {"column_name", "type_name", "size", "decimal_digits", "nullable"};
    
    auto table_name = input.inputs[0].GetValue<string>();
    
    auto &config = DBConfig::GetConfig(context);
    auto host = config.GetOption("odbcbridge_host").GetValue<string>();
    auto port = config.GetOption("odbcbridge_port").GetValue<int32_t>();
    
    OdbcBridgeClient client(host, port);
    
    auto result = make_uniq<DbiasmDescribeData>();
    result->columns = client.DescribeTable(table_name);
    
    return std::move(result);
}

static void DbiasmDescribeExecute(
    ClientContext &context,
    TableFunctionInput &input,
    DataChunk &output) {
    
    auto &data = input.bind_data->Cast<DbiasmDescribeData>();
    
    idx_t count = 0;
    while (data.offset < data.columns.size() && count < STANDARD_VECTOR_SIZE) {
        auto &col = data.columns[data.offset];
        output.SetValue(0, count, Value(col.name));
        output.SetValue(1, count, Value(col.type_name));
        output.SetValue(2, count, Value::INTEGER(col.size));
        output.SetValue(3, count, Value::INTEGER(col.decimal_digits));
        output.SetValue(4, count, Value::BOOLEAN(col.nullable));
        data.offset++;
        count++;
    }
    output.SetCardinality(count);
}

void RegisterDbiasmDescribeFunction(DatabaseInstance &db) {
    TableFunction func("dbisam_describe", {LogicalType::VARCHAR}, 
                       DbiasmDescribeExecute, DbiasmDescribeBind);
    ExtensionUtil::RegisterFunction(db, func);
}

// ============================================
// dbisam_query(sql, [limit]) - Execute query
// ============================================

struct DbiasmQueryData : public TableFunctionData {
    std::unique_ptr<OdbcBridgeClient> client;
    std::unique_ptr<QueryReader> reader;
    std::vector<std::vector<Value>> current_batch;
    idx_t batch_offset = 0;
    bool finished = false;
};

static unique_ptr<FunctionData> DbiasmQueryBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {
    
    auto sql = input.inputs[0].GetValue<string>();
    std::optional<int32_t> limit;
    if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
        limit = input.inputs[1].GetValue<int32_t>();
    }
    
    auto &config = DBConfig::GetConfig(context);
    auto host = config.GetOption("odbcbridge_host").GetValue<string>();
    auto port = config.GetOption("odbcbridge_port").GetValue<int32_t>();
    
    auto result = make_uniq<DbiasmQueryData>();
    result->client = make_uniq<OdbcBridgeClient>(host, port);
    result->reader = result->client->Query(sql, limit);
    
    // Set return types based on schema
    for (const auto &col : result->reader->GetSchema()) {
        return_types.push_back(MapOdbcTypeToDuckDB(col.odbc_type));
        names.push_back(col.name);
    }
    
    return std::move(result);
}

static void DbiasmQueryExecute(
    ClientContext &context,
    TableFunctionInput &input,
    DataChunk &output) {
    
    auto &data = input.bind_data->Cast<DbiasmQueryData>();
    
    if (data.finished) {
        output.SetCardinality(0);
        return;
    }
    
    idx_t count = 0;
    while (count < STANDARD_VECTOR_SIZE) {
        // Need more rows from current batch?
        if (data.batch_offset >= data.current_batch.size()) {
            if (!data.reader->ReadBatch(data.current_batch)) {
                data.finished = true;
                break;
            }
            data.batch_offset = 0;
        }
        
        // Copy row to output
        auto &row = data.current_batch[data.batch_offset];
        for (idx_t col = 0; col < row.size(); col++) {
            output.SetValue(col, count, row[col]);
        }
        
        data.batch_offset++;
        count++;
    }
    
    output.SetCardinality(count);
}

void RegisterDbiasmQueryFunction(DatabaseInstance &db) {
    // Version with just SQL
    TableFunction func1("dbisam_query", {LogicalType::VARCHAR},
                        DbiasmQueryExecute, DbiasmQueryBind);
    ExtensionUtil::RegisterFunction(db, func1);
    
    // Version with SQL and limit
    TableFunction func2("dbisam_query", {LogicalType::VARCHAR, LogicalType::INTEGER},
                        DbiasmQueryExecute, DbiasmQueryBind);
    ExtensionUtil::RegisterFunction(db, func2);
}

} // namespace duckdb
```

### Type Mapping (type_mapping.cpp)

```cpp
#include "type_mapping.hpp"
#include "duckdb.hpp"

namespace duckdb {

LogicalType MapOdbcTypeToDuckDB(int32_t odbc_type) {
    switch (odbc_type) {
        // Integer types
        case -6:  // SQL_TINYINT
        case 5:   // SQL_SMALLINT
            return LogicalType::SMALLINT;
        case 4:   // SQL_INTEGER
            return LogicalType::INTEGER;
        case -5:  // SQL_BIGINT
            return LogicalType::BIGINT;
            
        // Floating point
        case 7:   // SQL_REAL
            return LogicalType::FLOAT;
        case 6:   // SQL_FLOAT
        case 8:   // SQL_DOUBLE
            return LogicalType::DOUBLE;
            
        // Decimal - use VARCHAR to preserve precision
        case 2:   // SQL_NUMERIC
        case 3:   // SQL_DECIMAL
            return LogicalType::VARCHAR;  // Could use DECIMAL if we parse precision
            
        // Boolean
        case -7:  // SQL_BIT
            return LogicalType::BOOLEAN;
            
        // Binary
        case -2:  // SQL_BINARY
        case -3:  // SQL_VARBINARY
        case -4:  // SQL_LONGVARBINARY
            return LogicalType::BLOB;
            
        // Date/Time
        case 91:  // SQL_DATE / SQL_TYPE_DATE
            return LogicalType::DATE;
        case 92:  // SQL_TIME / SQL_TYPE_TIME
            return LogicalType::TIME;
        case 93:  // SQL_TIMESTAMP / SQL_TYPE_TIMESTAMP
            return LogicalType::TIMESTAMP;
            
        // Character types and everything else
        default:
            return LogicalType::VARCHAR;
    }
}

Value MapProtoValueToDuckDB(const odbcbridge::Value &proto_value,
                            const ColumnInfo &column_info) {
    if (proto_value.has_is_null() && proto_value.is_null()) {
        return Value();  // NULL
    }
    
    if (proto_value.has_int_value()) {
        return Value::BIGINT(proto_value.int_value());
    }
    
    if (proto_value.has_double_value()) {
        return Value::DOUBLE(proto_value.double_value());
    }
    
    if (proto_value.has_bool_value()) {
        return Value::BOOLEAN(proto_value.bool_value());
    }
    
    if (proto_value.has_bytes_value()) {
        auto &bytes = proto_value.bytes_value();
        return Value::BLOB(reinterpret_cast<const_data_ptr_t>(bytes.data()), 
                          bytes.size());
    }
    
    if (proto_value.has_string_value()) {
        const auto &str = proto_value.string_value();
        
        // Try to parse as date/time if that's what the column type suggests
        auto duckdb_type = MapOdbcTypeToDuckDB(column_info.odbc_type);
        
        if (duckdb_type == LogicalType::DATE) {
            return Value::DATE(Date::FromString(str));
        }
        if (duckdb_type == LogicalType::TIME) {
            return Value::TIME(Time::FromString(str));
        }
        if (duckdb_type == LogicalType::TIMESTAMP) {
            return Value::TIMESTAMP(Timestamp::FromString(str));
        }
        
        return Value(str);
    }
    
    return Value();  // Unknown type, return NULL
}

} // namespace duckdb
```

## Building

```bash
# Clone DuckDB extension template
git clone https://github.com/duckdb/extension-template.git duckdb-odbcbridge
cd duckdb-odbcbridge

# Replace template files with odbcbridge implementation
# ... copy sources ...

# Build
make release

# Output: build/release/extension/odbcbridge/odbcbridge.duckdb_extension
```

## Usage

```sql
-- Load extension
LOAD 'odbcbridge';

-- Configure connection to Windows service
SET odbcbridge_host = '192.168.1.100';
SET odbcbridge_port = 50051;

-- List tables
SELECT * FROM dbisam_tables();

-- Describe a table
SELECT * FROM dbisam_describe('customers');

-- Query data
SELECT * FROM dbisam_query('SELECT * FROM customers WHERE active = 1');

-- Query with limit
SELECT * FROM dbisam_query('SELECT * FROM orders', 100);

-- Use in joins with local DuckDB tables
SELECT c.name, o.total
FROM dbisam_query('SELECT * FROM customers') c
JOIN local_orders o ON c.id = o.customer_id;
```

## Error Handling

gRPC errors are translated to DuckDB exceptions:

- `UNAVAILABLE` → Connection error message
- `NOT_FOUND` → Table not found
- `INVALID_ARGUMENT` → SQL syntax error
- `INTERNAL` → Query execution error
