#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {

// Forward declarations
class Value;

// Column information from the service
struct ColumnInfo {
    std::string name;
    std::string type_name;
    int32_t odbc_type;
    int32_t size;
    int32_t decimal_digits;
    bool nullable;
};

// Forward declare the implementation class
class QueryReaderImpl;

// Streaming reader for query results
class QueryReader {
public:
    QueryReader(std::unique_ptr<QueryReaderImpl> impl);
    ~QueryReader();

    QueryReader(QueryReader &&other) noexcept;
    QueryReader &operator=(QueryReader &&other) noexcept;

    // Non-copyable
    QueryReader(const QueryReader &) = delete;
    QueryReader &operator=(const QueryReader &) = delete;

    const std::vector<ColumnInfo> &GetSchema() const;

    // Read next batch of rows, returns false when done
    bool ReadBatch(std::vector<std::vector<Value>> &rows);

private:
    std::unique_ptr<QueryReaderImpl> impl_;
};

// gRPC client for ODBC Bridge service
class OdbcBridgeClient {
public:
    OdbcBridgeClient(const std::string &host, int port);
    ~OdbcBridgeClient();

    // Non-copyable
    OdbcBridgeClient(const OdbcBridgeClient &) = delete;
    OdbcBridgeClient &operator=(const OdbcBridgeClient &) = delete;

    // Move constructors
    OdbcBridgeClient(OdbcBridgeClient &&other) noexcept;
    OdbcBridgeClient &operator=(OdbcBridgeClient &&other) noexcept;

    // List all tables
    std::vector<std::string> ListTables();

    // Describe a table's schema
    std::vector<ColumnInfo> DescribeTable(const std::string &table_name);

    // Execute a query and return a streaming reader
    std::unique_ptr<QueryReader> Query(const std::string &sql, std::optional<int32_t> limit);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace duckdb
