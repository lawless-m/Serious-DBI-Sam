#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "grpc_client.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Bind data for DBISAM table scans
//===--------------------------------------------------------------------===//
struct DbiasmScanBindData : public TableFunctionData {
    std::string table_name;
    std::vector<ColumnInfo> columns;
    std::vector<idx_t> column_ids;       // Projected column indices
    std::vector<std::string> filters;    // SQL WHERE conditions

    std::string host;
    int port;
};

//===--------------------------------------------------------------------===//
// Global state for DBISAM table scans
//===--------------------------------------------------------------------===//
struct DbiasmScanGlobalState : public GlobalTableFunctionState {
    std::unique_ptr<OdbcBridgeClient> client;
    std::unique_ptr<QueryReader> reader;
    std::vector<std::vector<Value>> current_batch;
    idx_t batch_offset = 0;
    bool finished = false;
    std::mutex lock;

    idx_t MaxThreads() const override { return 1; }
};

//===--------------------------------------------------------------------===//
// Local state for DBISAM table scans
//===--------------------------------------------------------------------===//
struct DbiasmScanLocalState : public LocalTableFunctionState {
    // Currently single-threaded, no local state needed
};

//===--------------------------------------------------------------------===//
// Scanner functions
//===--------------------------------------------------------------------===//

// Build SQL query from bind data
std::string BuildDbiasmQuery(const DbiasmScanBindData &bind_data);

// Format a DuckDB Value for SQL
std::string FormatValueForSQL(const Value &val);

// Table function implementations
unique_ptr<FunctionData> DbiasmScanBind(ClientContext &context,
                                         TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types,
                                         vector<string> &names);

unique_ptr<GlobalTableFunctionState> DbiasmScanInitGlobal(ClientContext &context,
                                                           TableFunctionInitInput &input);

unique_ptr<LocalTableFunctionState> DbiasmScanInitLocal(ExecutionContext &context,
                                                         TableFunctionInitInput &input,
                                                         GlobalTableFunctionState *global_state);

void DbiasmScanExecute(ClientContext &context,
                       TableFunctionInput &input,
                       DataChunk &output);

// Filter pushdown
void DbiasmFilterPushdown(ClientContext &context,
                          optional_ptr<FunctionData> bind_data,
                          vector<unique_ptr<Expression>> &filters);

// Get the table function for DBISAM scans
TableFunction GetDbiasmScanFunction();

} // namespace duckdb
