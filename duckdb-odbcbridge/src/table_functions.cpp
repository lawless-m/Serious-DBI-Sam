#include "odbcbridge_extension.hpp"
#include "grpc_client.hpp"
#include "type_mapping.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

// Helper to get configuration values
static std::pair<std::string, int32_t> GetConnectionConfig(ClientContext &context) {
    Value host_value, port_value;

    if (context.TryGetCurrentSetting("odbcbridge_host", host_value)) {
        // Setting exists
    } else {
        host_value = Value("localhost");
    }

    if (context.TryGetCurrentSetting("odbcbridge_port", port_value)) {
        // Setting exists
    } else {
        port_value = Value(50051);
    }

    return {host_value.GetValue<std::string>(), port_value.GetValue<int32_t>()};
}

// ============================================
// dbisam_tables() - List available tables
// ============================================

struct DbiasmTablesBindData : public TableFunctionData {
    std::vector<std::string> tables;
};

struct DbiasmTablesState : public GlobalTableFunctionState {
    idx_t offset = 0;
};

static unique_ptr<FunctionData> DbiasmTablesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("table_name");

    auto result = make_uniq<DbiasmTablesBindData>();

    // Connect and fetch table list
    auto [host, port] = GetConnectionConfig(context);
    OdbcBridgeClient client(host, port);
    result->tables = client.ListTables();

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DbiasmTablesInitGlobal(
    ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<DbiasmTablesState>();
}

static void DbiasmTablesExecute(
    ClientContext &context,
    TableFunctionInput &input,
    DataChunk &output) {

    auto &bind_data = input.bind_data->Cast<DbiasmTablesBindData>();
    auto &state = input.global_state->Cast<DbiasmTablesState>();

    idx_t count = 0;
    while (state.offset < bind_data.tables.size() && count < STANDARD_VECTOR_SIZE) {
        output.SetValue(0, count, Value(bind_data.tables[state.offset]));
        state.offset++;
        count++;
    }
    output.SetCardinality(count);
}

void RegisterDbiasmTablesFunction(DatabaseInstance &db) {
    TableFunction func("dbisam_tables", {}, DbiasmTablesExecute, DbiasmTablesBind);
    func.init_global = DbiasmTablesInitGlobal;
    ExtensionUtil::RegisterFunction(db, func);
}

// ============================================
// dbisam_describe(table_name) - Table schema
// ============================================

struct DbiasmDescribeBindData : public TableFunctionData {
    std::vector<ColumnInfo> columns;
};

struct DbiasmDescribeState : public GlobalTableFunctionState {
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

    auto table_name = input.inputs[0].GetValue<std::string>();

    auto [host, port] = GetConnectionConfig(context);
    OdbcBridgeClient client(host, port);

    auto result = make_uniq<DbiasmDescribeBindData>();
    result->columns = client.DescribeTable(table_name);

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DbiasmDescribeInitGlobal(
    ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<DbiasmDescribeState>();
}

static void DbiasmDescribeExecute(
    ClientContext &context,
    TableFunctionInput &input,
    DataChunk &output) {

    auto &bind_data = input.bind_data->Cast<DbiasmDescribeBindData>();
    auto &state = input.global_state->Cast<DbiasmDescribeState>();

    idx_t count = 0;
    while (state.offset < bind_data.columns.size() && count < STANDARD_VECTOR_SIZE) {
        auto &col = bind_data.columns[state.offset];
        output.SetValue(0, count, Value(col.name));
        output.SetValue(1, count, Value(col.type_name));
        output.SetValue(2, count, Value::INTEGER(col.size));
        output.SetValue(3, count, Value::INTEGER(col.decimal_digits));
        output.SetValue(4, count, Value::BOOLEAN(col.nullable));
        state.offset++;
        count++;
    }
    output.SetCardinality(count);
}

void RegisterDbiasmDescribeFunction(DatabaseInstance &db) {
    TableFunction func("dbisam_describe", {LogicalType::VARCHAR},
                       DbiasmDescribeExecute, DbiasmDescribeBind);
    func.init_global = DbiasmDescribeInitGlobal;
    ExtensionUtil::RegisterFunction(db, func);
}

// ============================================
// dbisam_query(sql, [limit]) - Execute query
// ============================================

struct DbiasmQueryBindData : public TableFunctionData {
    std::string sql;
    std::optional<int32_t> limit;
    std::string host;
    int32_t port;
    std::vector<ColumnInfo> schema;
};

struct DbiasmQueryState : public GlobalTableFunctionState {
    std::unique_ptr<OdbcBridgeClient> client;
    std::unique_ptr<QueryReader> reader;
    std::vector<std::vector<Value>> current_batch;
    idx_t batch_offset = 0;
    bool finished = false;
    bool initialized = false;
};

static unique_ptr<FunctionData> DbiasmQueryBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    auto sql = input.inputs[0].GetValue<std::string>();
    std::optional<int32_t> limit;
    if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
        limit = input.inputs[1].GetValue<int32_t>();
    }

    auto [host, port] = GetConnectionConfig(context);

    auto result = make_uniq<DbiasmQueryBindData>();
    result->sql = sql;
    result->limit = limit;
    result->host = host;
    result->port = port;

    // We need to query to get schema at bind time
    OdbcBridgeClient client(host, port);
    auto reader = client.Query(sql, limit);
    result->schema = reader->GetSchema();

    // Set return types based on schema
    for (const auto &col : result->schema) {
        return_types.push_back(MapOdbcTypeToDuckDB(col.odbc_type));
        names.push_back(col.name);
    }

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DbiasmQueryInitGlobal(
    ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<DbiasmQueryState>();
}

static void DbiasmQueryExecute(
    ClientContext &context,
    TableFunctionInput &input,
    DataChunk &output) {

    auto &bind_data = input.bind_data->Cast<DbiasmQueryBindData>();
    auto &state = input.global_state->Cast<DbiasmQueryState>();

    // Initialize connection on first call
    if (!state.initialized) {
        state.client = make_uniq<OdbcBridgeClient>(bind_data.host, bind_data.port);
        state.reader = state.client->Query(bind_data.sql, bind_data.limit);
        state.initialized = true;
    }

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    while (count < STANDARD_VECTOR_SIZE) {
        // Need more rows from current batch?
        if (state.batch_offset >= state.current_batch.size()) {
            if (!state.reader->ReadBatch(state.current_batch)) {
                state.finished = true;
                break;
            }
            state.batch_offset = 0;
        }

        // Copy row to output
        auto &row = state.current_batch[state.batch_offset];
        for (idx_t col = 0; col < row.size(); col++) {
            output.SetValue(col, count, row[col]);
        }

        state.batch_offset++;
        count++;
    }

    output.SetCardinality(count);
}

void RegisterDbiasmQueryFunction(DatabaseInstance &db) {
    // Version with just SQL
    TableFunction func1("dbisam_query", {LogicalType::VARCHAR},
                        DbiasmQueryExecute, DbiasmQueryBind);
    func1.init_global = DbiasmQueryInitGlobal;
    ExtensionUtil::RegisterFunction(db, func1);

    // Version with SQL and limit
    TableFunction func2("dbisam_query", {LogicalType::VARCHAR, LogicalType::INTEGER},
                        DbiasmQueryExecute, DbiasmQueryBind);
    func2.init_global = DbiasmQueryInitGlobal;
    ExtensionUtil::RegisterFunction(db, func2);
}

} // namespace duckdb
