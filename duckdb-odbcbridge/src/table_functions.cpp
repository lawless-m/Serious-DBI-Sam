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
    auto [host, port] = GetConnectionConfig(context);
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

    auto table_name = input.inputs[0].GetValue<std::string>();

    auto [host, port] = GetConnectionConfig(context);
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
    std::vector<ColumnInfo> schema;
    idx_t batch_offset = 0;
    bool finished = false;
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

    auto result = make_uniq<DbiasmQueryData>();
    result->client = make_uniq<OdbcBridgeClient>(host, port);
    result->reader = result->client->Query(sql, limit);
    result->schema = result->reader->GetSchema();

    // Set return types based on schema
    for (const auto &col : result->schema) {
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
