#include "dbisam_catalog.hpp"
#include "grpc_client.hpp"
#include "type_mapping.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// ============================================
// Filter Pushdown Helpers
// ============================================

static std::string ValueToSQL(const Value &val) {
    if (val.IsNull()) {
        return "NULL";
    }

    switch (val.type().id()) {
        case LogicalTypeId::BOOLEAN:
            return val.GetValue<bool>() ? "1" : "0";
        case LogicalTypeId::TINYINT:
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT:
            return val.ToString();
        case LogicalTypeId::FLOAT:
        case LogicalTypeId::DOUBLE:
        case LogicalTypeId::DECIMAL:
            return val.ToString();
        case LogicalTypeId::VARCHAR:
            // Escape single quotes
            {
                auto str = val.ToString();
                std::string escaped;
                escaped.reserve(str.length() + 2);
                for (char c : str) {
                    if (c == '\'') {
                        escaped += "''";
                    } else {
                        escaped += c;
                    }
                }
                return "'" + escaped + "'";
            }
        case LogicalTypeId::DATE:
        case LogicalTypeId::TIMESTAMP:
            return "'" + val.ToString() + "'";
        default:
            return "'" + val.ToString() + "'";
    }
}

static bool TryConvertFilter(const TableFilter &filter, const string &column_name,
                             string &sql_filter, ClientContext &context, bool &warning_issued) {
    switch (filter.filter_type) {
        case TableFilterType::CONSTANT_COMPARISON: {
            auto &const_filter = filter.Cast<ConstantFilter>();
            string op;
            switch (const_filter.comparison_type) {
                case ExpressionType::COMPARE_EQUAL:
                    op = "=";
                    break;
                case ExpressionType::COMPARE_NOTEQUAL:
                    op = "!=";
                    break;
                case ExpressionType::COMPARE_LESSTHAN:
                    op = "<";
                    break;
                case ExpressionType::COMPARE_GREATERTHAN:
                    op = ">";
                    break;
                case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                    op = "<=";
                    break;
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                    op = ">=";
                    break;
                default:
                    if (!warning_issued) {
                        context.Explain(StringUtil::Format(
                            "WARNING: Filter on '%s' cannot be pushed down to DBISAM (unsupported comparison type). "
                            "This will fetch all rows and filter locally, which may be slow.",
                            column_name));
                        warning_issued = true;
                    }
                    return false;
            }
            sql_filter = column_name + " " + op + " " + ValueToSQL(const_filter.constant);
            return true;
        }

        case TableFilterType::IS_NULL:
            sql_filter = column_name + " IS NULL";
            return true;

        case TableFilterType::IS_NOT_NULL:
            sql_filter = column_name + " IS NOT NULL";
            return true;

        case TableFilterType::CONJUNCTION_AND: {
            auto &conj_filter = filter.Cast<ConjunctionAndFilter>();
            vector<string> conditions;
            for (auto &child_filter : conj_filter.child_filters) {
                string child_sql;
                if (TryConvertFilter(*child_filter, column_name, child_sql, context, warning_issued)) {
                    conditions.push_back(child_sql);
                } else {
                    return false;
                }
            }
            if (conditions.empty()) {
                return false;
            }
            sql_filter = "(" + StringUtil::Join(conditions, " AND ") + ")";
            return true;
        }

        case TableFilterType::CONJUNCTION_OR: {
            auto &conj_filter = filter.Cast<ConjunctionOrFilter>();
            vector<string> conditions;
            for (auto &child_filter : conj_filter.child_filters) {
                string child_sql;
                if (TryConvertFilter(*child_filter, column_name, child_sql, context, warning_issued)) {
                    conditions.push_back(child_sql);
                } else {
                    // If any OR condition can't be pushed, we can't push the whole OR
                    if (!warning_issued) {
                        context.Explain(StringUtil::Format(
                            "WARNING: Complex OR filter on '%s' cannot be fully pushed down to DBISAM. "
                            "This will fetch all rows and filter locally, which may be slow.",
                            column_name));
                        warning_issued = true;
                    }
                    return false;
                }
            }
            if (conditions.empty()) {
                return false;
            }
            sql_filter = "(" + StringUtil::Join(conditions, " OR ") + ")";
            return true;
        }

        default:
            if (!warning_issued) {
                context.Explain(StringUtil::Format(
                    "WARNING: Filter on '%s' (type: %s) cannot be pushed down to DBISAM. "
                    "This will fetch all rows and filter locally, which may be slow. "
                    "Consider using simple comparisons (=, !=, <, >, <=, >=) for better performance.",
                    column_name, TableFilterTypeToString(filter.filter_type)));
                warning_issued = true;
            }
            return false;
    }
}

// ============================================
// DBISAM Table Scan Function
// ============================================

struct DbiasmScanBindData : public TableFunctionData {
    std::shared_ptr<OdbcBridgeClient> client;
    std::string table_name;
    std::vector<ColumnInfo> schema;
    vector<string> column_names;
    vector<LogicalType> column_types;

    // Filter pushdown
    std::string filter_sql;
    std::optional<int32_t> limit;
    bool has_unpushed_filters = false;
};

struct DbiasmScanGlobalState : public GlobalTableFunctionState {
    std::unique_ptr<QueryReader> reader;
    std::vector<std::vector<Value>> current_batch;
    idx_t batch_offset = 0;
    bool finished = false;

    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> DbiasmScanBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<DbiasmScanBindData>();

    // Get bind data from table entry
    auto &table_bind_data = input.bind_data->Cast<DbiasmScanBindData>();
    bind_data->client = table_bind_data.client;
    bind_data->table_name = table_bind_data.table_name;
    bind_data->schema = table_bind_data.schema;
    bind_data->column_names = table_bind_data.column_names;
    bind_data->column_types = table_bind_data.column_types;

    return_types = bind_data->column_types;
    names = bind_data->column_names;

    return bind_data;
}

static unique_ptr<GlobalTableFunctionState> DbiasmScanInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<DbiasmScanBindData>();
    auto state = make_uniq<DbiasmScanGlobalState>();

    // Build SQL query
    std::string sql = "SELECT ";
    for (size_t i = 0; i < bind_data.column_names.size(); i++) {
        if (i > 0) sql += ", ";
        sql += bind_data.column_names[i];
    }
    sql += " FROM " + bind_data.table_name;

    if (!bind_data.filter_sql.empty()) {
        sql += " WHERE " + bind_data.filter_sql;
    }

    // Execute query
    state->reader = bind_data.client->Query(sql, bind_data.limit);

    return state;
}

static void DbiasmScanFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.global_state->Cast<DbiasmScanGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    while (count < STANDARD_VECTOR_SIZE) {
        // Need more rows?
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

static void DbiasmScanPushdownFilter(ClientContext &context, optional_ptr<FunctionData> bind_data_p,
                                     vector<unique_ptr<Expression>> &filters) {
    auto &bind_data = bind_data_p->Cast<DbiasmScanBindData>();
    vector<string> pushed_filters;
    bool warning_issued = false;

    // Try to push down each filter
    for (idx_t i = 0; i < filters.size();) {
        auto &filter = filters[i];

        // Only handle simple column references with table filters
        if (filter->type == ExpressionType::BOUND_COLUMN_REF) {
            auto &col_ref = filter->Cast<BoundColumnRefExpression>();
            idx_t col_idx = col_ref.binding.column_index;

            if (col_idx < bind_data.column_names.size()) {
                // This is a placeholder - actual filter logic happens in table filter pushdown
                // Just track that we have filters
                bind_data.has_unpushed_filters = true;
            }
        }
        i++;
    }
}

static unique_ptr<NodeStatistics> DbiasmScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    // We don't know the cardinality without executing the query
    return make_uniq<NodeStatistics>();
}

static void DbiasmScanPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData &bind_data_p,
                                            vector<unique_ptr<Expression>> &filters) {
    auto &bind_data = bind_data_p.Cast<DbiasmScanBindData>();
    vector<string> pushed_conditions;
    bool warning_issued = false;

    // Process table filters (simpler filters that DuckDB pre-processes)
    if (!get.table_filters.filters.empty()) {
        for (auto &entry : get.table_filters.filters) {
            idx_t col_idx = entry.first;
            auto &filter = *entry.second;

            if (col_idx < bind_data.column_names.size()) {
                string sql_filter;
                if (TryConvertFilter(filter, bind_data.column_names[col_idx], sql_filter, context, warning_issued)) {
                    pushed_conditions.push_back(sql_filter);
                } else {
                    bind_data.has_unpushed_filters = true;
                }
            }
        }
    }

    // Combine all pushed conditions
    if (!pushed_conditions.empty()) {
        bind_data.filter_sql = StringUtil::Join(pushed_conditions, " AND ");

        // Show info about successful pushdown
        context.Explain(StringUtil::Format(
            "INFO: Pushed filter to DBISAM: %s", bind_data.filter_sql));
    } else if (bind_data.has_unpushed_filters) {
        // We have filters but couldn't push any
        if (!warning_issued) {
            context.Explain(
                "WARNING: No filters could be pushed down to DBISAM. "
                "All data will be fetched and filtered locally. "
                "For better performance, use simple comparisons (=, !=, <, >, <=, >=, IS NULL, IS NOT NULL).");
        }
    }
}

static TableFunction GetDbiasmScanFunction() {
    TableFunction func("dbisam_scan", {}, DbiasmScanFunc, DbiasmScanBind, DbiasmScanInit);
    func.filter_pushdown = true;
    func.pushdown_complex_filter = DbiasmScanPushdownComplexFilter;
    func.cardinality = DbiasmScanCardinality;
    return func;
}

// ============================================
// DbiasmTableEntry Implementation
// ============================================

DbiasmTableEntry::DbiasmTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                   CreateTableInfo &info, std::string table_name,
                                   std::shared_ptr<OdbcBridgeClient> client)
    : TableCatalogEntry(catalog, schema, info), table_name_(table_name), client_(client) {
}

unique_ptr<BaseStatistics> DbiasmTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
    return nullptr;
}

TableFunction DbiasmTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    // Get schema from remote
    auto schema = client_->DescribeTable(table_name_);

    // Create bind data
    auto scan_bind_data = make_uniq<DbiasmScanBindData>();
    scan_bind_data->client = client_;
    scan_bind_data->table_name = table_name_;
    scan_bind_data->schema = schema;

    for (const auto &col : schema) {
        scan_bind_data->column_names.push_back(col.name);
        scan_bind_data->column_types.push_back(MapOdbcTypeToDuckDB(col.odbc_type));
    }

    bind_data = std::move(scan_bind_data);
    return GetDbiasmScanFunction();
}

TableStorageInfo DbiasmTableEntry::GetStorageInfo(ClientContext &context) {
    TableStorageInfo info;
    info.cardinality = 0; // Unknown
    return info;
}

// ============================================
// DbiasmSchema Implementation
// ============================================

DbiasmSchema::DbiasmSchema(Catalog &catalog, CreateSchemaInfo &info,
                           std::shared_ptr<OdbcBridgeClient> client)
    : SchemaCatalogEntry(catalog, info), client_(client) {
}

void DbiasmSchema::LoadTables() {
    if (tables_loaded_) {
        return;
    }

    auto table_names = client_->ListTables();

    for (const auto &table_name : table_names) {
        CreateTableInfo info;
        info.catalog = catalog.GetName();
        info.schema = name;
        info.table = table_name;

        auto table_entry = make_uniq<DbiasmTableEntry>(catalog, *this, info, table_name, client_);
        tables_[table_name] = std::move(table_entry);
    }

    tables_loaded_ = true;
}

void DbiasmSchema::EnsureTablesLoaded() {
    if (!tables_loaded_) {
        LoadTables();
    }
}

optional_ptr<CatalogEntry> DbiasmSchema::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
    throw NotImplementedException("DBISAM catalog does not support CREATE TABLE");
}

optional_ptr<CatalogEntry> DbiasmSchema::GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) {
    if (type != CatalogType::TABLE_ENTRY) {
        return nullptr;
    }

    EnsureTablesLoaded();

    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return nullptr;
    }

    return it->second.get();
}

void DbiasmSchema::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    if (type != CatalogType::TABLE_ENTRY) {
        return;
    }

    EnsureTablesLoaded();

    for (auto &entry : tables_) {
        callback(*entry.second);
    }
}

void DbiasmSchema::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    if (type != CatalogType::TABLE_ENTRY) {
        return;
    }

    EnsureTablesLoaded();

    for (auto &entry : tables_) {
        callback(*entry.second);
    }
}

// ============================================
// DbiasmCatalog Implementation
// ============================================

DbiasmCatalog::DbiasmCatalog(AttachedDatabase &db, const std::string &host, int port)
    : Catalog(db), client_(std::make_shared<OdbcBridgeClient>(host, port)) {
}

void DbiasmCatalog::Initialize(bool load_builtin) {
    CreateSchemaInfo info;
    info.schema = DEFAULT_SCHEMA;
    main_schema_ = make_uniq<DbiasmSchema>(*this, info, client_);
}

optional_ptr<CatalogEntry> DbiasmCatalog::GetEntry(CatalogTransaction transaction, CatalogType type,
                                                   const string &schema, const string &name) {
    if (schema != DEFAULT_SCHEMA) {
        return nullptr;
    }

    return main_schema_->GetEntry(transaction, type, name);
}

void DbiasmCatalog::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    main_schema_->Scan(context, type, callback);
}

optional_ptr<SchemaCatalogEntry> DbiasmCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                          OnEntryNotFound if_not_found, QueryErrorContext error_context) {
    if (schema_name == DEFAULT_SCHEMA) {
        return main_schema_.get();
    }

    if (if_not_found == OnEntryNotFound::RETURN_NULL) {
        return nullptr;
    }

    throw CatalogException("Schema \"%s\" not found in DBISAM catalog", schema_name);
}

unique_ptr<PhysicalOperator> DbiasmCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                               unique_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("DBISAM catalog does not support CREATE TABLE AS");
}

unique_ptr<PhysicalOperator> DbiasmCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                        unique_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("DBISAM catalog does not support INSERT");
}

unique_ptr<LogicalOperator> DbiasmCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                            TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
    throw NotImplementedException("DBISAM catalog does not support CREATE INDEX");
}

DatabaseSize DbiasmCatalog::GetDatabaseSize(ClientContext &context) {
    DatabaseSize size;
    size.bytes = 0;
    size.block_size = 0;
    size.total_blocks = 0;
    size.used_blocks = 0;
    size.free_blocks = 0;
    size.wal_size = 0;
    return size;
}

vector<MetadataBlockInfo> DbiasmCatalog::GetMetadataInfo(ClientContext &context) {
    return {};
}

// ============================================
// Catalog Registration
// ============================================

void RegisterDbiasmCatalog(DatabaseInstance &db) {
    auto &config = DBConfig::GetConfig(db);

    // Get connection configuration
    std::string host = "localhost";
    int port = 50051;
    std::string catalog_name = "dbisam";

    Value host_value, port_value, catalog_name_value;
    if (config.options.default_extension_options.find("odbcbridge_host") != config.options.default_extension_options.end()) {
        host_value = config.options.default_extension_options["odbcbridge_host"];
        host = host_value.GetValue<std::string>();
    }

    if (config.options.default_extension_options.find("odbcbridge_port") != config.options.default_extension_options.end()) {
        port_value = config.options.default_extension_options["odbcbridge_port"];
        port = port_value.GetValue<int32_t>();
    }

    if (config.options.default_extension_options.find("odbcbridge_catalog_name") != config.options.default_extension_options.end()) {
        catalog_name_value = config.options.default_extension_options["odbcbridge_catalog_name"];
        catalog_name = catalog_name_value.GetValue<std::string>();
    }

    // Attach the DBISAM catalog
    auto &catalog_manager = Catalog::GetSystemCatalog(db);

    // Create attach info
    AttachInfo attach_info;
    attach_info.name = catalog_name;
    attach_info.options["type"] = Value("dbisam");

    // Create the catalog
    auto attached_db = make_uniq<AttachedDatabase>(db, attach_info);
    auto dbisam_catalog = make_uniq<DbiasmCatalog>(*attached_db, host, port);

    // Register the catalog
    catalog_manager.RegisterCatalog(std::move(dbisam_catalog));
}

} // namespace duckdb
