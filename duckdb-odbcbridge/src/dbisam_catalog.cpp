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
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
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
                case ExpressionType::COMPARE_BETWEEN:
                    // BETWEEN is syntactic sugar for >= AND <=
                    // DuckDB should decompose this, but handle it just in case
                    return false;
                default:
                    // Unsupported comparison type - filter locally
                    warning_issued = true;
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
                    warning_issued = true;
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
            // Unsupported filter type - filter locally
            warning_issued = true;
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
    idx_t row_id = 0;
    bool finished = false;
    bool rowid_scan = false;
    vector<string> projected_columns;

    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> DbiasmScanBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
    // This function should not be called when using the catalog approach.
    // The bind_data is provided directly by TableCatalogEntry::GetScanFunction().
    // If we reach here, it means the function was called directly which is not supported.
    throw NotImplementedException("dbisam_scan can only be used via the DBISAM catalog (ATTACH ... TYPE dbisam)");
}

static unique_ptr<GlobalTableFunctionState> DbiasmScanInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<DbiasmScanBindData>();
    auto state = make_uniq<DbiasmScanGlobalState>();

    // Build SQL query with only the projected columns
    std::string sql = "SELECT ";
    vector<string> projected_columns;

    // Track if we're doing a row-id scan (for COUNT(*) etc.)
    bool rowid_scan = false;

    // Use column_ids to determine which columns DuckDB wants
    if (!input.column_ids.empty()) {
        for (auto &col_id : input.column_ids) {
            // COLUMN_IDENTIFIER_ROW_ID = UINT64_MAX means DuckDB wants row IDs
            if (col_id == std::numeric_limits<column_t>::max()) {
                rowid_scan = true;
            } else if (col_id < bind_data.column_names.size()) {
                projected_columns.push_back(bind_data.column_names[col_id]);
            }
        }
    }

    // If only row-id scan, select first column for row counting
    if (projected_columns.empty() && !bind_data.column_names.empty()) {
        projected_columns.push_back(bind_data.column_names[0]);
    }

    // Store rowid_scan flag for later use
    state->rowid_scan = rowid_scan;

    // Build column list with quoted identifiers (handles special chars like ?)
    for (size_t i = 0; i < projected_columns.size(); i++) {
        if (i > 0) sql += ", ";
        sql += "\"" + projected_columns[i] + "\"";
    }
    sql += " FROM " + bind_data.table_name;

    if (!bind_data.filter_sql.empty()) {
        sql += " WHERE " + bind_data.filter_sql;
    }

    // Store projected columns for result mapping
    state->projected_columns = projected_columns;

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

        if (state.rowid_scan) {
            // For COUNT(*) etc., return row IDs
            output.SetValue(0, count, Value::BIGINT(state.row_id++));
        } else {
            // Return actual column data
            idx_t num_cols = output.ColumnCount();
            for (idx_t col = 0; col < num_cols && col < row.size(); col++) {
                output.SetValue(col, count, row[col]);
            }
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

// Helper to convert expression to SQL
static bool TryConvertExpressionToSQL(const Expression &expr, const vector<string> &column_names,
                                       string &sql_filter) {
    if (expr.type == ExpressionType::COMPARE_EQUAL ||
        expr.type == ExpressionType::COMPARE_NOTEQUAL ||
        expr.type == ExpressionType::COMPARE_LESSTHAN ||
        expr.type == ExpressionType::COMPARE_GREATERTHAN ||
        expr.type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
        expr.type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {

        auto &comp = expr.Cast<BoundComparisonExpression>();
        const Expression *col_expr = nullptr;
        const Expression *const_expr = nullptr;

        // Figure out which side is the column and which is the constant
        if (comp.left->type == ExpressionType::BOUND_COLUMN_REF &&
            comp.right->type == ExpressionType::VALUE_CONSTANT) {
            col_expr = comp.left.get();
            const_expr = comp.right.get();
        } else if (comp.right->type == ExpressionType::BOUND_COLUMN_REF &&
                   comp.left->type == ExpressionType::VALUE_CONSTANT) {
            col_expr = comp.right.get();
            const_expr = comp.left.get();
        } else {
            return false;
        }

        auto &col_ref = col_expr->Cast<BoundColumnRefExpression>();
        auto &const_val = const_expr->Cast<BoundConstantExpression>();

        idx_t col_idx = col_ref.binding.column_index;
        if (col_idx >= column_names.size()) return false;

        string op;
        switch (expr.type) {
            case ExpressionType::COMPARE_EQUAL: op = "="; break;
            case ExpressionType::COMPARE_NOTEQUAL: op = "!="; break;
            case ExpressionType::COMPARE_LESSTHAN: op = "<"; break;
            case ExpressionType::COMPARE_GREATERTHAN: op = ">"; break;
            case ExpressionType::COMPARE_LESSTHANOREQUALTO: op = "<="; break;
            case ExpressionType::COMPARE_GREATERTHANOREQUALTO: op = ">="; break;
            default: return false;
        }

        sql_filter = "\"" + column_names[col_idx] + "\" " + op + " " + ValueToSQL(const_val.value);
        return true;
    }
    return false;
}

static void DbiasmScanPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                            vector<unique_ptr<Expression>> &filters) {
    if (!bind_data_p) {
        return;
    }
    auto &bind_data = bind_data_p->Cast<DbiasmScanBindData>();
    vector<string> pushed_conditions;
    bool warning_issued = false;

    // Process table filters (simpler filters that DuckDB pre-processes)
    if (!get.table_filters.filters.empty()) {
        for (auto &entry : get.table_filters.filters) {
            idx_t col_idx = entry.first;
            auto &filter = *entry.second;

            if (col_idx < bind_data.column_names.size()) {
                string sql_filter;
                if (TryConvertFilter(filter, "\"" + bind_data.column_names[col_idx] + "\"", sql_filter, context, warning_issued)) {
                    pushed_conditions.push_back(sql_filter);
                } else {
                    bind_data.has_unpushed_filters = true;
                }
            }
        }
    }

    // Process expression filters
    for (idx_t i = 0; i < filters.size();) {
        string sql_filter;
        if (TryConvertExpressionToSQL(*filters[i], bind_data.column_names, sql_filter)) {
            pushed_conditions.push_back(sql_filter);
            // Remove the filter since we're handling it
            filters.erase(filters.begin() + i);
        } else {
            bind_data.has_unpushed_filters = true;
            i++;
        }
    }

    // Combine all pushed conditions
    if (!pushed_conditions.empty()) {
        bind_data.filter_sql = StringUtil::Join(pushed_conditions, " AND ");
    }
}

static TableFunction GetDbiasmScanFunction() {
    TableFunction func("dbisam_scan", {}, DbiasmScanFunc, DbiasmScanBind, DbiasmScanInit);
    func.filter_pushdown = true;
    func.projection_pushdown = true;
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
        // Fetch column info for this table
        auto columns = client_->DescribeTable(table_name);

        CreateTableInfo info;
        info.catalog = catalog.GetName();
        info.schema = name;
        info.table = table_name;

        // Add columns to the table info
        for (const auto &col : columns) {
            ColumnDefinition col_def(col.name, MapOdbcTypeToDuckDB(col.odbc_type));
            info.columns.AddColumn(std::move(col_def));
        }

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

// Read-only schema - these all throw NotImplementedException
optional_ptr<CatalogEntry> DbiasmSchema::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                      TableCatalogEntry &table) {
    throw NotImplementedException("DBISAM catalog is read-only");
}

optional_ptr<CatalogEntry> DbiasmSchema::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
    throw NotImplementedException("DBISAM catalog is read-only");
}

optional_ptr<CatalogEntry> DbiasmSchema::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
    throw NotImplementedException("DBISAM catalog is read-only");
}

optional_ptr<CatalogEntry> DbiasmSchema::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
    throw NotImplementedException("DBISAM catalog is read-only");
}

optional_ptr<CatalogEntry> DbiasmSchema::CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) {
    throw NotImplementedException("DBISAM catalog is read-only");
}

optional_ptr<CatalogEntry> DbiasmSchema::CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) {
    throw NotImplementedException("DBISAM catalog is read-only");
}

optional_ptr<CatalogEntry> DbiasmSchema::CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info) {
    throw NotImplementedException("DBISAM catalog is read-only");
}

optional_ptr<CatalogEntry> DbiasmSchema::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
    throw NotImplementedException("DBISAM catalog is read-only");
}

optional_ptr<CatalogEntry> DbiasmSchema::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
    throw NotImplementedException("DBISAM catalog is read-only");
}

void DbiasmSchema::DropEntry(ClientContext &context, DropInfo &info) {
    throw NotImplementedException("DBISAM catalog is read-only");
}

void DbiasmSchema::Alter(CatalogTransaction transaction, AlterInfo &info) {
    throw NotImplementedException("DBISAM catalog is read-only");
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

void DbiasmCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
    callback(*main_schema_);
}

optional_ptr<CatalogEntry> DbiasmCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
    throw NotImplementedException("DBISAM catalog does not support CREATE SCHEMA");
}

void DbiasmCatalog::DropSchema(ClientContext &context, DropInfo &info) {
    throw NotImplementedException("DBISAM catalog does not support DROP SCHEMA");
}

unique_ptr<PhysicalOperator> DbiasmCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                        unique_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("DBISAM catalog does not support DELETE");
}

unique_ptr<PhysicalOperator> DbiasmCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                        unique_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("DBISAM catalog does not support UPDATE");
}

// ============================================
// Read-only Transaction Manager
// ============================================

class DbiasmTransaction : public Transaction {
public:
    DbiasmTransaction(TransactionManager &manager, ClientContext &context)
        : Transaction(manager, context) {}
};

class DbiasmTransactionManager : public TransactionManager {
public:
    explicit DbiasmTransactionManager(AttachedDatabase &db) : TransactionManager(db) {}

    Transaction &StartTransaction(ClientContext &context) override {
        auto transaction = make_uniq<DbiasmTransaction>(*this, context);
        auto &result = *transaction;
        active_transactions.push_back(std::move(transaction));
        return result;
    }

    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override {
        // Read-only, nothing to commit
        return ErrorData();
    }

    void RollbackTransaction(Transaction &transaction) override {
        // Read-only, nothing to rollback
    }

    void Checkpoint(ClientContext &context, bool force) override {
        // Read-only, nothing to checkpoint
    }

private:
    vector<unique_ptr<Transaction>> active_transactions;
};

static unique_ptr<TransactionManager> DbiasmCreateTransactionManager(StorageExtensionInfo *storage_info,
                                                                      AttachedDatabase &db, Catalog &catalog) {
    return make_uniq<DbiasmTransactionManager>(db);
}

// ============================================
// Storage Extension for ATTACH support
// ============================================

struct DbiasmStorageExtensionInfo : public StorageExtensionInfo {
    // No additional info needed - connection details come from extension settings
};

static unique_ptr<Catalog> DbiasmAttach(StorageExtensionInfo *storage_info, ClientContext &context,
                                         AttachedDatabase &db, const string &name, AttachInfo &info,
                                         AccessMode access_mode) {
    // Get connection settings
    std::string host = "localhost";
    int32_t port = 50051;

    Value host_value, port_value;
    if (context.TryGetCurrentSetting("odbcbridge_host", host_value)) {
        host = host_value.GetValue<std::string>();
    }
    if (context.TryGetCurrentSetting("odbcbridge_port", port_value)) {
        port = port_value.GetValue<int32_t>();
    }

    // Check for overrides in attach options
    for (auto &entry : info.options) {
        auto lower_name = StringUtil::Lower(entry.first);
        if (lower_name == "host") {
            host = entry.second.GetValue<std::string>();
        } else if (lower_name == "port") {
            port = entry.second.GetValue<int32_t>();
        }
    }

    return make_uniq<DbiasmCatalog>(db, host, port);
}

// ============================================
// Catalog Registration
// ============================================

void RegisterDbiasmCatalog(DatabaseInstance &db) {
    auto &config = DBConfig::GetConfig(db);

    // Create and register the storage extension
    auto storage_ext = make_uniq<StorageExtension>();
    storage_ext->attach = DbiasmAttach;
    storage_ext->create_transaction_manager = DbiasmCreateTransactionManager;
    storage_ext->storage_info = make_shared_ptr<DbiasmStorageExtensionInfo>();

    config.storage_extensions["odbcbridge"] = std::move(storage_ext);
}

} // namespace duckdb
