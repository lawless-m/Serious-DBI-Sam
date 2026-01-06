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
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

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

static TableFunction GetDbiasmScanFunction() {
    TableFunction func("dbisam_scan", {}, DbiasmScanFunc, DbiasmScanBind, DbiasmScanInit);
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
