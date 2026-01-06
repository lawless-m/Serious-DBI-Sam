#include "dbisam_catalog.hpp"
#include "dbisam_scanner.hpp"
#include "type_mapping.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// DbiasmCatalog
//===--------------------------------------------------------------------===//

DbiasmCatalog::DbiasmCatalog(AttachedDatabase &db, const std::string &host, int port)
    : Catalog(db) {
    client_ = make_uniq<OdbcBridgeClient>(host, port);
}

DbiasmCatalog::~DbiasmCatalog() = default;

void DbiasmCatalog::Initialize(bool load_builtin) {
    CreateSchemaInfo info;
    info.schema = DEFAULT_SCHEMA;
    info.internal = true;
    main_schema_ = make_uniq<DbiasmSchemaEntry>(*this, info);
}

optional_ptr<CatalogEntry> DbiasmCatalog::CreateSchema(CatalogTransaction transaction,
                                                        CreateSchemaInfo &info) {
    throw BinderException("Cannot create schema in DBISAM catalog");
}

void DbiasmCatalog::ScanSchemas(ClientContext &context,
                                 std::function<void(SchemaCatalogEntry &)> callback) {
    callback(*main_schema_);
}

optional_ptr<SchemaCatalogEntry> DbiasmCatalog::GetSchema(CatalogTransaction transaction,
                                                           const std::string &schema_name,
                                                           OnEntryNotFound if_not_found,
                                                           QueryErrorContext error_context) {
    if (schema_name == DEFAULT_SCHEMA || schema_name == "dbisam" || schema_name.empty()) {
        return main_schema_.get();
    }
    if (if_not_found == OnEntryNotFound::RETURN_NULL) {
        return nullptr;
    }
    throw BinderException("Schema \"%s\" not found in DBISAM catalog", schema_name);
}

unique_ptr<PhysicalOperator> DbiasmCatalog::PlanInsert(ClientContext &context,
                                                        LogicalInsert &op,
                                                        unique_ptr<PhysicalOperator> plan) {
    throw BinderException("DBISAM catalog is read-only");
}

unique_ptr<PhysicalOperator> DbiasmCatalog::PlanCreateTableAs(ClientContext &context,
                                                               LogicalCreateTable &op,
                                                               unique_ptr<PhysicalOperator> plan) {
    throw BinderException("DBISAM catalog is read-only");
}

unique_ptr<PhysicalOperator> DbiasmCatalog::PlanDelete(ClientContext &context,
                                                        LogicalDelete &op,
                                                        unique_ptr<PhysicalOperator> plan) {
    throw BinderException("DBISAM catalog is read-only");
}

unique_ptr<PhysicalOperator> DbiasmCatalog::PlanUpdate(ClientContext &context,
                                                        LogicalUpdate &op,
                                                        unique_ptr<PhysicalOperator> plan) {
    throw BinderException("DBISAM catalog is read-only");
}

unique_ptr<LogicalOperator> DbiasmCatalog::BindCreateIndex(Binder &binder,
                                                            CreateStatement &stmt,
                                                            TableCatalogEntry &table,
                                                            unique_ptr<LogicalOperator> plan) {
    throw BinderException("DBISAM catalog is read-only");
}

DatabaseSize DbiasmCatalog::GetDatabaseSize(ClientContext &context) {
    return DatabaseSize();
}

vector<MetadataBlockInfo> DbiasmCatalog::GetMetadataInfo(ClientContext &context) {
    return {};
}

//===--------------------------------------------------------------------===//
// DbiasmSchemaEntry
//===--------------------------------------------------------------------===//

DbiasmSchemaEntry::DbiasmSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info) {
}

DbiasmCatalog &DbiasmSchemaEntry::GetDbiasmCatalog() {
    return catalog.Cast<DbiasmCatalog>();
}

void DbiasmSchemaEntry::LoadTables(ClientContext &context) {
    std::lock_guard<std::mutex> lock(tables_mutex_);
    if (tables_loaded_) {
        return;
    }

    auto &dbisam_catalog = GetDbiasmCatalog();
    auto &client = dbisam_catalog.GetClient();

    // Get table list
    auto table_names = client.ListTables();

    for (const auto &table_name : table_names) {
        // Get column info
        auto columns = client.DescribeTable(table_name);

        // Create table info
        CreateTableInfo info;
        info.table = table_name;
        info.schema = name;
        info.catalog = catalog.GetName();

        for (const auto &col : columns) {
            ColumnDefinition col_def(col.name, MapOdbcTypeToDuckDB(col.odbc_type));
            info.columns.AddColumn(std::move(col_def));
        }

        auto table_entry = make_uniq<DbiasmTableEntry>(catalog, *this, info, columns);
        tables_[table_name] = std::move(table_entry);
    }

    tables_loaded_ = true;
}

void DbiasmSchemaEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry &)> &callback) {
    if (type != CatalogType::TABLE_ENTRY) {
        return;
    }

    LoadTables(context);

    std::lock_guard<std::mutex> lock(tables_mutex_);
    for (auto &entry : tables_) {
        callback(*entry.second);
    }
}

void DbiasmSchemaEntry::Scan(CatalogType type,
                              const std::function<void(CatalogEntry &)> &callback) {
    // Can't load without context - skip
    if (type != CatalogType::TABLE_ENTRY) {
        return;
    }
    std::lock_guard<std::mutex> lock(tables_mutex_);
    for (auto &entry : tables_) {
        callback(*entry.second);
    }
}

optional_ptr<CatalogEntry> DbiasmSchemaEntry::GetEntry(CatalogTransaction transaction,
                                                        CatalogType type,
                                                        const std::string &name) {
    if (type != CatalogType::TABLE_ENTRY) {
        return nullptr;
    }

    // Try to load tables if we have a context
    if (transaction.context) {
        LoadTables(*transaction.context);
    }

    std::lock_guard<std::mutex> lock(tables_mutex_);
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        return it->second.get();
    }

    // Table not in cache - try to load it directly
    if (transaction.context) {
        auto &dbisam_catalog = GetDbiasmCatalog();
        auto &client = dbisam_catalog.GetClient();

        try {
            auto columns = client.DescribeTable(name);
            if (!columns.empty()) {
                CreateTableInfo info;
                info.table = name;
                info.schema = this->name;
                info.catalog = catalog.GetName();

                for (const auto &col : columns) {
                    ColumnDefinition col_def(col.name, MapOdbcTypeToDuckDB(col.odbc_type));
                    info.columns.AddColumn(std::move(col_def));
                }

                auto table_entry = make_uniq<DbiasmTableEntry>(catalog, *this, info, columns);
                auto result = table_entry.get();
                tables_[name] = std::move(table_entry);
                return result;
            }
        } catch (...) {
            // Table doesn't exist
        }
    }

    return nullptr;
}

optional_ptr<CatalogEntry> DbiasmSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                           BoundCreateTableInfo &info) {
    throw BinderException("DBISAM catalog is read-only");
}

optional_ptr<CatalogEntry> DbiasmSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                              CreateFunctionInfo &info) {
    throw BinderException("Cannot create functions in DBISAM catalog");
}

optional_ptr<CatalogEntry> DbiasmSchemaEntry::CreateIndex(CatalogTransaction transaction,
                                                           CreateIndexInfo &info,
                                                           TableCatalogEntry &table) {
    throw BinderException("DBISAM catalog is read-only");
}

optional_ptr<CatalogEntry> DbiasmSchemaEntry::CreateView(CatalogTransaction transaction,
                                                          CreateViewInfo &info) {
    throw BinderException("Cannot create views in DBISAM catalog");
}

optional_ptr<CatalogEntry> DbiasmSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                              CreateSequenceInfo &info) {
    throw BinderException("Cannot create sequences in DBISAM catalog");
}

optional_ptr<CatalogEntry> DbiasmSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                   CreateTableFunctionInfo &info) {
    throw BinderException("Cannot create table functions in DBISAM catalog");
}

optional_ptr<CatalogEntry> DbiasmSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                  CreateCopyFunctionInfo &info) {
    throw BinderException("Cannot create copy functions in DBISAM catalog");
}

optional_ptr<CatalogEntry> DbiasmSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                    CreatePragmaFunctionInfo &info) {
    throw BinderException("Cannot create pragma functions in DBISAM catalog");
}

optional_ptr<CatalogEntry> DbiasmSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                               CreateCollationInfo &info) {
    throw BinderException("Cannot create collations in DBISAM catalog");
}

optional_ptr<CatalogEntry> DbiasmSchemaEntry::CreateType(CatalogTransaction transaction,
                                                          CreateTypeInfo &info) {
    throw BinderException("Cannot create types in DBISAM catalog");
}

void DbiasmSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
    throw BinderException("DBISAM catalog is read-only");
}

void DbiasmSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
    throw BinderException("DBISAM catalog is read-only");
}

//===--------------------------------------------------------------------===//
// DbiasmTableEntry
//===--------------------------------------------------------------------===//

DbiasmTableEntry::DbiasmTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                   CreateTableInfo &info, std::vector<ColumnInfo> columns)
    : TableCatalogEntry(catalog, schema, info), dbisam_columns_(std::move(columns)) {
}

unique_ptr<BaseStatistics> DbiasmTableEntry::GetStatistics(ClientContext &context,
                                                            column_t column_id) {
    return nullptr;
}

TableFunction DbiasmTableEntry::GetScanFunction(ClientContext &context,
                                                 unique_ptr<FunctionData> &bind_data) {
    auto &dbisam_catalog = catalog.Cast<DbiasmCatalog>();

    // Get connection settings
    Value host_value, port_value;
    if (!context.TryGetCurrentSetting("odbcbridge_host", host_value)) {
        host_value = Value("localhost");
    }
    if (!context.TryGetCurrentSetting("odbcbridge_port", port_value)) {
        port_value = Value(50051);
    }

    // Create bind data
    auto data = make_uniq<DbiasmScanBindData>();
    data->table_name = name;
    data->columns = dbisam_columns_;
    data->host = host_value.GetValue<std::string>();
    data->port = port_value.GetValue<int32_t>();

    bind_data = std::move(data);

    return GetDbiasmScanFunction();
}

TableStorageInfo DbiasmTableEntry::GetStorageInfo(ClientContext &context) {
    TableStorageInfo info;
    info.cardinality = 0;  // Unknown
    return info;
}

} // namespace duckdb
