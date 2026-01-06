#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "grpc_client.hpp"

namespace duckdb {

class DbiasmCatalog;
class DbiasmSchemaEntry;
class DbiasmTableEntry;

//===--------------------------------------------------------------------===//
// DbiasmCatalog - The "dbisam" catalog
//===--------------------------------------------------------------------===//
class DbiasmCatalog : public Catalog {
public:
    explicit DbiasmCatalog(AttachedDatabase &db, const std::string &host, int port);
    ~DbiasmCatalog() override;

    std::string GetCatalogType() override { return "dbisam"; }

    void Initialize(bool load_builtin) override;

    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction,
                                            CreateSchemaInfo &info) override;

    void ScanSchemas(ClientContext &context,
                     std::function<void(SchemaCatalogEntry &)> callback) override;

    optional_ptr<SchemaCatalogEntry> GetSchema(CatalogTransaction transaction,
                                                const std::string &schema_name,
                                                OnEntryNotFound if_not_found,
                                                QueryErrorContext error_context = QueryErrorContext()) override;

    unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context,
                                            LogicalInsert &op,
                                            unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context,
                                                    LogicalCreateTable &op,
                                                    unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context,
                                            LogicalDelete &op,
                                            unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context,
                                            LogicalUpdate &op,
                                            unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder,
                                                CreateStatement &stmt,
                                                TableCatalogEntry &table,
                                                unique_ptr<LogicalOperator> plan) override;

    DatabaseSize GetDatabaseSize(ClientContext &context) override;
    vector<MetadataBlockInfo> GetMetadataInfo(ClientContext &context) override;

    bool InMemory() override { return false; }
    string GetDBPath() override { return ""; }

    OdbcBridgeClient &GetClient() { return *client_; }

private:
    unique_ptr<OdbcBridgeClient> client_;
    unique_ptr<DbiasmSchemaEntry> main_schema_;
};

//===--------------------------------------------------------------------===//
// DbiasmSchemaEntry - The "main" schema in dbisam catalog
//===--------------------------------------------------------------------===//
class DbiasmSchemaEntry : public SchemaCatalogEntry {
public:
    DbiasmSchemaEntry(Catalog &catalog, CreateSchemaInfo &info);

    void Scan(ClientContext &context, CatalogType type,
              const std::function<void(CatalogEntry &)> &callback) override;
    void Scan(CatalogType type,
              const std::function<void(CatalogEntry &)> &callback) override;

    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction,
                                           BoundCreateTableInfo &info) override;
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction,
                                              CreateFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction,
                                           CreateIndexInfo &info,
                                           TableCatalogEntry &table) override;
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction,
                                          CreateViewInfo &info) override;
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction,
                                              CreateSequenceInfo &info) override;
    optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
                                                   CreateTableFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
                                                  CreateCopyFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
                                                    CreatePragmaFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction,
                                               CreateCollationInfo &info) override;
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction,
                                          CreateTypeInfo &info) override;

    optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction,
                                        CatalogType type,
                                        const std::string &name) override;

    void DropEntry(ClientContext &context, DropInfo &info) override;
    void Alter(CatalogTransaction transaction, AlterInfo &info) override;

private:
    DbiasmCatalog &GetDbiasmCatalog();
    void LoadTables(ClientContext &context);

    std::mutex tables_mutex_;
    std::unordered_map<std::string, unique_ptr<DbiasmTableEntry>> tables_;
    bool tables_loaded_ = false;
};

//===--------------------------------------------------------------------===//
// DbiasmTableEntry - A table in the dbisam catalog
//===--------------------------------------------------------------------===//
class DbiasmTableEntry : public TableCatalogEntry {
public:
    DbiasmTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                     CreateTableInfo &info, std::vector<ColumnInfo> columns);

    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context,
                                             column_t column_id) override;

    TableFunction GetScanFunction(ClientContext &context,
                                  unique_ptr<FunctionData> &bind_data) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;

    const std::vector<ColumnInfo> &GetDbiasmColumns() const { return dbisam_columns_; }

private:
    std::vector<ColumnInfo> dbisam_columns_;
};

} // namespace duckdb
