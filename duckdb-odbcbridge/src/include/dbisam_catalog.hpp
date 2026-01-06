#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "grpc_client.hpp"
#include <memory>
#include <string>

namespace duckdb {

class DbiasmCatalog;
class DbiasmSchema;

// Custom table entry for DBISAM tables
class DbiasmTableEntry : public TableCatalogEntry {
public:
    DbiasmTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                     CreateTableInfo &info, std::string table_name,
                     std::shared_ptr<OdbcBridgeClient> client);

    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
    TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
    std::string table_name_;
    std::shared_ptr<OdbcBridgeClient> client_;
};

// Custom schema for DBISAM
class DbiasmSchema : public SchemaCatalogEntry {
public:
    DbiasmSchema(Catalog &catalog, CreateSchemaInfo &info,
                 std::shared_ptr<OdbcBridgeClient> client);

    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
    optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;
    void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
    void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

private:
    std::shared_ptr<OdbcBridgeClient> client_;
    std::unordered_map<std::string, unique_ptr<DbiasmTableEntry>> tables_;
    bool tables_loaded_ = false;

    void LoadTables();
    void EnsureTablesLoaded();
};

// Custom catalog for DBISAM
class DbiasmCatalog : public Catalog {
public:
    explicit DbiasmCatalog(AttachedDatabase &db, const std::string &host, int port);

    string GetCatalogType() override { return "dbisam"; }
    void Initialize(bool load_builtin) override;
    optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, CatalogType type,
                                       const string &schema, const string &name) override;
    void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
    optional_ptr<SchemaCatalogEntry> GetSchema(CatalogTransaction transaction, const string &schema_name,
                                               OnEntryNotFound if_not_found, QueryErrorContext error_context = QueryErrorContext()) override;
    unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                    unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
                                            unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                 unique_ptr<LogicalOperator> plan) override;
    DatabaseSize GetDatabaseSize(ClientContext &context) override;
    vector<MetadataBlockInfo> GetMetadataInfo(ClientContext &context) override;

private:
    std::shared_ptr<OdbcBridgeClient> client_;
    unique_ptr<DbiasmSchema> main_schema_;
};

// Register the DBISAM catalog
void RegisterDbiasmCatalog(DatabaseInstance &db);

} // namespace duckdb
