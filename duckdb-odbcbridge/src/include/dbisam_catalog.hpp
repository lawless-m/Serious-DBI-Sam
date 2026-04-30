#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "grpc_client.hpp"
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

namespace duckdb {

class DbiasmCatalog;
class DbiasmSchema;
class PhysicalPlanGenerator;

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
    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction,
                                           const EntryLookupInfo &lookup_info) override;
    void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
    void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

    // Read-only catalog - these all throw NotImplementedException
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                           TableCatalogEntry &table) override;
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
    optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
    void DropEntry(ClientContext &context, DropInfo &info) override;
    void Alter(CatalogTransaction transaction, AlterInfo &info) override;

private:
    std::shared_ptr<OdbcBridgeClient> client_;
    // lowercase -> canonical-case name, for case-insensitive lookup
    std::unordered_map<std::string, std::string> table_names_;
    std::unordered_map<std::string, unique_ptr<DbiasmTableEntry>> tables_;
    bool names_loaded_ = false;
    std::mutex cache_mutex_;

    void EnsureNamesLoaded();
    // Returns nullptr if the table is in the listing but DescribeTable fails
    // (e.g. permission-denied tables that the remote refuses to open).
    DbiasmTableEntry *EnsureTableEntry(const std::string &name);
};

// Custom catalog for DBISAM
class DbiasmCatalog : public Catalog {
public:
    explicit DbiasmCatalog(AttachedDatabase &db, const std::string &host, int port);

    string GetCatalogType() override { return "dbisam"; }
    void Initialize(bool load_builtin) override;
    void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
    optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction,
                                                  const EntryLookupInfo &schema_lookup,
                                                  OnEntryNotFound if_not_found) override;
    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
    void DropSchema(ClientContext &context, DropInfo &info) override;
    PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                        LogicalCreateTable &op, PhysicalOperator &plan) override;
    PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                 optional_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                 PhysicalOperator &plan) override;
    PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                 PhysicalOperator &plan) override;
    unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                 unique_ptr<LogicalOperator> plan) override;
    DatabaseSize GetDatabaseSize(ClientContext &context) override;
    vector<MetadataBlockInfo> GetMetadataInfo(ClientContext &context) override;
    bool InMemory() override { return false; }
    string GetDBPath() override { return ""; }

private:
    std::shared_ptr<OdbcBridgeClient> client_;
    unique_ptr<DbiasmSchema> main_schema_;
};

// Register the DBISAM catalog
void RegisterDbiasmCatalog(DatabaseInstance &db);

} // namespace duckdb
