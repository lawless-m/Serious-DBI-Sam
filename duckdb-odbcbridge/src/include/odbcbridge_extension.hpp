#pragma once

#include "duckdb.hpp"

namespace duckdb {

class OdbcbridgeExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override;
};

// Table function registration
void RegisterDbiasmTablesFunction(DatabaseInstance &db);
void RegisterDbiasmDescribeFunction(DatabaseInstance &db);
void RegisterDbiasmQueryFunction(DatabaseInstance &db);

// Catalog registration
void RegisterDbiasmCatalog(DatabaseInstance &db);

} // namespace duckdb
