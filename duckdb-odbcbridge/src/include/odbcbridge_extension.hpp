#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

class OdbcbridgeExtension : public Extension {
public:
    void Load(ExtensionLoader &loader) override;
    std::string Name() override;
};

// Table function registration
void RegisterDbiasmTablesFunction(ExtensionLoader &loader);
void RegisterDbiasmDescribeFunction(ExtensionLoader &loader);
void RegisterDbiasmQueryFunction(ExtensionLoader &loader);

// Catalog registration
void RegisterDbiasmCatalog(DatabaseInstance &db);

} // namespace duckdb
