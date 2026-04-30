#define DUCKDB_EXTENSION_MAIN

#include "odbcbridge_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
    auto &instance = loader.GetDatabaseInstance();
    auto &config = DBConfig::GetConfig(instance);

    config.AddExtensionOption(
        "odbcbridge_host",
        "Host address of the ODBC Bridge service",
        LogicalType::VARCHAR,
        Value("localhost"));

    config.AddExtensionOption(
        "odbcbridge_port",
        "Port of the ODBC Bridge service",
        LogicalType::INTEGER,
        Value(50051));

    config.AddExtensionOption(
        "odbcbridge_catalog_name",
        "Name of the catalog to register (default: dbisam)",
        LogicalType::VARCHAR,
        Value("dbisam"));

    RegisterDbiasmTablesFunction(loader);
    RegisterDbiasmDescribeFunction(loader);
    RegisterDbiasmQueryFunction(loader);

    RegisterDbiasmCatalog(instance);
}

void OdbcbridgeExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string OdbcbridgeExtension::Name() {
    return "odbcbridge";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(odbcbridge, loader) {
    duckdb::LoadInternal(loader);
}

}
