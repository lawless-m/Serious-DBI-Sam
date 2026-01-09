#define DUCKDB_EXTENSION_MAIN

#include "odbcbridge_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
    // Register configuration settings
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

    // Register table functions
    RegisterDbiasmTablesFunction(instance);
    RegisterDbiasmDescribeFunction(instance);
    RegisterDbiasmQueryFunction(instance);

    // TODO: Virtual catalog needs work to compile against DuckDB 1.2.1 API
    // RegisterDbiasmCatalog(instance);
}

void OdbcbridgeExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string OdbcbridgeExtension::Name() {
    return "odbcbridge";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void odbcbridge_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::OdbcbridgeExtension>();
}

DUCKDB_EXTENSION_API const char *odbcbridge_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}
