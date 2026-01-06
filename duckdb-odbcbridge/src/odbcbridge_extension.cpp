#define DUCKDB_EXTENSION_MAIN

#include "odbcbridge_extension.hpp"
#include "dbisam_storage.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

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

    // Register table functions (legacy interface)
    RegisterDbiasmTablesFunction(instance);
    RegisterDbiasmDescribeFunction(instance);
    RegisterDbiasmQueryFunction(instance);

    // Register storage extension for ATTACH
    config.storage_extensions["dbisam"] = make_uniq<DbiasmStorageExtension>();
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
