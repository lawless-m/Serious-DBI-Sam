#include "dbisam_storage.hpp"
#include "dbisam_catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Attach function for DBISAM
//===--------------------------------------------------------------------===//

static unique_ptr<Catalog> DbiasmAttach(StorageExtensionInfo *storage_info,
                                         ClientContext &context,
                                         AttachedDatabase &db,
                                         const string &name,
                                         AttachInfo &info,
                                         AccessMode access_mode) {
    // Parse connection options from attach info
    std::string host = "localhost";
    int port = 50051;

    for (auto &entry : info.options) {
        auto key = StringUtil::Lower(entry.first);
        if (key == "host") {
            host = entry.second.GetValue<std::string>();
        } else if (key == "port") {
            port = entry.second.GetValue<int32_t>();
        }
    }

    // Also check settings
    Value host_value, port_value;
    if (context.TryGetCurrentSetting("odbcbridge_host", host_value)) {
        host = host_value.GetValue<std::string>();
    }
    if (context.TryGetCurrentSetting("odbcbridge_port", port_value)) {
        port = port_value.GetValue<int32_t>();
    }

    return make_uniq<DbiasmCatalog>(db, host, port);
}

//===--------------------------------------------------------------------===//
// Storage Extension
//===--------------------------------------------------------------------===//

DbiasmStorageExtension::DbiasmStorageExtension() {
    attach = DbiasmAttach;
}

} // namespace duckdb
