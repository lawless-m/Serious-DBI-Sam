#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Storage extension for ATTACH DBISAM
//===--------------------------------------------------------------------===//

class DbiasmStorageExtension : public StorageExtension {
public:
    DbiasmStorageExtension();
};

} // namespace duckdb
