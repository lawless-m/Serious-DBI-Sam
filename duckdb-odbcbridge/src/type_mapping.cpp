#include "type_mapping.hpp"
#include "duckdb.hpp"
#include "odbcbridge.pb.h"

namespace duckdb {

LogicalType MapOdbcTypeToDuckDB(int32_t odbc_type) {
    switch (odbc_type) {
        // Integer types
        case -6:  // SQL_TINYINT
        case 5:   // SQL_SMALLINT
            return LogicalType::SMALLINT;
        case 4:   // SQL_INTEGER
            return LogicalType::INTEGER;
        case -5:  // SQL_BIGINT
            return LogicalType::BIGINT;

        // Floating point
        case 7:   // SQL_REAL
            return LogicalType::FLOAT;
        case 6:   // SQL_FLOAT
        case 8:   // SQL_DOUBLE
            return LogicalType::DOUBLE;

        // Decimal - use VARCHAR to preserve precision
        case 2:   // SQL_NUMERIC
        case 3:   // SQL_DECIMAL
            return LogicalType::VARCHAR;  // Could use DECIMAL if we parse precision

        // Boolean
        case -7:  // SQL_BIT
            return LogicalType::BOOLEAN;

        // Binary
        case -2:  // SQL_BINARY
        case -3:  // SQL_VARBINARY
        case -4:  // SQL_LONGVARBINARY
            return LogicalType::BLOB;

        // Date/Time
        case 91:  // SQL_DATE / SQL_TYPE_DATE
            return LogicalType::DATE;
        case 92:  // SQL_TIME / SQL_TYPE_TIME
            return LogicalType::TIME;
        case 93:  // SQL_TIMESTAMP / SQL_TYPE_TIMESTAMP
            return LogicalType::TIMESTAMP;

        // Character types and everything else
        default:
            return LogicalType::VARCHAR;
    }
}

Value MapProtoValueToDuckDB(const odbcbridge::Value &proto_value,
                            const ColumnInfo &column_info) {
    // Check for null first
    if (proto_value.has_is_null() && proto_value.is_null()) {
        return Value();  // NULL
    }

    if (proto_value.has_int_value()) {
        return Value::BIGINT(proto_value.int_value());
    }

    if (proto_value.has_double_value()) {
        return Value::DOUBLE(proto_value.double_value());
    }

    if (proto_value.has_bool_value()) {
        return Value::BOOLEAN(proto_value.bool_value());
    }

    if (proto_value.has_bytes_value()) {
        auto &bytes = proto_value.bytes_value();
        return Value::BLOB(reinterpret_cast<const_data_ptr_t>(bytes.data()),
                          bytes.size());
    }

    if (proto_value.has_string_value()) {
        const auto &str = proto_value.string_value();

        // Try to parse as date/time if that's what the column type suggests
        auto duckdb_type = MapOdbcTypeToDuckDB(column_info.odbc_type);

        if (duckdb_type == LogicalType::DATE) {
            try {
                return Value::DATE(Date::FromString(str));
            } catch (...) {
                return Value(str);
            }
        }
        if (duckdb_type == LogicalType::TIME) {
            try {
                return Value::TIME(Time::FromString(str));
            } catch (...) {
                return Value(str);
            }
        }
        if (duckdb_type == LogicalType::TIMESTAMP) {
            try {
                return Value::TIMESTAMP(Timestamp::FromString(str));
            } catch (...) {
                return Value(str);
            }
        }

        return Value(str);
    }

    return Value();  // Unknown type, return NULL
}

} // namespace duckdb
