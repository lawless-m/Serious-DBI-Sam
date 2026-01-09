#include "grpc_client.hpp"
#include "type_mapping.hpp"
#include "duckdb.hpp"

#include <grpcpp/grpcpp.h>
#include "odbcbridge.grpc.pb.h"

namespace duckdb {

// Implementation class for QueryReader
class QueryReaderImpl {
public:
    QueryReaderImpl(std::unique_ptr<odbcbridge::OdbcBridge::Stub> &stub,
                    const std::string &sql,
                    std::optional<int32_t> limit) {
        odbcbridge::QueryRequest request;
        request.set_sql(sql);
        if (limit.has_value()) {
            request.set_limit(limit.value());
        }

        reader_ = stub->Query(&context_, request);

        // First message should be schema
        odbcbridge::QueryResponse response;
        if (!reader_->Read(&response)) {
            auto status = reader_->Finish();
            throw IOException("Query failed to read first response: " + status.error_message() +
                            " (code: " + std::to_string(static_cast<int>(status.error_code())) + ")");
        }

        if (!response.has_schema()) {
            std::string got_type = response.has_rows() ? "rows" : "empty";
            throw IOException("Query response missing schema - got: " + got_type);
        }

        for (const auto &col : response.schema().columns()) {
            schema_.push_back({
                col.name(),
                col.type_name(),
                col.odbc_type(),
                col.size(),
                col.decimal_digits(),
                col.nullable()
            });
        }

        if (schema_.empty()) {
            throw IOException("Query returned empty schema (0 columns)");
        }
    }

    const std::vector<ColumnInfo> &GetSchema() const { return schema_; }

    bool ReadBatch(std::vector<std::vector<Value>> &rows) {
        odbcbridge::QueryResponse response;
        if (!reader_->Read(&response)) {
            auto status = reader_->Finish();
            if (!status.ok()) {
                throw IOException("Query failed: " + status.error_message());
            }
            return false;
        }

        if (!response.has_rows()) {
            return ReadBatch(rows); // Skip non-row messages
        }

        rows.clear();
        for (const auto &row : response.rows().rows()) {
            std::vector<Value> values;
            for (int i = 0; i < row.values_size(); i++) {
                values.push_back(MapProtoValueToDuckDB(row.values(i), schema_[i]));
            }
            rows.push_back(std::move(values));
        }
        return true;
    }

private:
    grpc::ClientContext context_;
    std::unique_ptr<grpc::ClientReader<odbcbridge::QueryResponse>> reader_;
    std::vector<ColumnInfo> schema_;
};

// QueryReader implementation
QueryReader::QueryReader(std::unique_ptr<QueryReaderImpl> impl)
    : impl_(std::move(impl)) {}

QueryReader::~QueryReader() = default;

QueryReader::QueryReader(QueryReader &&other) noexcept = default;
QueryReader &QueryReader::operator=(QueryReader &&other) noexcept = default;

const std::vector<ColumnInfo> &QueryReader::GetSchema() const {
    return impl_->GetSchema();
}

bool QueryReader::ReadBatch(std::vector<std::vector<Value>> &rows) {
    return impl_->ReadBatch(rows);
}

// Implementation class for OdbcBridgeClient
class OdbcBridgeClient::Impl {
public:
    Impl(const std::string &host, int port) {
        auto channel = grpc::CreateChannel(
            host + ":" + std::to_string(port),
            grpc::InsecureChannelCredentials());
        stub_ = odbcbridge::OdbcBridge::NewStub(channel);
    }

    std::vector<std::string> ListTables() {
        odbcbridge::ListTablesRequest request;
        odbcbridge::ListTablesResponse response;
        grpc::ClientContext context;

        auto status = stub_->ListTables(&context, request, &response);
        if (!status.ok()) {
            throw IOException("ListTables failed: " + status.error_message());
        }

        std::vector<std::string> tables;
        for (const auto &name : response.table_names()) {
            tables.push_back(name);
        }
        return tables;
    }

    std::vector<ColumnInfo> DescribeTable(const std::string &table_name) {
        odbcbridge::DescribeTableRequest request;
        request.set_table_name(table_name);

        odbcbridge::DescribeTableResponse response;
        grpc::ClientContext context;

        auto status = stub_->DescribeTable(&context, request, &response);
        if (!status.ok()) {
            throw IOException("DescribeTable failed: " + status.error_message());
        }

        std::vector<ColumnInfo> columns;
        for (const auto &col : response.columns()) {
            columns.push_back({
                col.name(),
                col.type_name(),
                col.odbc_type(),
                col.size(),
                col.decimal_digits(),
                col.nullable()
            });
        }
        return columns;
    }

    std::unique_ptr<QueryReader> Query(const std::string &sql, std::optional<int32_t> limit) {
        auto impl = std::make_unique<QueryReaderImpl>(stub_, sql, limit);
        return std::make_unique<QueryReader>(std::move(impl));
    }

    std::unique_ptr<odbcbridge::OdbcBridge::Stub> stub_;
};

// OdbcBridgeClient implementation
OdbcBridgeClient::OdbcBridgeClient(const std::string &host, int port)
    : impl_(std::make_unique<Impl>(host, port)) {}

OdbcBridgeClient::~OdbcBridgeClient() = default;

OdbcBridgeClient::OdbcBridgeClient(OdbcBridgeClient &&other) noexcept = default;
OdbcBridgeClient &OdbcBridgeClient::operator=(OdbcBridgeClient &&other) noexcept = default;

std::vector<std::string> OdbcBridgeClient::ListTables() {
    return impl_->ListTables();
}

std::vector<ColumnInfo> OdbcBridgeClient::DescribeTable(const std::string &table_name) {
    return impl_->DescribeTable(table_name);
}

std::unique_ptr<QueryReader> OdbcBridgeClient::Query(const std::string &sql, std::optional<int32_t> limit) {
    return impl_->Query(sql, limit);
}

} // namespace duckdb
