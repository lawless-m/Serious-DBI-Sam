#include "dbisam_scanner.hpp"
#include "type_mapping.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// SQL Building
//===--------------------------------------------------------------------===//

std::string FormatValueForSQL(const Value &val) {
    if (val.IsNull()) {
        return "NULL";
    }

    switch (val.type().id()) {
        case LogicalTypeId::VARCHAR:
        case LogicalTypeId::CHAR: {
            // Escape single quotes by doubling them
            auto str = val.GetValue<std::string>();
            std::string escaped;
            escaped.reserve(str.size() + 2);
            escaped += '\'';
            for (char c : str) {
                if (c == '\'') {
                    escaped += "''";
                } else {
                    escaped += c;
                }
            }
            escaped += '\'';
            return escaped;
        }
        case LogicalTypeId::BOOLEAN:
            return val.GetValue<bool>() ? "TRUE" : "FALSE";
        case LogicalTypeId::DATE:
            return "'" + val.ToString() + "'";
        case LogicalTypeId::TIME:
            return "'" + val.ToString() + "'";
        case LogicalTypeId::TIMESTAMP:
            return "'" + val.ToString() + "'";
        default:
            return val.ToString();
    }
}

std::string BuildDbiasmQuery(const DbiasmScanBindData &bind_data) {
    std::string sql = "SELECT ";

    // Column projection
    if (bind_data.column_ids.empty()) {
        sql += "*";
    } else {
        bool first = true;
        for (auto col_id : bind_data.column_ids) {
            if (!first) sql += ", ";
            first = false;
            if (col_id < bind_data.columns.size()) {
                sql += bind_data.columns[col_id].name;
            }
        }
    }

    sql += " FROM " + bind_data.table_name;

    // WHERE clause from pushed filters
    if (!bind_data.filters.empty()) {
        sql += " WHERE ";
        bool first = true;
        for (const auto &filter : bind_data.filters) {
            if (!first) sql += " AND ";
            first = false;
            sql += filter;
        }
    }

    return sql;
}

//===--------------------------------------------------------------------===//
// Filter Pushdown
//===--------------------------------------------------------------------===//

static bool TryPushdownFilter(const ColumnInfo &column,
                               TableFilter &filter,
                               std::vector<std::string> &result) {
    const std::string &col_name = column.name;

    switch (filter.filter_type) {
        case TableFilterType::CONSTANT_COMPARISON: {
            auto &const_filter = filter.Cast<ConstantFilter>();
            std::string op;
            switch (const_filter.comparison_type) {
                case ExpressionType::COMPARE_EQUAL:
                    op = " = ";
                    break;
                case ExpressionType::COMPARE_NOTEQUAL:
                    op = " <> ";
                    break;
                case ExpressionType::COMPARE_LESSTHAN:
                    op = " < ";
                    break;
                case ExpressionType::COMPARE_GREATERTHAN:
                    op = " > ";
                    break;
                case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                    op = " <= ";
                    break;
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                    op = " >= ";
                    break;
                default:
                    return false;
            }
            result.push_back(col_name + op + FormatValueForSQL(const_filter.constant));
            return true;
        }
        case TableFilterType::IS_NULL:
            result.push_back(col_name + " IS NULL");
            return true;
        case TableFilterType::IS_NOT_NULL:
            result.push_back(col_name + " IS NOT NULL");
            return true;
        case TableFilterType::CONJUNCTION_AND: {
            auto &conjunction = filter.Cast<ConjunctionAndFilter>();
            for (auto &child : conjunction.child_filters) {
                if (!TryPushdownFilter(column, *child, result)) {
                    return false;
                }
            }
            return true;
        }
        case TableFilterType::CONJUNCTION_OR: {
            // OR is trickier - need to group conditions
            auto &conjunction = filter.Cast<ConjunctionOrFilter>();
            std::vector<std::string> or_conditions;
            for (auto &child : conjunction.child_filters) {
                std::vector<std::string> child_conditions;
                if (!TryPushdownFilter(column, *child, child_conditions)) {
                    return false;
                }
                if (child_conditions.size() == 1) {
                    or_conditions.push_back(child_conditions[0]);
                } else {
                    // Multiple conditions from child, AND them
                    std::string combined = "(";
                    for (size_t i = 0; i < child_conditions.size(); i++) {
                        if (i > 0) combined += " AND ";
                        combined += child_conditions[i];
                    }
                    combined += ")";
                    or_conditions.push_back(combined);
                }
            }
            if (!or_conditions.empty()) {
                std::string or_clause = "(";
                for (size_t i = 0; i < or_conditions.size(); i++) {
                    if (i > 0) or_clause += " OR ";
                    or_clause += or_conditions[i];
                }
                or_clause += ")";
                result.push_back(or_clause);
            }
            return true;
        }
        default:
            return false;
    }
}

//===--------------------------------------------------------------------===//
// Table Function Implementation
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> DbiasmScanBind(ClientContext &context,
                                         TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types,
                                         vector<string> &names) {
    // This is called from the catalog, bind_data is pre-populated
    auto &bind_data = input.bind_data->Cast<DbiasmScanBindData>();

    for (const auto &col : bind_data.columns) {
        return_types.push_back(MapOdbcTypeToDuckDB(col.odbc_type));
        names.push_back(col.name);
    }

    return nullptr; // bind_data already set
}

unique_ptr<GlobalTableFunctionState> DbiasmScanInitGlobal(ClientContext &context,
                                                           TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<DbiasmScanBindData>();
    auto state = make_uniq<DbiasmScanGlobalState>();

    // Store projected column IDs
    auto &data = const_cast<DbiasmScanBindData &>(bind_data);
    data.column_ids.clear();
    for (auto col_id : input.column_ids) {
        if (col_id != COLUMN_IDENTIFIER_ROW_ID) {
            data.column_ids.push_back(col_id);
        }
    }

    // Process table filters
    if (input.filters) {
        for (auto &entry : input.filters->filters) {
            auto col_idx = entry.first;
            if (col_idx < bind_data.columns.size()) {
                TryPushdownFilter(bind_data.columns[col_idx], *entry.second, data.filters);
            }
        }
    }

    // Build and execute query
    std::string sql = BuildDbiasmQuery(bind_data);

    state->client = make_uniq<OdbcBridgeClient>(bind_data.host, bind_data.port);
    state->reader = state->client->Query(sql, std::nullopt);

    return std::move(state);
}

unique_ptr<LocalTableFunctionState> DbiasmScanInitLocal(ExecutionContext &context,
                                                         TableFunctionInitInput &input,
                                                         GlobalTableFunctionState *global_state) {
    return make_uniq<DbiasmScanLocalState>();
}

void DbiasmScanExecute(ClientContext &context,
                       TableFunctionInput &input,
                       DataChunk &output) {
    auto &gstate = input.global_state->Cast<DbiasmScanGlobalState>();
    auto &bind_data = input.bind_data->Cast<DbiasmScanBindData>();

    std::lock_guard<std::mutex> lock(gstate.lock);

    if (gstate.finished) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    while (count < STANDARD_VECTOR_SIZE) {
        // Need more rows?
        if (gstate.batch_offset >= gstate.current_batch.size()) {
            if (!gstate.reader->ReadBatch(gstate.current_batch)) {
                gstate.finished = true;
                break;
            }
            gstate.batch_offset = 0;
        }

        // Copy row to output
        auto &row = gstate.current_batch[gstate.batch_offset];

        // Map from full row to projected columns
        for (idx_t out_col = 0; out_col < output.ColumnCount(); out_col++) {
            idx_t src_col = bind_data.column_ids.empty() ? out_col : bind_data.column_ids[out_col];
            if (src_col < row.size()) {
                output.SetValue(out_col, count, row[src_col]);
            } else {
                output.SetValue(out_col, count, Value());
            }
        }

        gstate.batch_offset++;
        count++;
    }

    output.SetCardinality(count);
}

TableFunction GetDbiasmScanFunction() {
    TableFunction func("dbisam_scan", {}, DbiasmScanExecute, nullptr);

    func.init_global = DbiasmScanInitGlobal;
    func.init_local = DbiasmScanInitLocal;

    // Enable filter pushdown
    func.filter_pushdown = true;

    // Enable projection pushdown
    func.projection_pushdown = true;

    return func;
}

} // namespace duckdb
