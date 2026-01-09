using System.Data;
using System.Data.Odbc;
using System.Text.RegularExpressions;
using Google.Protobuf;
using Microsoft.Extensions.Options;
using OdbcBridge.Configuration;

namespace OdbcBridge.Data;

public class OdbcQueryExecutor
{
    private readonly OdbcConnectionManager _connectionManager;
    private readonly int _batchSize;
    private readonly int _commandTimeout;
    private readonly ILogger<OdbcQueryExecutor> _logger;

    public OdbcQueryExecutor(
        OdbcConnectionManager connectionManager,
        IOptions<ServiceSettings> settings,
        ILogger<OdbcQueryExecutor> logger)
    {
        _connectionManager = connectionManager;
        _batchSize = settings.Value.BatchSize;
        _commandTimeout = settings.Value.CommandTimeout;
        _logger = logger;
    }

    public List<string> ListTables()
    {
        using var conn = _connectionManager.GetConnection();
        var tables = conn.GetSchema("Tables");
        var result = new List<string>();

        foreach (DataRow row in tables.Rows)
        {
            var tableType = row["TABLE_TYPE"]?.ToString();
            if (tableType == "TABLE")
            {
                var tableName = row["TABLE_NAME"]?.ToString();
                if (!string.IsNullOrEmpty(tableName))
                {
                    result.Add(tableName);
                }
            }
        }

        _logger.LogInformation("Listed {Count} tables", result.Count);
        return result;
    }

    public List<ColumnInfo> DescribeTable(string tableName)
    {
        using var conn = _connectionManager.GetConnection();
        var restrictions = new string?[] { null, null, tableName };
        var columns = conn.GetSchema("Columns", restrictions);
        var result = new List<ColumnInfo>();

        foreach (DataRow row in columns.Rows)
        {
            result.Add(new ColumnInfo
            {
                Name = GetString(row, "COLUMN_NAME"),
                TypeName = GetString(row, "TYPE_NAME"),
                OdbcType = GetInt(row, "DATA_TYPE", 0),
                Size = GetInt(row, "COLUMN_SIZE", 0),
                DecimalDigits = GetInt(row, "DECIMAL_DIGITS", 0),
                Nullable = GetBool(row, "NULLABLE", true)
            });
        }

        _logger.LogInformation("Described table {TableName}: {Count} columns", tableName, result.Count);
        return result;
    }

    // Helper methods for handling DBNull in DataRow
    private static string GetString(DataRow row, string column)
    {
        if (!row.Table.Columns.Contains(column)) return "";
        var val = row[column];
        return val is DBNull ? "" : val?.ToString() ?? "";
    }

    private static int GetInt(DataRow row, string column, int defaultValue)
    {
        if (!row.Table.Columns.Contains(column)) return defaultValue;
        var val = row[column];
        return val is DBNull ? defaultValue : Convert.ToInt32(val);
    }

    private static bool GetBool(DataRow row, string column, bool defaultValue)
    {
        if (!row.Table.Columns.Contains(column)) return defaultValue;
        var val = row[column];
        return val is DBNull ? defaultValue : Convert.ToBoolean(val);
    }

    public async IAsyncEnumerable<QueryResponse> ExecuteQueryAsync(
        string sql,
        int? limit,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var effectiveSql = ApplyLimit(sql, limit);
        _logger.LogInformation("Executing query: {Sql}", effectiveSql);

        using var conn = _connectionManager.GetConnection();
        using var cmd = new OdbcCommand(effectiveSql, conn);
        cmd.CommandTimeout = _commandTimeout;

        using var reader = cmd.ExecuteReader();

        // Build and send schema first
        var schema = BuildSchema(reader);
        _logger.LogDebug("Query schema: {ColumnCount} columns", schema.Columns.Count);
        yield return new QueryResponse { Schema = schema };

        // Stream row batches
        var batch = new RowBatch();
        var totalRows = 0;

        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();

            batch.Rows.Add(BuildRow(reader, schema.Columns));

            if (batch.Rows.Count >= _batchSize)
            {
                totalRows += batch.Rows.Count;
                _logger.LogDebug("Sending batch of {Count} rows (total: {Total})", batch.Rows.Count, totalRows);
                yield return new QueryResponse { Rows = batch };
                batch = new RowBatch();
            }
        }

        // Send remaining rows
        if (batch.Rows.Count > 0)
        {
            totalRows += batch.Rows.Count;
            _logger.LogDebug("Sending final batch of {Count} rows (total: {Total})", batch.Rows.Count, totalRows);
            yield return new QueryResponse { Rows = batch };
        }

        _logger.LogInformation("Query completed: {TotalRows} rows returned", totalRows);
    }

    private QuerySchema BuildSchema(OdbcDataReader reader)
    {
        var schema = new QuerySchema();
        var schemaTable = reader.GetSchemaTable();

        if (schemaTable != null)
        {
            // Log available columns for debugging
            _logger.LogDebug("Schema table columns: {Columns}",
                string.Join(", ", schemaTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName)));

            foreach (DataRow row in schemaTable.Rows)
            {
                // DataTypeName may not exist in all ODBC drivers
                string typeName = "";
                if (schemaTable.Columns.Contains("DataTypeName"))
                {
                    typeName = row["DataTypeName"]?.ToString() ?? "";
                }
                else if (schemaTable.Columns.Contains("DataType"))
                {
                    // Fallback: use .NET type name
                    typeName = (row["DataType"] as Type)?.Name ?? "";
                }

                schema.Columns.Add(new ColumnInfo
                {
                    Name = row["ColumnName"]?.ToString() ?? "",
                    TypeName = typeName,
                    OdbcType = schemaTable.Columns.Contains("ProviderType")
                        ? Convert.ToInt32(row["ProviderType"] ?? 0)
                        : 12, // SQL_VARCHAR as fallback
                    Size = schemaTable.Columns.Contains("ColumnSize")
                        ? Convert.ToInt32(row["ColumnSize"] ?? 0)
                        : 0,
                    DecimalDigits = schemaTable.Columns.Contains("NumericScale")
                        ? Convert.ToInt32(row["NumericScale"] ?? 0)
                        : 0,
                    Nullable = schemaTable.Columns.Contains("AllowDBNull")
                        ? Convert.ToBoolean(row["AllowDBNull"] ?? true)
                        : true
                });
            }
        }

        return schema;
    }

    private Row BuildRow(OdbcDataReader reader, IList<ColumnInfo> columns)
    {
        var row = new Row();

        for (int i = 0; i < columns.Count; i++)
        {
            row.Values.Add(MapValue(reader, i, columns[i].OdbcType));
        }

        return row;
    }

    private Value MapValue(OdbcDataReader reader, int ordinal, int odbcType)
    {
        if (reader.IsDBNull(ordinal))
        {
            return new Value { IsNull = true };
        }

        try
        {
            return odbcType switch
            {
                // Integer types: SQL_TINYINT (-6), SQL_SMALLINT (5), SQL_INTEGER (4), SQL_BIGINT (-5)
                -6 or 5 or 4 or -5 => new Value { IntValue = reader.GetInt64(ordinal) },

                // Floating point: SQL_REAL (7), SQL_FLOAT (6), SQL_DOUBLE (8)
                7 or 6 or 8 => new Value { DoubleValue = reader.GetDouble(ordinal) },

                // Decimal/Numeric: SQL_DECIMAL (3), SQL_NUMERIC (2) - send as string to preserve precision
                2 or 3 => new Value { StringValue = reader.GetDecimal(ordinal).ToString() },

                // Boolean: SQL_BIT (-7)
                -7 => new Value { BoolValue = reader.GetBoolean(ordinal) },

                // Binary: SQL_BINARY (-2), SQL_VARBINARY (-3), SQL_LONGVARBINARY (-4)
                -2 or -3 or -4 => new Value { BytesValue = ByteString.CopyFrom(GetBytes(reader, ordinal)) },

                // Date: SQL_DATE (91)
                91 => new Value { StringValue = reader.GetDate(ordinal).ToString("yyyy-MM-dd") },

                // Time: SQL_TIME (92)
                92 => new Value { StringValue = reader.GetDateTime(ordinal).ToString("HH:mm:ss") },

                // Timestamp: SQL_TIMESTAMP (93)
                93 => new Value { StringValue = reader.GetDateTime(ordinal).ToString("O") },

                // Everything else as string
                _ => new Value { StringValue = reader.GetValue(ordinal)?.ToString() ?? "" }
            };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error mapping value at ordinal {Ordinal} with ODBC type {OdbcType}, falling back to string",
                ordinal, odbcType);
            try
            {
                return new Value { StringValue = reader.GetValue(ordinal)?.ToString() ?? "" };
            }
            catch
            {
                return new Value { IsNull = true };
            }
        }
    }

    private static byte[] GetBytes(OdbcDataReader reader, int ordinal)
    {
        var length = reader.GetBytes(ordinal, 0, null, 0, 0);
        var buffer = new byte[length];
        reader.GetBytes(ordinal, 0, buffer, 0, (int)length);
        return buffer;
    }

    private string ApplyLimit(string sql, int? requestedLimit)
    {
        if (!requestedLimit.HasValue || requestedLimit.Value <= 0)
            return sql;

        var limit = requestedLimit.Value;

        // Check for existing TOP clause at END of query (DBISAM style: SELECT * FROM t TOP 5)
        var topMatch = Regex.Match(sql, @"\bTOP\s+(\d+)\s*$", RegexOptions.IgnoreCase);
        if (topMatch.Success)
        {
            var existingLimit = int.Parse(topMatch.Groups[1].Value);
            if (existingLimit <= limit)
                return sql; // Existing limit is smaller, keep it

            // Replace with smaller limit
            return Regex.Replace(sql, @"\bTOP\s+\d+\s*$", $"TOP {limit}",
                RegexOptions.IgnoreCase);
        }

        // No existing limit - append TOP at the end (DBISAM style)
        sql = sql.TrimEnd().TrimEnd(';');
        return $"{sql} TOP {limit}";
    }
}
