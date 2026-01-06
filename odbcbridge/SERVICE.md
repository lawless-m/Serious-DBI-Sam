# ODBC Bridge Service (Windows)

A C# gRPC service that exposes a single ODBC data source over the network.

## Overview

The service:
- Connects to a single configured ODBC DSN
- Exposes table listing, schema discovery, and query execution via gRPC
- Handles connection pooling and error translation
- Runs as a Windows service or console application

## Project Structure

```
OdbcBridge.Service/
├── OdbcBridge.Service.csproj
├── Program.cs                 # Entry point, host configuration
├── appsettings.json          # Configuration
├── Protos/
│   └── odbcbridge.proto      # Protocol definition (copy from PROTOCOL.md)
├── Services/
│   └── OdbcBridgeService.cs  # gRPC service implementation
├── Data/
│   ├── OdbcConnectionManager.cs  # Connection pooling
│   └── OdbcQueryExecutor.cs      # Query execution and type mapping
└── Configuration/
    └── ServiceSettings.cs     # Configuration model
```

## Dependencies

```xml
<PackageReference Include="Grpc.AspNetCore" Version="2.60.0" />
<PackageReference Include="System.Data.Odbc" Version="8.0.0" />
<PackageReference Include="Microsoft.Extensions.Hosting.WindowsServices" Version="8.0.0" />
```

## Configuration

`appsettings.json`:

```json
{
  "OdbcBridge": {
    "Dsn": "YourDsnName",
    "ConnectionString": "DSN=YourDsnName;",
    "Port": 50051,
    "BatchSize": 1000,
    "ConnectionTimeout": 30,
    "CommandTimeout": 300
  },
  "Kestrel": {
    "Endpoints": {
      "Grpc": {
        "Url": "http://*:50051",
        "Protocols": "Http2"
      }
    }
  }
}
```

Either `Dsn` or `ConnectionString` can be used. If both are provided, `ConnectionString` takes precedence.

## Key Implementation Details

### Program.cs

```csharp
var builder = WebApplication.CreateBuilder(args);

// Add Windows service support
builder.Host.UseWindowsService();

// Configure gRPC
builder.Services.AddGrpc();

// Register services
builder.Services.Configure<ServiceSettings>(
    builder.Configuration.GetSection("OdbcBridge"));
builder.Services.AddSingleton<OdbcConnectionManager>();
builder.Services.AddScoped<OdbcQueryExecutor>();

var app = builder.Build();
app.MapGrpcService<OdbcBridgeService>();
app.Run();
```

### OdbcConnectionManager.cs

Handles connection lifecycle:

```csharp
public class OdbcConnectionManager : IDisposable
{
    private readonly string _connectionString;
    private readonly int _timeout;
    
    public OdbcConnectionManager(IOptions<ServiceSettings> settings)
    {
        var config = settings.Value;
        _connectionString = config.ConnectionString 
            ?? $"DSN={config.Dsn};";
        _timeout = config.ConnectionTimeout;
    }
    
    public OdbcConnection GetConnection()
    {
        var conn = new OdbcConnection(_connectionString);
        conn.ConnectionTimeout = _timeout;
        conn.Open();
        return conn;
    }
}
```

### OdbcBridgeService.cs

The gRPC service implementation:

```csharp
public class OdbcBridgeService : OdbcBridge.OdbcBridgeBase
{
    private readonly OdbcConnectionManager _connManager;
    private readonly OdbcQueryExecutor _executor;
    private readonly int _batchSize;
    
    public override Task<ListTablesResponse> ListTables(
        ListTablesRequest request, 
        ServerCallContext context)
    {
        using var conn = _connManager.GetConnection();
        var tables = conn.GetSchema("Tables");
        
        var response = new ListTablesResponse();
        foreach (DataRow row in tables.Rows)
        {
            var tableType = row["TABLE_TYPE"]?.ToString();
            if (tableType == "TABLE")
            {
                response.TableNames.Add(row["TABLE_NAME"].ToString());
            }
        }
        return Task.FromResult(response);
    }
    
    public override Task<DescribeTableResponse> DescribeTable(
        DescribeTableRequest request,
        ServerCallContext context)
    {
        using var conn = _connManager.GetConnection();
        
        // Use GetSchema to get column info
        var restrictions = new string[] { null, null, request.TableName };
        var columns = conn.GetSchema("Columns", restrictions);
        
        var response = new DescribeTableResponse
        {
            TableName = request.TableName
        };
        
        foreach (DataRow row in columns.Rows)
        {
            response.Columns.Add(new ColumnInfo
            {
                Name = row["COLUMN_NAME"].ToString(),
                TypeName = row["TYPE_NAME"].ToString(),
                OdbcType = Convert.ToInt32(row["DATA_TYPE"]),
                Size = Convert.ToInt32(row["COLUMN_SIZE"] ?? 0),
                DecimalDigits = Convert.ToInt32(row["DECIMAL_DIGITS"] ?? 0),
                Nullable = Convert.ToBoolean(row["NULLABLE"] ?? true)
            });
        }
        
        return Task.FromResult(response);
    }
    
    public override async Task Query(
        QueryRequest request,
        IServerStreamWriter<QueryResponse> responseStream,
        ServerCallContext context)
    {
        var sql = ApplyLimit(request.Sql, request.Limit);
        
        using var conn = _connManager.GetConnection();
        using var cmd = new OdbcCommand(sql, conn);
        cmd.CommandTimeout = _commandTimeout;
        
        using var reader = cmd.ExecuteReader();
        
        // Send schema first
        var schema = BuildSchema(reader);
        await responseStream.WriteAsync(new QueryResponse { Schema = schema });
        
        // Stream row batches
        var batch = new RowBatch();
        while (reader.Read())
        {
            batch.Rows.Add(BuildRow(reader, schema.Columns));
            
            if (batch.Rows.Count >= _batchSize)
            {
                await responseStream.WriteAsync(new QueryResponse { Rows = batch });
                batch = new RowBatch();
            }
            
            context.CancellationToken.ThrowIfCancellationRequested();
        }
        
        // Send remaining rows
        if (batch.Rows.Count > 0)
        {
            await responseStream.WriteAsync(new QueryResponse { Rows = batch });
        }
    }
}
```

### Type Mapping

```csharp
private Value MapValue(OdbcDataReader reader, int ordinal, int odbcType)
{
    if (reader.IsDBNull(ordinal))
        return new Value { IsNull = true };
    
    return odbcType switch
    {
        // Integers
        OdbcType.TinyInt or OdbcType.SmallInt or 
        OdbcType.Int or OdbcType.BigInt 
            => new Value { IntValue = reader.GetInt64(ordinal) },
        
        // Floating point
        OdbcType.Real or OdbcType.Double 
            => new Value { DoubleValue = reader.GetDouble(ordinal) },
        
        // Decimal - send as string to preserve precision
        OdbcType.Decimal or OdbcType.Numeric 
            => new Value { StringValue = reader.GetDecimal(ordinal).ToString() },
        
        // Boolean
        OdbcType.Bit 
            => new Value { BoolValue = reader.GetBoolean(ordinal) },
        
        // Binary
        OdbcType.Binary or OdbcType.VarBinary or OdbcType.LongVarBinary 
            => new Value { BytesValue = ByteString.CopyFrom(GetBytes(reader, ordinal)) },
        
        // Date/Time - ISO 8601 format
        OdbcType.Date 
            => new Value { StringValue = reader.GetDate(ordinal).ToString("yyyy-MM-dd") },
        OdbcType.Time 
            => new Value { StringValue = reader.GetTime(ordinal).ToString("HH:mm:ss") },
        OdbcType.DateTime or OdbcType.Timestamp 
            => new Value { StringValue = reader.GetDateTime(ordinal).ToString("O") },
        
        // Everything else as string
        _ => new Value { StringValue = reader.GetValue(ordinal).ToString() }
    };
}
```

### Limit Handling

```csharp
private string ApplyLimit(string sql, int? requestedLimit)
{
    if (!requestedLimit.HasValue || requestedLimit.Value <= 0)
        return sql;
    
    var limit = requestedLimit.Value;
    
    // Check for existing LIMIT clause (simple regex, handles most cases)
    var limitMatch = Regex.Match(sql, @"\bLIMIT\s+(\d+)", RegexOptions.IgnoreCase);
    if (limitMatch.Success)
    {
        var existingLimit = int.Parse(limitMatch.Groups[1].Value);
        if (existingLimit <= limit)
            return sql; // Existing limit is smaller, keep it
        
        // Replace with smaller limit
        return Regex.Replace(sql, @"\bLIMIT\s+\d+", $"LIMIT {limit}", 
            RegexOptions.IgnoreCase);
    }
    
    // No existing limit - append one
    // Remove trailing semicolon if present
    sql = sql.TrimEnd().TrimEnd(';');
    return $"{sql} LIMIT {limit}";
}
```

## Running

### As Console Application (Development)

```bash
dotnet run
```

### As Windows Service

```bash
# Publish
dotnet publish -c Release -o C:\Services\OdbcBridge

# Install service
sc create OdbcBridge binPath="C:\Services\OdbcBridge\OdbcBridge.Service.exe"
sc start OdbcBridge
```

## Error Handling

The service translates ODBC errors to gRPC status codes:

```csharp
catch (OdbcException ex)
{
    var status = ex.Errors[0].NativeError switch
    {
        // Connection errors
        08001 or 08S01 => new Status(StatusCode.Unavailable, ex.Message),
        
        // Syntax errors
        42000 => new Status(StatusCode.InvalidArgument, ex.Message),
        
        // Table not found
        42S02 => new Status(StatusCode.NotFound, ex.Message),
        
        // Default
        _ => new Status(StatusCode.Internal, ex.Message)
    };
    
    throw new RpcException(status);
}
```

## Security Considerations

- The service binds to all interfaces by default (`http://*:50051`)
- For production, consider:
  - Binding to specific IP addresses
  - Using TLS (change to `https://` and configure certificates)
  - Firewall rules to restrict access
  - No authentication is implemented in v1 - add if needed
