using System.Data.Odbc;
using Grpc.Core;
using OdbcBridge.Data;

namespace OdbcBridge.Services;

public class OdbcBridgeService : OdbcBridge.OdbcBridgeBase
{
    private readonly OdbcQueryExecutor _executor;
    private readonly ILogger<OdbcBridgeService> _logger;

    public OdbcBridgeService(
        OdbcQueryExecutor executor,
        ILogger<OdbcBridgeService> logger)
    {
        _executor = executor;
        _logger = logger;
    }

    public override Task<ListTablesResponse> ListTables(
        ListTablesRequest request,
        ServerCallContext context)
    {
        _logger.LogInformation("ListTables called");

        try
        {
            var tables = _executor.ListTables();
            var response = new ListTablesResponse();
            response.TableNames.AddRange(tables);
            return Task.FromResult(response);
        }
        catch (OdbcException ex)
        {
            _logger.LogError(ex, "ODBC error in ListTables");
            throw TranslateOdbcException(ex);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in ListTables");
            throw new RpcException(new Status(StatusCode.Internal, ex.Message));
        }
    }

    public override Task<DescribeTableResponse> DescribeTable(
        DescribeTableRequest request,
        ServerCallContext context)
    {
        _logger.LogInformation("DescribeTable called for table: {TableName}", request.TableName);

        if (string.IsNullOrWhiteSpace(request.TableName))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Table name is required"));
        }

        try
        {
            var columns = _executor.DescribeTable(request.TableName);

            if (columns.Count == 0)
            {
                throw new RpcException(new Status(StatusCode.NotFound, $"Table '{request.TableName}' not found"));
            }

            var response = new DescribeTableResponse
            {
                TableName = request.TableName
            };
            response.Columns.AddRange(columns);
            return Task.FromResult(response);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OdbcException ex)
        {
            _logger.LogError(ex, "ODBC error in DescribeTable");
            throw TranslateOdbcException(ex);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in DescribeTable");
            throw new RpcException(new Status(StatusCode.Internal, ex.Message));
        }
    }

    public override async Task Query(
        QueryRequest request,
        IServerStreamWriter<QueryResponse> responseStream,
        ServerCallContext context)
    {
        _logger.LogInformation("Query called: {Sql}, Limit: {Limit}",
            request.Sql, request.HasLimit ? request.Limit : null);

        if (string.IsNullOrWhiteSpace(request.Sql))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "SQL query is required"));
        }

        if (request.HasLimit && request.Limit <= 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Limit must be positive"));
        }

        try
        {
            int? limit = request.HasLimit ? request.Limit : null;

            await foreach (var response in _executor.ExecuteQueryAsync(request.Sql, limit, context.CancellationToken))
            {
                await responseStream.WriteAsync(response);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Query cancelled by client");
            throw;
        }
        catch (OdbcException ex)
        {
            _logger.LogError(ex, "ODBC error in Query");
            throw TranslateOdbcException(ex);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in Query");
            throw new RpcException(new Status(StatusCode.Internal, ex.Message));
        }
    }

    private static RpcException TranslateOdbcException(OdbcException ex)
    {
        if (ex.Errors.Count == 0)
        {
            return new RpcException(new Status(StatusCode.Internal, ex.Message));
        }

        var nativeError = ex.Errors[0].NativeError;
        var sqlState = ex.Errors[0].SQLState;

        var status = sqlState switch
        {
            // Connection errors (08xxx)
            "08001" or "08S01" => new Status(StatusCode.Unavailable, ex.Message),

            // Syntax errors (42000)
            "42000" => new Status(StatusCode.InvalidArgument, ex.Message),

            // Table not found (42S02)
            "42S02" => new Status(StatusCode.NotFound, ex.Message),

            // Default
            _ => new Status(StatusCode.Internal, ex.Message)
        };

        return new RpcException(status);
    }
}
