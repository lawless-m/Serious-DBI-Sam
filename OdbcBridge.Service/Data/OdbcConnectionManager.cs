using System.Data.Odbc;
using Microsoft.Extensions.Options;
using OdbcBridge.Configuration;

namespace OdbcBridge.Data;

public class OdbcConnectionManager : IDisposable
{
    private readonly string _connectionString;
    private readonly int _timeout;
    private readonly ILogger<OdbcConnectionManager> _logger;
    private bool _disposed;

    public OdbcConnectionManager(
        IOptions<ServiceSettings> settings,
        ILogger<OdbcConnectionManager> logger)
    {
        var config = settings.Value;
        _connectionString = config.GetConnectionString();
        _timeout = config.ConnectionTimeout;
        _logger = logger;

        _logger.LogInformation("OdbcConnectionManager initialized with connection string: {ConnectionString}",
            MaskConnectionString(_connectionString));
    }

    public OdbcConnection GetConnection()
    {
        var conn = new OdbcConnection(_connectionString);
        conn.ConnectionTimeout = _timeout;

        _logger.LogDebug("Opening ODBC connection...");
        conn.Open();
        _logger.LogDebug("ODBC connection opened successfully");

        return conn;
    }

    private static string MaskConnectionString(string connectionString)
    {
        // Mask any password in the connection string for logging
        return System.Text.RegularExpressions.Regex.Replace(
            connectionString,
            @"(PWD|PASSWORD)=[^;]*",
            "$1=***",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _logger.LogInformation("OdbcConnectionManager disposed");
    }
}
