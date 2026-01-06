namespace OdbcBridge.Configuration;

public class ServiceSettings
{
    public string? Dsn { get; set; }
    public string? ConnectionString { get; set; }
    public int Port { get; set; } = 50051;
    public int BatchSize { get; set; } = 1000;
    public int ConnectionTimeout { get; set; } = 30;
    public int CommandTimeout { get; set; } = 300;

    public string GetConnectionString()
    {
        if (!string.IsNullOrEmpty(ConnectionString))
            return ConnectionString;

        if (!string.IsNullOrEmpty(Dsn))
            return $"DSN={Dsn};";

        throw new InvalidOperationException("Either Dsn or ConnectionString must be configured");
    }
}
