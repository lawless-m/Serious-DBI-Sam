using Microsoft.AspNetCore.Server.Kestrel.Core;
using OdbcBridge.Configuration;
using OdbcBridge.Data;
using OdbcBridge.Services;

var builder = WebApplication.CreateBuilder(args);

// Add Windows service support
builder.Host.UseWindowsService();

// Configure UTF-8 file logging
var logDir = builder.Configuration.GetSection("OdbcBridge")["LogDir"];
if (!string.IsNullOrEmpty(logDir))
{
    builder.Logging.AddUtf8FileLogger(logDir);
}

// Configure Kestrel for HTTP/2 (required for gRPC)
builder.WebHost.ConfigureKestrel(options =>
{
    // HTTP/2 only for gRPC
    options.ListenAnyIP(50051, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http2;
    });
    // HTTP/1.1 on separate port for health checks
    options.ListenAnyIP(50052, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http1;
    });
});

// Configure gRPC
builder.Services.AddGrpc(options =>
{
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
    options.MaxReceiveMessageSize = 16 * 1024 * 1024; // 16 MB
    options.MaxSendMessageSize = 64 * 1024 * 1024; // 64 MB
});

// Register configuration
builder.Services.Configure<ServiceSettings>(
    builder.Configuration.GetSection("OdbcBridge"));

// Register services
builder.Services.AddSingleton<OdbcConnectionManager>();
builder.Services.AddScoped<OdbcQueryExecutor>();

var app = builder.Build();

// Map gRPC service
app.MapGrpcService<OdbcBridgeService>();

// Health check endpoint
app.MapGet("/", () => "ODBC Bridge Service is running. Use a gRPC client to connect.");

app.Run();
