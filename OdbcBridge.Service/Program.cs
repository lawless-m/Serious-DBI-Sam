using OdbcBridge.Configuration;
using OdbcBridge.Data;
using OdbcBridge.Services;

var builder = WebApplication.CreateBuilder(args);

// Add Windows service support
builder.Host.UseWindowsService();

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
