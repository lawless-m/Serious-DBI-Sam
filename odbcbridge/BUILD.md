# Build Instructions

## Prerequisites

### Windows Service (C#)

- .NET 8 SDK
- Visual Studio 2022 or VS Code with C# extension
- ODBC driver for your data source (DBISAM) installed and DSN configured

### DuckDB Extension (C++)

- CMake 3.21+
- C++17 compiler (GCC 11+, Clang 13+)
- gRPC and Protocol Buffers
- DuckDB source code

## Building the Windows Service

### 1. Create the Project

```powershell
dotnet new grpc -n OdbcBridge.Service
cd OdbcBridge.Service
```

### 2. Add Dependencies

```powershell
dotnet add package System.Data.Odbc
dotnet add package Microsoft.Extensions.Hosting.WindowsServices
```

### 3. Copy Protocol Definition

Copy the `.proto` content from PROTOCOL.md to `Protos/odbcbridge.proto`.

Update the `.csproj` to include proto compilation:

```xml
<ItemGroup>
  <Protobuf Include="Protos\odbcbridge.proto" GrpcServices="Server" />
</ItemGroup>
```

### 4. Implement the Service

Follow the implementation guide in SERVICE.md.

### 5. Configure

Edit `appsettings.json` with your DSN name and desired port.

### 6. Build and Run

```powershell
# Development
dotnet run

# Release build
dotnet publish -c Release -o publish/

# Install as Windows service
sc create OdbcBridge binPath="C:\path\to\publish\OdbcBridge.Service.exe"
sc start OdbcBridge
```

### 7. Test the Service

Use a gRPC testing tool like `grpcurl`:

```bash
# List tables
grpcurl -plaintext localhost:50051 odbcbridge.OdbcBridge/ListTables

# Describe table
grpcurl -plaintext -d '{"table_name": "customers"}' \
    localhost:50051 odbcbridge.OdbcBridge/DescribeTable
```

## Building the DuckDB Extension

### 1. Install gRPC (Debian)

```bash
# Install dependencies
sudo apt-get update
sudo apt-get install -y build-essential autoconf libtool pkg-config \
    cmake ninja-build

# Install gRPC via vcpkg (recommended) or system packages
# Option A: vcpkg
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.sh
./vcpkg install grpc

# Option B: System packages (may be older)
sudo apt-get install -y libgrpc++-dev protobuf-compiler-grpc
```

### 2. Clone Extension Template

```bash
git clone https://github.com/duckdb/extension-template.git duckdb-odbcbridge
cd duckdb-odbcbridge

# Initialise submodules (gets DuckDB source)
git submodule update --init --recursive
```

### 3. Set Up Source Files

```bash
# Create directory structure
mkdir -p src/include proto

# Copy sources from this documentation into:
# - src/odbcbridge_extension.cpp
# - src/grpc_client.cpp
# - src/table_functions.cpp
# - src/type_mapping.cpp
# - src/include/*.hpp
# - proto/odbcbridge.proto
```

### 4. Update CMakeLists.txt

Replace the template CMakeLists.txt with the one from EXTENSION.md.

If using vcpkg:

```bash
export CMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### 5. Build

```bash
# Debug build
make debug

# Release build
make release

# Output location
ls build/release/extension/odbcbridge/
# odbcbridge.duckdb_extension
```

### 6. Test Locally

```bash
# Start DuckDB
./build/release/duckdb

# Load extension
LOAD 'build/release/extension/odbcbridge/odbcbridge.duckdb_extension';

# Configure (pointing to your Windows machine)
SET odbcbridge_host = '192.168.1.100';
SET odbcbridge_port = 50051;

# Test
SELECT * FROM dbisam_tables();
```

## Network Setup

The Linux machine running DuckDB needs network access to the Windows machine running the service.

### Firewall

On Windows, allow inbound connections on the service port (default 50051):

```powershell
netsh advfirewall firewall add rule name="ODBC Bridge" dir=in action=allow protocol=tcp localport=50051
```

### Testing Connectivity

From Linux:

```bash
# Check port is reachable
nc -zv 192.168.1.100 50051

# Or with grpcurl
grpcurl -plaintext 192.168.1.100:50051 list
```

## Troubleshooting

### Service Won't Start

- Check Windows Event Viewer for errors
- Verify DSN is configured correctly in ODBC Data Source Administrator
- Test DSN connection manually with another tool

### Extension Build Fails

- Ensure gRPC is found: `cmake` should report finding gRPC
- Check DuckDB version compatibility
- Verify protobuf files are being generated

### Connection Refused

- Verify service is running: `sc query OdbcBridge`
- Check firewall rules
- Confirm correct IP and port

### Query Errors

- Check service logs for ODBC errors
- Test the same SQL directly against DBISAM
- Verify table names are correct (case sensitivity)

## Development Workflow

1. **Service changes**: Edit C#, `dotnet run` to test
2. **Protocol changes**: Update `.proto`, rebuild both service and extension
3. **Extension changes**: Edit C++, `make debug`, test in DuckDB

For rapid iteration, keep the service running in console mode (`dotnet run`) rather than as a Windows service.
