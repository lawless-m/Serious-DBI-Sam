cmake_minimum_required(VERSION 3.21)

set(TARGET_NAME odbcbridge)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find DuckDB
include(FetchContent)

# Set extension options
set(EXTENSION_STATIC_BUILD ON)

# Find gRPC and Protobuf
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

message(STATUS "Using gRPC ${gRPC_VERSION}")
message(STATUS "Using protobuf ${Protobuf_VERSION}")

# Get the proto compiler and gRPC plugin
get_target_property(PROTOC_EXECUTABLE protobuf::protoc LOCATION)
get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin LOCATION)

# Proto file
set(PROTO_FILE ${CMAKE_SOURCE_DIR}/proto/odbcbridge.proto)
set(PROTO_OUT_DIR ${CMAKE_BINARY_DIR}/generated)

file(MAKE_DIRECTORY ${PROTO_OUT_DIR})

# Generate protobuf and gRPC files
set(PROTO_SRCS ${PROTO_OUT_DIR}/odbcbridge.pb.cc)
set(PROTO_HDRS ${PROTO_OUT_DIR}/odbcbridge.pb.h)
set(GRPC_SRCS ${PROTO_OUT_DIR}/odbcbridge.grpc.pb.cc)
set(GRPC_HDRS ${PROTO_OUT_DIR}/odbcbridge.grpc.pb.h)

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${PROTOC_EXECUTABLE}
    ARGS --grpc_out=${PROTO_OUT_DIR}
         --cpp_out=${PROTO_OUT_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
         -I${CMAKE_SOURCE_DIR}/proto
         ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating protobuf and gRPC files"
)

# Create proto library
add_library(odbcbridge_proto STATIC
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

# Need PIC for linking into shared library
set_target_properties(odbcbridge_proto PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_link_libraries(odbcbridge_proto
    PUBLIC
    protobuf::libprotobuf
    gRPC::grpc++
)

target_include_directories(odbcbridge_proto
    PUBLIC
    ${PROTO_OUT_DIR}
)

# Extension sources
set(EXTENSION_SOURCES
    src/odbcbridge_extension.cpp
    src/grpc_client.cpp
    src/table_functions.cpp
    src/type_mapping.cpp
    src/dbisam_catalog.cpp
)

# Include DuckDB
if(NOT DEFINED DUCKDB_DIR)
    message(STATUS "DUCKDB_DIR not set, using FetchContent to download DuckDB")
    FetchContent_Declare(
        duckdb
        GIT_REPOSITORY https://github.com/duckdb/duckdb.git
        GIT_TAG v1.2.1
    )
    FetchContent_MakeAvailable(duckdb)
    set(DUCKDB_DIR ${duckdb_SOURCE_DIR})
endif()

include_directories(${DUCKDB_DIR}/src/include)

# Build loadable extension
add_library(${LOADABLE_EXTENSION_NAME} SHARED ${EXTENSION_SOURCES})

# Required for static linking with DuckDB
set_target_properties(${LOADABLE_EXTENSION_NAME} PROPERTIES CXX_VISIBILITY_PRESET hidden)

target_link_libraries(${LOADABLE_EXTENSION_NAME}
    PRIVATE
    odbcbridge_proto
    gRPC::grpc++
    duckdb_static
    -Wl,--gc-sections
    -Wl,--exclude-libs,ALL
)

target_include_directories(${LOADABLE_EXTENSION_NAME}
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/include
    ${PROTO_OUT_DIR}
)

target_compile_definitions(${LOADABLE_EXTENSION_NAME} PUBLIC -DDUCKDB_BUILD_LOADABLE_EXTENSION)

set_target_properties(${LOADABLE_EXTENSION_NAME} PROPERTIES
    OUTPUT_NAME ${TARGET_NAME}
    PREFIX ""
    SUFFIX ".duckdb_extension"
)

# Use null.txt from DuckDB scripts directory
set(NULL_FILE ${duckdb_SOURCE_DIR}/scripts/null.txt)

# Use DuckDB's version - hardcoded to match FetchContent tag
set(DUCKDB_VERSION_NORMALIZED "v1.2.1")

# Add metadata to extension (required for DuckDB to load it)
add_custom_command(
    TARGET ${LOADABLE_EXTENSION_NAME}
    POST_BUILD
    COMMAND ${CMAKE_COMMAND}
        -DABI_TYPE=CPP
        -DEXTENSION=$<TARGET_FILE:${LOADABLE_EXTENSION_NAME}>
        -DPLATFORM_FILE=${duckdb_BINARY_DIR}/duckdb_platform_out
        -DVERSION_FIELD=${DUCKDB_VERSION_NORMALIZED}
        -DEXTENSION_VERSION=${DUCKDB_VERSION_NORMALIZED}
        -DNULL_FILE=${NULL_FILE}
        -P ${duckdb_SOURCE_DIR}/scripts/append_metadata.cmake
)
add_dependencies(${LOADABLE_EXTENSION_NAME} duckdb_platform)

# Build static extension for linking into DuckDB
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME}
    PRIVATE
    odbcbridge_proto
    gRPC::grpc++
)

target_include_directories(${EXTENSION_NAME}
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/include
    ${PROTO_OUT_DIR}
)
