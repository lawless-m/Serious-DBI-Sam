.PHONY: all debug release clean configure

BUILD_DIR := build
GENERATOR := Ninja

# Check for vcpkg toolchain
ifdef VCPKG_ROOT
    CMAKE_TOOLCHAIN := -DCMAKE_TOOLCHAIN_FILE=$(VCPKG_ROOT)/scripts/buildsystems/vcpkg.cmake
else ifdef CMAKE_TOOLCHAIN_FILE
    CMAKE_TOOLCHAIN := -DCMAKE_TOOLCHAIN_FILE=$(CMAKE_TOOLCHAIN_FILE)
else
    CMAKE_TOOLCHAIN :=
endif

all: release

debug:
	@mkdir -p $(BUILD_DIR)/debug
	@cd $(BUILD_DIR)/debug && cmake -G $(GENERATOR) $(CMAKE_TOOLCHAIN) -DCMAKE_BUILD_TYPE=Debug ../..
	@cmake --build $(BUILD_DIR)/debug

release:
	@mkdir -p $(BUILD_DIR)/release
	@cd $(BUILD_DIR)/release && cmake -G $(GENERATOR) $(CMAKE_TOOLCHAIN) -DCMAKE_BUILD_TYPE=Release ../..
	@cmake --build $(BUILD_DIR)/release

configure:
	@mkdir -p $(BUILD_DIR)/release
	@cd $(BUILD_DIR)/release && cmake -G $(GENERATOR) $(CMAKE_TOOLCHAIN) -DCMAKE_BUILD_TYPE=Release ../..

clean:
	@rm -rf $(BUILD_DIR)

# Helper to show extension location
show:
	@echo "Extension location: $(BUILD_DIR)/release/odbcbridge.duckdb_extension"
