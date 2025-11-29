# Default build directory
BUILD_DIR = build

# Default target
all:
	meson setup $(BUILD_DIR) --wipe
	meson compile -C $(BUILD_DIR)

# Run executables (optional shortcuts)
client: all
	./$(BUILD_DIR)/client

gateway: all
	./$(BUILD_DIR)/gateway

# For incremental rebuild without wiping build dir
fast:
	meson compile -C $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR)
