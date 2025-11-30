#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status

# -----------------------------------------------------------------------------
# Vegh E2E Simulation Script
# This script simulates a user workflow: Create Data -> Snap -> Check -> Restore
# -----------------------------------------------------------------------------

# Determine the binary path based on the OS
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    VEGH_BIN="./target/release/vegh.exe"
else
    VEGH_BIN="./target/release/vegh"
fi

# Define test paths
TEST_DIR="test_data_input"
RESTORE_DIR="test_data_output"
SNAP_FILE="test_archive.snap"

echo "🥬 Starting Vegh E2E Simulation..."

# 1. Setup: Create dummy data for testing
echo "🛠️  Creating dummy data..."
mkdir -p "$TEST_DIR"
echo "Hello Teaserverse" > "$TEST_DIR/hello.txt"
echo "CodeTease Platform Inc" > "$TEST_DIR/config.json"

# Create a random 1MB file to test compression effectiveness
echo "   Generating random binary file..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS specific command
    mkfile 1m "$TEST_DIR/random.bin"
else
    # Linux/Windows (Git Bash) command
    dd if=/dev/urandom of="$TEST_DIR/random.bin" bs=1M count=1 status=none
fi

# 2. Test SNAP (First Run - Cold Start)
echo "📸 Testing SNAP (Cold Run)..."
# We use level 1 for speed during tests
"$VEGH_BIN" snap "$TEST_DIR" --output "$SNAP_FILE" --level 1

if [ ! -f "$SNAP_FILE" ]; then
    echo "❌ Error: Snapshot file was not created!"
    exit 1
fi

# 3. Test CACHE (Second Run - Warm Start)
echo "🔥 Testing SNAP (Cached Run)..."
# Vegh should detect the existing .veghcache and speed up this run
"$VEGH_BIN" snap "$TEST_DIR" --output "$SNAP_FILE" --level 1

# 4. Test LIST command
echo "📜 Testing LIST command..."
"$VEGH_BIN" list "$SNAP_FILE"

# 5. Test CHECK command (Integrity Verification)
echo "✅ Testing CHECK command..."
"$VEGH_BIN" check "$SNAP_FILE"

# 6. Test RESTORE command
echo "📦 Testing RESTORE command..."
# Ensure the output directory is clean
rm -rf "$RESTORE_DIR"
"$VEGH_BIN" restore "$SNAP_FILE" --out_dir "$RESTORE_DIR"

# 7. Verification: Compare Input and Output Data
echo "🔍 Verifying Data Integrity..."
# Diff returns 0 if directories are identical, 1 if they differ
# We explicitly check the subdirectory because Vegh restores into the out_dir
if diff -r "$TEST_DIR" "$RESTORE_DIR/$TEST_DIR"; then
    echo "✨ SUCCESS: Restored data matches the original source perfectly!"
else
    echo "❌ FAILURE: Restored data differs from original!"
    # List differences for debugging
    diff -r "$TEST_DIR" "$RESTORE_DIR/$TEST_DIR"
    exit 1
fi

# Cleanup artifacts
echo "🧹 Cleaning up test artifacts..."
rm -rf "$TEST_DIR" "$RESTORE_DIR" "$SNAP_FILE" .veghcache

echo "🎉 All Vegh tests passed successfully!"