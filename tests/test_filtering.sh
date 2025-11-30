#!/bin/bash
set -e

# -----------------------------------------------------------------------------
# Vegh Filtering Logic Test
# Objective: Verify .veghignore, --exclude, and --include priorities.
# -----------------------------------------------------------------------------

if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    VEGH_BIN="./target/release/vegh.exe"
else
    VEGH_BIN="./target/release/vegh"
fi

TEST_DIR="filter_test_data"
SNAP_FILE="filter_test.snap"

echo "🥬 Starting Vegh Filtering Test..."

# 1. Setup Complex Directory Structure
echo "🛠️  Creating scenario..."
rm -rf "$TEST_DIR" "$SNAP_FILE"
mkdir -p "$TEST_DIR/src"
mkdir -p "$TEST_DIR/node_modules/fake-lib"
mkdir -p "$TEST_DIR/build"
mkdir -p "$TEST_DIR/secrets"

# Create valid files (Should be kept)
echo "pub fn main() {}" > "$TEST_DIR/src/main.rs"
echo "Project Config" > "$TEST_DIR/package.json"

# Create ignored files (Should be ignored via .veghignore)
echo "Heavy dependency code" > "$TEST_DIR/node_modules/fake-lib/index.js"
echo "API_KEY=123456" > "$TEST_DIR/secrets/.env"

# Create excluded files (Should be ignored via CLI --exclude)
echo "Binary blob" > "$TEST_DIR/build/output.bin"

# Create forced include files (Should be kept via CLI --include even if ignored)
echo "Important Log" > "$TEST_DIR/error.log"

# Create .veghignore
echo "📝 Creating .veghignore..."
cat <<EOF > "$TEST_DIR/.veghignore"
node_modules
secrets/
*.log
EOF

# 2. Execute Snap with Flags
echo "📸 Running Snap with filters..."
# Logic:
# - Standard ignore: node_modules, secrets
# - CLI Exclude: build/
# - CLI Include: error.log (Overrides *.log in .veghignore)

"$VEGH_BIN" snap "$TEST_DIR" \
    --output "$SNAP_FILE" \
    --exclude "build" \
    --include "error.log" \
    --level 1

# 3. Verification
echo "🔍 Verifying archive contents..."

# Helper function to check file existence in archive
check_file() {
    local file=$1
    local should_exist=$2
    
    # We use 'vegh list' and grep. We suppress output for cleaner logs.
    if "$VEGH_BIN" list "$SNAP_FILE" | grep -q "$file"; then
        if [ "$should_exist" == "true" ]; then
            echo "   ✅ Found expected file: $file"
        else
            echo "   ❌ ERROR: Found unexpected file: $file (Should be ignored!)"
            exit 1
        fi
    else
        if [ "$should_exist" == "true" ]; then
            echo "   ❌ ERROR: Missing expected file: $file"
            exit 1
        else
            echo "   ✅ Correctly ignored: $file"
        fi
    fi
}

# --- Assertion Phase ---

# Positive Checks (Files that MUST exist)
check_file "src/main.rs" "true"
check_file "package.json" "true"
check_file "error.log" "true"       # Included via CLI, overrides .veghignore

# Negative Checks (Files that MUST NOT exist)
check_file "node_modules" "false"   # Ignored via .veghignore
check_file "secrets/.env" "false"   # Ignored via .veghignore
check_file "build/output.bin" "false" # Excluded via CLI

# Cleanup
echo "🧹 Cleaning up..."
rm -rf "$TEST_DIR" "$SNAP_FILE" .veghcache

echo "🎉 Filtering logic verification PASSED!"