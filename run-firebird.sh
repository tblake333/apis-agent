#!/bin/bash
#
# Run Firebird-dependent Python scripts in a Docker container
# This handles the ARM Mac compatibility issue with the fdb library
#
# Usage:
#   ./run-firebird.sh schema_discovery.py --output schema.json
#   ./run-firebird.sh testdata.py add articulos 100
#   ./run-firebird.sh fennec.py add 100
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NETWORK="probe_default"
DB_HOST="probe-firebird"
DB_PORT="3050"
DB_PATH="/firebird/data"

# Check if docker network exists
if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
    echo "Error: Docker network '$NETWORK' not found."
    echo "Make sure the Firebird container is running:"
    echo "  cd probe && docker-compose up -d"
    exit 1
fi

# Find the .fdb file in the test-data directory
FDB_FILE=$(ls "$SCRIPT_DIR/probe/test-data/"*.FDB "$SCRIPT_DIR/probe/test-data/"*.fdb 2>/dev/null | head -1)
if [ -z "$FDB_FILE" ]; then
    echo "Error: No .fdb file found in probe/test-data/"
    echo "Copy a Firebird database file to probe/test-data/"
    exit 1
fi
FDB_BASENAME=$(basename "$FDB_FILE")
DB_FULL_PATH="$DB_PATH/$FDB_BASENAME"

# Build the command to run
SCRIPT="$1"
shift

if [ -z "$SCRIPT" ]; then
    echo "Usage: $0 <script.py> [args...]"
    echo ""
    echo "Examples:"
    echo "  $0 schema_discovery.py --output schema.json"
    echo "  $0 testdata.py add articulos 100"
    echo "  $0 testdata.py list"
    echo "  $0 fennec.py add 100"
    exit 1
fi

# Check if the script exists
if [ ! -f "$SCRIPT_DIR/$SCRIPT" ]; then
    echo "Error: Script '$SCRIPT' not found in $SCRIPT_DIR"
    exit 1
fi

echo "Running $SCRIPT in Docker container..."
echo "Database: $DB_FULL_PATH"

# Run the Python script with Firebird embedded client (direct file access, no auth)
# Use platform linux/amd64 to match firebird libraries
docker run --rm \
    --platform linux/amd64 \
    -v "$SCRIPT_DIR:/app" \
    -v "$SCRIPT_DIR/probe/test-data:/firebird/data" \
    -w /app \
    python:3.11-slim \
    bash -c "
        apt-get update -qq >/dev/null 2>&1 && \
        apt-get install -qq -y libfbembed2.5 firebird2.5-common >/dev/null 2>&1 && \
        export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/firebird/2.5/lib && \
        pip install -q fdb && \
        python $SCRIPT --db \"$DB_FULL_PATH\" \"\$@\"
    " -- "$@"
