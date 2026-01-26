#!/bin/bash
# Run the probe inside the probe container (which has Firebird + Python + fdb)
#
# Usage:
#   ./run-probe.sh                    # Start the probe
#   ./run-probe.sh --reset            # Reset database state (drop triggers/CHANGES_LOG) and start
#   ./run-probe.sh --reset-and-exit   # Reset database state and exit
#
# The probe runs inside the container because fdb (Firebird Python driver) requires
# the x86 architecture Firebird client libraries.

set -e

# Ensure probe container is running
if ! docker ps --format '{{.Names}}' | grep -q '^probe$'; then
    echo "Error: 'probe' container is not running."
    echo "Start it with: docker start probe"
    exit 1
fi

# Check container health
HEALTH=$(docker inspect probe --format='{{.State.Health.Status}}' 2>/dev/null || echo "unknown")
if [ "$HEALTH" != "healthy" ]; then
    echo "Warning: probe container health status is '$HEALTH' (expected 'healthy')"
    echo "Firebird may not be fully ready. Waiting 5 seconds..."
    sleep 5
fi

echo "Running probe inside container..."
echo "Press Ctrl+C to stop"
echo ""

# Run main.py with --env flag to load .env configuration
# Pass through any arguments (--reset, --reset-and-exit, etc.)
docker exec -it -w /apis/probe probe python3 main.py --env "$@"
