#!/bin/bash
# Convenience wrapper for running Python scripts inside the store-sim container.
#
# Usage:
#   ./run-in-store.sh schema_discovery.py --output schema.json
#   ./run-in-store.sh testdata.py add articulos 100
#   ./run-in-store.sh testdata.py list
#
# Uses the existing 'probe' container (tristanblake/microsip-env) which has
# Firebird + Python + fdb already configured. Project is mounted at /apis.

docker exec -w /apis probe python3 "$@"
