#!/usr/bin/env bash
# Simple helper to start the DB viewer manually (dev-only).
# Usage:
#   cd database_sqlite/db_visualizer
#   ./start_viewer.sh
# Notes:
# - This does not run as part of the database container startup.
# - Ensure Node 18+ is installed. This will install dependencies if missing.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Install deps if missing
if [ ! -d "node_modules" ]; then
  echo "Installing dependencies..."
  npm install --no-audit --no-fund
fi

echo "Starting DB viewer on http://localhost:3000"
NODE_ENV=development node server.js --host 0.0.0.0
