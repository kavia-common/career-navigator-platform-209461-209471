# Simple DB Viewer (Optional)

A minimal Node/Express app to browse PostgreSQL, MySQL, SQLite, and MongoDB for local development.

Requirements
- Node.js v18+ (package.json engines enforces >=18)

Install and run
- SQLite example:
  cd database_sqlite/db_visualizer
  # Clean install to avoid stale modules
  rm -rf node_modules package-lock.json
  npm ci --no-audit --no-fund || npm install --no-audit --no-fund

  # Point to your SQLite DB path (init_db.py writes sqlite.env automatically)
  source sqlite.env

  # Start
  node server.js --host 0.0.0.0

Open http://localhost:3000

Notes
- This tool is optional and should not run as part of the database container in production.
- The server.js uses require('express') (not relative). If you see a MODULE_NOT_FOUND for './lib/express', do a clean install as above with Node 18+.
