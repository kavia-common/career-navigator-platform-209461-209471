# database_sqlite

Career Navigator Platform - SQLite database container

Overview
- Schema aligned to SFIA/leadership-like concepts:
  - roles, skills, role_skill_requirements
  - user_skills, user_progress
  - learning_resources, learning_resource_skills
  - career_paths, recommendations
- Idempotent seeding from JSON files in ./seeds

Requirements
- Python 3.9+
- sqlite3 runtime available
- Optional: Node.js (for the simple DB viewer)
  - Node v18+ recommended (package engines enforce >=18)

Environment
- Copy .env.example to .env and set:
  - SQLITE_DB_PATH: Path to SQLite file (default ./myapp.db)

Initialize
- Create schema and seed:
  cd database_sqlite
  python3 init_db.py

Re-run to re-seed safely (upserts rather than duplicates).

Legacy note on learning_resources upsert
- New databases include a UNIQUE(title, url) constraint to support ON CONFLICT upserts.
- init_db.py now attempts a lightweight migration by creating a unique index on (title, url) if it does not exist: CREATE UNIQUE INDEX IF NOT EXISTS ux_learning_resources_title_url ON learning_resources(title, url).
  - If duplicates exist in a legacy DB, this migration will be skipped and seeding will still work using a fallback pattern (INSERT OR IGNORE + UPDATE).
- If you want to enforce uniqueness on older DBs with duplicates, first deduplicate rows in learning_resources by (title, url), then re-run init_db.py.

Seed files
- seeds/sfia_skills.json
- seeds/roles.json
- seeds/learning_resources.json

Utilities
- db_shell.py: Interactive SQLite shell with helpers (.tables, .schema, .describe).
- db_visualizer/server.js: Simple viewer for multiple DB types (optional, not started by default).
  - Load env:
    source db_visualizer/sqlite.env
  - Start viewer (Node 18+):
    cd db_visualizer
    # Clean install to avoid MODULE_NOT_FOUND issues
    rm -rf node_modules package-lock.json
    npm ci --no-audit --no-fund || npm install --no-audit --no-fund
    node server.js --host 0.0.0.0
  - Open http://localhost:3000

Notes
- This container exposes no external API. The FastAPI backend should connect using the SQLITE_DB_PATH.
- Foreign keys are enforced (PRAGMA foreign_keys = ON).
- Production: Do NOT start the db_visualizer as part of the database container; it is a dev tool only.
