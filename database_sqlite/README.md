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

Environment
- Copy .env.example to .env and set:
  - SQLITE_DB_PATH: Path to SQLite file (default ./myapp.db)

Initialize
- Create schema and seed:
  cd database_sqlite
  python3 init_db.py

Re-run to re-seed safely (upserts rather than duplicates).

Seed files
- seeds/sfia_skills.json
- seeds/roles.json
- seeds/learning_resources.json

Utilities
- db_shell.py: Interactive SQLite shell with helpers (.tables, .schema, .describe).
- db_visualizer/server.js: Simple viewer for multiple DB types.
  - Load env:
    source db_visualizer/sqlite.env
  - Start viewer:
    node db_visualizer/server.js
  - Open http://localhost:3000

Notes
- This container exposes no external API. The FastAPI backend should connect using the SQLITE_DB_PATH.
- Foreign keys are enforced (PRAGMA foreign_keys = ON).
