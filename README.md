# career-navigator-platform-209461-209471

Database (SQLite) setup for Career Navigator Platform

- Container: database_sqlite
- Purpose: Stores roles, skills, requirements, user skill profiles, learning resources, career paths, and recommendation history.

Quick start
1) Copy .env.example to .env inside database_sqlite and adjust path if needed
   cp database_sqlite/.env.example database_sqlite/.env
   # Set SQLITE_DB_PATH to desired database location (default ./myapp.db)

2) Initialize schema and seed data
   cd database_sqlite
   python3 init_db.py

3) Inspect the DB (optional)
   - Use the interactive shell:
     python3 db_shell.py
   - Or the simple viewer (Node.js):
     source db_visualizer/sqlite.env
     node db_visualizer/server.js
     # Open http://localhost:3000 and choose SQLite

What gets created
- Tables: roles, skills, role_skill_requirements, user_skills, user_progress,
  learning_resources, learning_resource_skills, career_paths, recommendations.
- Data: 20 SFIA/leadership-like skills, 15+ roles with 10–20 skill requirements each,
  and learning resources mapped to related skills.

Seeding files (database_sqlite/seeds)
- sfia_skills.json
- roles.json
- learning_resources.json

Idempotency
- Re-running init_db.py will upsert skills, roles, role requirements, and learning resources without duplicating records.