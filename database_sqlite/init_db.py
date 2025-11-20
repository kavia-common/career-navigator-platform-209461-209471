#!/usr/bin/env python3
"""Initialize SQLite database for Career Navigator Platform (database_sqlite)

This script:
- Loads config from environment (.env recommended) with SQLITE_DB_PATH.
- Creates the full schema (roles, skills, role_skill_requirements, user_skills,
  user_progress, learning_resources, career_paths, recommendations) with FKs and indexes.
- Idempotently seeds data from JSON files in ./seeds:
    - seeds/sfia_skills.json
    - seeds/roles.json
    - seeds/learning_resources.json
- Writes db_connection.txt and db_visualizer/sqlite.env for convenience.

Run:
  python3 init_db.py

Environment:
  SQLITE_DB_PATH=/absolute/or/relative/path/to/myapp.db
"""

import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path

DEFAULT_DB = "myapp.db"
SEEDS_DIR = Path(__file__).parent / "seeds"
SKILLS_FILE = SEEDS_DIR / "sfia_skills.json"
ROLES_FILE = SEEDS_DIR / "roles.json"
RESOURCES_FILE = SEEDS_DIR / "learning_resources.json"

def load_env():
    """Load environment vars needed by this script."""
    db_path = os.getenv("SQLITE_DB_PATH", DEFAULT_DB)
    return {"db_path": db_path}

def ensure_dirs():
    """Ensure seeds dir and db_visualizer dir exist."""
    SEEDS_DIR.mkdir(parents=True, exist_ok=True)
    (Path(__file__).parent / "db_visualizer").mkdir(parents=True, exist_ok=True)

def connect(db_path: str) -> sqlite3.Connection:
    """Create a SQLite connection with foreign keys enabled."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn

def create_schema(conn: sqlite3.Connection):
    """Create all required tables, constraints, and indexes if missing.

    Also performs a lightweight migration to ensure learning_resources has a UNIQUE(title, url)
    index for new databases. For legacy databases that lack this constraint, seeds will still
    succeed by using a fallback upsert pattern.
    """
    cur = conn.cursor()

    # Core entities
    cur.execute("""
    CREATE TABLE IF NOT EXISTS skills (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,                -- e.g., "AUSM", "DLMG" for leadership-like codes
        name TEXT NOT NULL,
        category TEXT,
        level_min INTEGER,               -- min SFIA-like level suggested (1-7)
        level_max INTEGER,               -- max SFIA-like level suggested (1-7)
        description TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS roles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        family TEXT,                     -- e.g., "Engineering", "Leadership"
        seniority TEXT,                  -- e.g., "Junior", "Mid", "Senior"
        description TEXT
    )
    """)

    # Requirements: a role requires a skill at a certain target level/weight
    cur.execute("""
    CREATE TABLE IF NOT EXISTS role_skill_requirements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role_id INTEGER NOT NULL,
        skill_id INTEGER NOT NULL,
        target_level INTEGER NOT NULL,   -- 1-7 scale
        weight REAL DEFAULT 1.0,         -- relative importance
        FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE,
        FOREIGN KEY (skill_id) REFERENCES skills(id) ON DELETE CASCADE,
        UNIQUE (role_id, skill_id)
    )
    """)

    # User skill profile and progress
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_skills (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        skill_id INTEGER NOT NULL,
        level INTEGER NOT NULL,          -- user current level 1-7
        evidence TEXT,                   -- notes/links supporting level
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (user_id, skill_id),
        FOREIGN KEY (skill_id) REFERENCES skills(id) ON DELETE CASCADE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_progress (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        role_id INTEGER,                 -- optional: focus role
        skill_id INTEGER NOT NULL,
        action TEXT NOT NULL,            -- e.g., "completed_resource", "assessed_level"
        details TEXT,                    -- JSON or text blob
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE SET NULL,
        FOREIGN KEY (skill_id) REFERENCES skills(id) ON DELETE CASCADE
    )
    """)

    # Learning resources and mapping to skills
    # For new DBs, define the UNIQUE(title, url) constraint in the table DDL.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS learning_resources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        provider TEXT,
        resource_type TEXT,              -- e.g., course, article, book
        difficulty TEXT,                 -- beginner/intermediate/advanced
        description TEXT,
        UNIQUE (title, url)              -- ensure idempotent upsert on known identity
    )
    """)

    # Lightweight migration for legacy DBs: ensure a unique index exists if possible.
    try:
        # If the unique index already exists, this will do nothing.
        # If the legacy DB didn't have any duplicates, this will succeed.
        # If duplicates exist, this will raise an IntegrityError and we will skip,
        # but seeding will still work using the fallback upsert logic.
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_learning_resources_title_url ON learning_resources(title, url)")
    except sqlite3.Error:
        # Leave as-is; fallback logic will handle upserts without ON CONFLICT
        pass

    cur.execute("""
    CREATE TABLE IF NOT EXISTS learning_resource_skills (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        resource_id INTEGER NOT NULL,
        skill_id INTEGER NOT NULL,
        recommended_level_min INTEGER,   -- suitable from level
        recommended_level_max INTEGER,   -- suitable to level
        FOREIGN KEY (resource_id) REFERENCES learning_resources(id) ON DELETE CASCADE,
        FOREIGN KEY (skill_id) REFERENCES skills(id) ON DELETE CASCADE,
        UNIQUE (resource_id, skill_id)
    )
    """)

    # Career path graph: role -> next_role with recommended skills deltas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS career_paths (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_role_id INTEGER NOT NULL,
        to_role_id INTEGER NOT NULL,
        rationale TEXT,                  -- explanation
        FOREIGN KEY (from_role_id) REFERENCES roles(id) ON DELETE CASCADE,
        FOREIGN KEY (to_role_id) REFERENCES roles(id) ON DELETE CASCADE,
        UNIQUE (from_role_id, to_role_id)
    )
    """)

    # Recommendations storage (e.g., from LLM) for auditability
    cur.execute("""
    CREATE TABLE IF NOT EXISTS recommendations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        context TEXT,                    -- optional JSON of inputs
        recommendations_json TEXT NOT NULL, -- JSON blob of recommendations
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Helpful indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_role_skill_role ON role_skill_requirements(role_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_role_skill_skill ON role_skill_requirements(skill_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_skills_user ON user_skills(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_skills_skill ON user_skills(skill_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_progress_user ON user_progress(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_learning_resource_skills_res ON learning_resource_skills(resource_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_learning_resource_skills_skill ON learning_resource_skills(skill_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_career_paths_from ON career_paths(from_role_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_career_paths_to ON career_paths(to_role_id)")

    conn.commit()

def _upsert_skill(cur, skill):
    cur.execute("""
        INSERT INTO skills (code, name, category, level_min, level_max, description)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            name=excluded.name,
            category=excluded.category,
            level_min=excluded.level_min,
            level_max=excluded.level_max,
            description=excluded.description
    """, (
        skill.get("code"),
        skill["name"],
        skill.get("category"),
        skill.get("level_min"),
        skill.get("level_max"),
        skill.get("description"),
    ))

def _get_skill_id(cur, code_or_name):
    cur.execute("SELECT id FROM skills WHERE code = ? OR name = ?", (code_or_name, code_or_name))
    row = cur.fetchone()
    return row[0] if row else None

def _upsert_role(cur, role):
    cur.execute("""
        INSERT INTO roles (name, family, seniority, description)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            family=excluded.family,
            seniority=excluded.seniority,
            description=excluded.description
    """, (role["name"], role.get("family"), role.get("seniority"), role.get("description")))

def _get_role_id(cur, name):
    cur.execute("SELECT id FROM roles WHERE name = ?", (name,))
    row = cur.fetchone()
    return row[0] if row else None

def _upsert_role_skill_req(cur, role_id, skill_id, target_level, weight):
    cur.execute("""
        INSERT INTO role_skill_requirements (role_id, skill_id, target_level, weight)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(role_id, skill_id) DO UPDATE SET
            target_level=excluded.target_level,
            weight=excluded.weight
    """, (role_id, skill_id, target_level, weight))

def _has_unique_on_learning_resources(cur) -> bool:
    """
    Detect if learning_resources has a UNIQUE constraint on (title, url).
    Returns True if found, False otherwise.
    """
    try:
        cur.execute("PRAGMA index_list('learning_resources')")
        indexes = cur.fetchall() or []
        # Each row format: (seq, name, unique, origin, partial)
        for idx in indexes:
            if len(idx) >= 3 and idx[2] == 1:  # unique index
                idx_name = idx[1]
                # Inspect columns of this index
                cur.execute(f"PRAGMA index_info('{idx_name}')")
                cols = [r[2] for r in (cur.fetchall() or [])]
                if cols == ["title", "url"]:
                    return True
        # As a fallback, scan table SQL definition if available
        cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='learning_resources'")
        row = cur.fetchone()
        if row and row[0]:
            return "UNIQUE (title, url)" in row[0].replace("\n", " ").replace("\t", " ")
    except sqlite3.Error:
        pass
    return False

def _upsert_resource(cur, res):
    """
    Insert or update a learning resource idempotently.
    Uses ON CONFLICT(title, url) when a matching UNIQUE constraint exists.
    Falls back to INSERT OR IGNORE + UPDATE pattern if constraint is absent (legacy DBs).
    """
    title = res["title"]
    url = res["url"]
    provider = res.get("provider")
    resource_type = res.get("resource_type")
    difficulty = res.get("difficulty")
    description = res.get("description")

    if _has_unique_on_learning_resources(cur):
        # Preferred path with composite UNIQUE constraint
        cur.execute("""
            INSERT INTO learning_resources (title, url, provider, resource_type, difficulty, description)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(title, url) DO UPDATE SET
                provider=excluded.provider,
                resource_type=excluded.resource_type,
                difficulty=excluded.difficulty,
                description=excluded.description
        """, (title, url, provider, resource_type, difficulty, description))
    else:
        # Fallback path: safe upsert without relying on ON CONFLICT target
        # 1) Attempt to insert (ignored if duplicate)
        cur.execute("""
            INSERT OR IGNORE INTO learning_resources (title, url, provider, resource_type, difficulty, description)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (title, url, provider, resource_type, difficulty, description))
        # 2) Update existing row if it already existed
        cur.execute("""
            UPDATE learning_resources
               SET provider = ?,
                   resource_type = ?,
                   difficulty = ?,
                   description = ?
             WHERE title = ? AND url = ?
        """, (provider, resource_type, difficulty, description, title, url))

    # fetch id precisely by the composite key
    cur.execute("SELECT id FROM learning_resources WHERE title = ? AND url = ?", (title, url))
    row = cur.fetchone()
    return row[0] if row else None

def _upsert_resource_skill(cur, resource_id, skill_code_or_name, lv_min, lv_max):
    skill_id = _get_skill_id(cur, skill_code_or_name)
    if not skill_id:
        return
    cur.execute("""
        INSERT INTO learning_resource_skills (resource_id, skill_id, recommended_level_min, recommended_level_max)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(resource_id, skill_id) DO UPDATE SET
            recommended_level_min=excluded.recommended_level_min,
            recommended_level_max=excluded.recommended_level_max
    """, (resource_id, skill_id, lv_min, lv_max))

def seed_from_json(conn: sqlite3.Connection):
    """Seed skills, roles with requirements, and learning resources idempotently."""
    cur = conn.cursor()

    # Skills
    if SKILLS_FILE.exists():
        with open(SKILLS_FILE, "r", encoding="utf-8") as f:
            skills = json.load(f)
        for s in skills:
            _upsert_skill(cur, s)
        conn.commit()
        print(f"Seeded skills: {len(skills)}")
    else:
        print(f"Warning: {SKILLS_FILE} not found. Skills not seeded.")

    # Roles and role_skill_requirements
    if ROLES_FILE.exists():
        with open(ROLES_FILE, "r", encoding="utf-8") as f:
            roles = json.load(f)
        role_count = 0
        req_count = 0
        for r in roles:
            _upsert_role(cur, r)
            role_id = _get_role_id(cur, r["name"])
            if role_id and r.get("requirements"):
                for req in r["requirements"]:
                    skill_id = _get_skill_id(cur, req["skill"])
                    if skill_id:
                        _upsert_role_skill_req(
                            cur, role_id, skill_id,
                            int(req.get("target_level", 3)),
                            float(req.get("weight", 1.0))
                        )
                        req_count += 1
            role_count += 1
        conn.commit()
        print(f"Seeded roles: {role_count} and requirements: {req_count}")
    else:
        print(f"Warning: {ROLES_FILE} not found. Roles not seeded.")

    # Learning resources and mapping to skills
    if RESOURCES_FILE.exists():
        with open(RESOURCES_FILE, "r", encoding="utf-8") as f:
            resources = json.load(f)
        res_count = 0
        link_count = 0
        for res in resources:
            res_id = _upsert_resource(cur, res)
            res_count += 1
            for sk in res.get("skills", []):
                _upsert_resource_skill(
                    cur, res_id,
                    sk["skill"],
                    sk.get("level_min"),
                    sk.get("level_max"),
                )
                link_count += 1
        conn.commit()
        print(f"Seeded learning resources: {res_count} and links: {link_count}")
    else:
        print(f"Warning: {RESOURCES_FILE} not found. Learning resources not seeded.")

def write_connection_info(db_path: str):
    current_dir = os.getcwd()
    abs_db = str(Path(db_path).resolve())
    conn_str = f"sqlite:///{abs_db}"
    with open(Path(__file__).parent / "db_connection.txt", "w", encoding="utf-8") as f:
        f.write("# SQLite connection methods:\n")
        f.write(f"# Python: sqlite3.connect('{abs_db}')\n")
        f.write(f"# Connection string: {conn_str}\n")
        f.write(f"# File path: {abs_db}\n")
    # For viewer
    with open(Path(__file__).parent / "db_visualizer" / "sqlite.env", "w", encoding="utf-8") as f:
        f.write(f'export SQLITE_DB="{abs_db}"\n')

def main():
    env = load_env()
    ensure_dirs()
    db_path = env["db_path"]

    db_exists = Path(db_path).exists()
    if db_exists:
        print(f"Using existing SQLite database at {db_path}")
    else:
        print(f"Creating SQLite database at {db_path}")

    with closing(connect(db_path)) as conn:
        create_schema(conn)
        seed_from_json(conn)

    write_connection_info(db_path)

    print("\nSQLite setup complete!")
    print(f"Database: {Path(db_path).name}")
    print(f"Location: {str(Path(db_path).resolve())}")

if __name__ == "__main__":
    main()
