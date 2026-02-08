import sqlite3
from pathlib import Path

DB_PATH = Path("data/app.db")
SCHEMA_PATH = Path("db/schema.sql")

DB_PATH.parent.mkdir(exist_ok=True)

conn = sqlite3.connect(DB_PATH)
with open(SCHEMA_PATH) as f:
    conn.executescript(f.read())

conn.commit()
conn.close()

print("Database initialized.")