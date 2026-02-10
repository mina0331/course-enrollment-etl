import sqlite3
from sqlalchemy import create_engine, text
import pandas as pd

SQLITE_PATH = "data/app.db"
PG_URL = "postgresql+psycopg2://app:app@localhost:5433/appdb"

TABLES_IN_ORDER = [
    "course",
    "section",
    "professor",
    "section_professor",
    "raw_sis_data",
    "professor_rating_raw_html"
]

def copy_table(table: str, sqlite_conn, pg_engine):
    df = pd.read_sql_query(f'SELECT * FROM "{table}"', sqlite_conn)
    print(f"{table}: {len(df)} rows")

    if table == "course":
        # booleans: SQLite often stores as 0/1 ints
        for col in ["has_discussion", "has_lab"]:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int).astype(bool)

    # Fast bulk load (replaces existing table content)
    with pg_engine.begin() as pg_conn:
        pg_conn.execute(text(f'TRUNCATE TABLE "{table}" CASCADE'))
    df.to_sql(table, pg_engine, if_exists="append", index=False, method="multi", chunksize=5000)

def main():
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    pg_engine = create_engine(PG_URL)

    for t in TABLES_IN_ORDER:
        copy_table(t, sqlite_conn, pg_engine)

    sqlite_conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
