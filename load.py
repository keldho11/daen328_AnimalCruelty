import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from extract import extract
from transform import transform

load_dotenv()

DB_PARAMS = {
    "dbname":   os.environ.get("DB_NAME",     "animal_db"),
    "user":     os.environ.get("DB_USER",     "postgres"),
    "password": os.environ.get("DB_PASSWORD", "postgres123"),
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "port":     os.environ.get("DB_PORT",     "5433"),
}


def create_schema(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS issue_types (
            id   SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS ticket_statuses (
            id   SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS priorities (
            id   SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS submission_methods (
            id   SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS districts (
            id   SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS tickets (
            id                    SERIAL PRIMARY KEY,
            ticket_id             VARCHAR(20)      NOT NULL UNIQUE,
            issue_type_id         INT              REFERENCES issue_types(id),
            street_address        VARCHAR(200),
            city                  VARCHAR(100),
            state                 VARCHAR(50),
            zip_code              INT,
            district_id           INT              REFERENCES districts(id),
            ticket_created_at     TIMESTAMP,
            ticket_updated_at     TIMESTAMP,
            ticket_closed_at      TIMESTAMP,
            status_id             INT              REFERENCES ticket_statuses(id),
            latitude              DOUBLE PRECISION,
            longitude             DOUBLE PRECISION,
            method_id             INT              REFERENCES submission_methods(id),
            priority_id           INT              REFERENCES priorities(id),
            actual_completed_days NUMERIC,
            arcgis_object_id      INT
        );
        CREATE INDEX IF NOT EXISTS idx_tickets_created  ON tickets (ticket_created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tickets_district ON tickets (district_id);
        CREATE INDEX IF NOT EXISTS idx_tickets_issue    ON tickets (issue_type_id);
        CREATE INDEX IF NOT EXISTS idx_tickets_status   ON tickets (status_id);
    """)


def load_lookup(cursor, table: str, values: list[str]) -> dict[str, int]:
    rows = [(v,) for v in sorted(set(v for v in values if pd.notna(v)))]
    execute_values(
        cursor,
        f"INSERT INTO {table} (name) VALUES %s ON CONFLICT (name) DO NOTHING",
        rows,
    )
    cursor.execute(f"SELECT id, name FROM {table}")
    return {name: id_ for id_, name in cursor.fetchall()}


def main():
    print("Step 1: Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    print(f"  Connected to {DB_PARAMS['dbname']} at {DB_PARAMS['host']}:{DB_PARAMS['port']}")

    print("Step 2: Creating schema...")
    create_schema(cur)
    conn.commit()

    print("Step 3: Checking for existing data...")
    cur.execute("SELECT MAX(arcgis_object_id) FROM tickets")
    max_object_id = cur.fetchone()[0]

    print("Step 4: Extracting data from API...")
    raw = extract(since_object_id=max_object_id)

    if not raw:
        print("  No new records found. Pipeline complete.")
        cur.close()
        conn.close()
        return

    print("Step 5: Transforming data...")
    df = transform(pd.DataFrame(raw))

    print("Step 6: Loading lookup tables...")
    issue_map    = load_lookup(cur, "issue_types",        df["issue_type"].tolist())
    status_map   = load_lookup(cur, "ticket_statuses",    df["ticket_status"].tolist())
    priority_map = load_lookup(cur, "priorities",         df["sr_priority"].tolist())
    method_map   = load_lookup(cur, "submission_methods", df["method_received"].tolist())
    district_map = load_lookup(cur, "districts",          df["neighborhood_district"].tolist())
    conn.commit()

    print("Step 7: Inserting tickets...")

    def to_ts(val):
        return None if pd.isna(val) else val.to_pydatetime()

    def to_int(val):
        return None if pd.isna(val) else int(val)

    rows = []
    for row in df.itertuples(index=False):
        rows.append((
            row.ticket_id,
            issue_map.get(row.issue_type),
            row.street_address if pd.notna(row.street_address) else None,
            row.city           if pd.notna(row.city)           else None,
            row.state          if pd.notna(row.state)          else None,
            to_int(row.zip_code),
            district_map.get(row.neighborhood_district),
            to_ts(row.ticket_created_date_time),
            to_ts(row.ticket_last_update_date_time),
            to_ts(row.ticket_closed_date_time),
            status_map.get(row.ticket_status),
            row.latitude  if pd.notna(row.latitude)  else None,
            row.longitude if pd.notna(row.longitude) else None,
            method_map.get(row.method_received),
            priority_map.get(row.sr_priority),
            row.actual_completed_days if pd.notna(row.actual_completed_days) else None,
            to_int(row.ObjectId),
        ))

    execute_values(
        cur,
        """
        INSERT INTO tickets (
            ticket_id, issue_type_id, street_address, city, state, zip_code,
            district_id, ticket_created_at, ticket_updated_at, ticket_closed_at,
            status_id, latitude, longitude, method_id, priority_id,
            actual_completed_days, arcgis_object_id
        ) VALUES %s
        ON CONFLICT (ticket_id) DO NOTHING
        """,
        rows,
        page_size=1000,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"  {len(rows):,} tickets inserted")
    print("ETL pipeline complete.")


if __name__ == "__main__":
    main()
