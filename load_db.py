# load_db.py
# Reads animal_services_clean.csv and loads it into the normalized PostgreSQL schema.
# Run once after `docker compose up`:  python load_db.py

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB_PARAMS = {
    "dbname":   os.environ.get("DB_NAME", "animal_db"),
    "user":     os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "postgres123"),
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     os.environ.get("DB_PORT", "5432"),
}

CLEAN_CSV = "animal_services_clean.csv"
DATE_COLS = [
    "ticket_created_date_time",
    "ticket_last_update_date_time",
    "ticket_closed_date_time",
]


def load_lookup(cursor, table: str, values: list[str]) -> dict[str, int]:
    """Insert unique values into a lookup table, return name→id mapping."""
    rows = [(v,) for v in sorted(set(v for v in values if pd.notna(v)))]
    execute_values(
        cursor,
        f"INSERT INTO {table} (name) VALUES %s ON CONFLICT (name) DO NOTHING",
        rows,
    )
    cursor.execute(f"SELECT id, name FROM {table}")
    return {name: id_ for id_, name in cursor.fetchall()}


def main():
    print(f"Reading {CLEAN_CSV}...")
    df = pd.read_csv(CLEAN_CSV, parse_dates=DATE_COLS, date_format="mixed")
    df["zip_code"] = pd.to_numeric(df["zip_code"], errors="coerce")
    print(f"  {len(df):,} rows loaded")

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    print("Loading lookup tables...")
    issue_map   = load_lookup(cur, "issue_types",       df["issue_type"].tolist())
    status_map  = load_lookup(cur, "ticket_statuses",   df["ticket_status"].tolist())
    priority_map= load_lookup(cur, "priorities",        df["sr_priority"].tolist())
    method_map  = load_lookup(cur, "submission_methods",df["method_received"].tolist())
    district_map= load_lookup(cur, "districts",         df["neighborhood_district"].tolist())
    conn.commit()
    print("  Lookup tables ready")

    print("Loading tickets...")

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
    print("Done.")


if __name__ == "__main__":
    main()
