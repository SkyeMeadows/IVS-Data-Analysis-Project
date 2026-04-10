"""
influx_to_sqlite.py
--------------------
Extracts all data from an InfluxDB v1.8 instance into a SQLite database.

Each InfluxDB measurement becomes a SQLite table.
Tags and fields become columns; the timestamp is stored as an ISO-8601 string
(SQLite has no native timestamp type, but this format sorts correctly and
is easy to parse with pandas or standard Python).

Usage:
    pip install influxdb pandas

    python influx_to_sqlite.py

Edit the CONFIG section below before running.
"""

import sqlite3
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd
from influxdb import DataFrameClient

# ---------------------------------------------------------------------------
# CONFIG — edit these
# ---------------------------------------------------------------------------
INFLUX_HOST     = "localhost"
INFLUX_PORT     = 8086
INFLUX_DATABASE = "IronVale"          # name of your InfluxDB database
INFLUX_USER     = "admin"              # leave empty if auth is disabled
INFLUX_PASSWORD = "IronVale"
INFLUX_SSL      = False           # set True if your server uses HTTPS

SQLITE_PATH     = "influx_export.db"   # output file (created if missing)

# How far back to extract. Set START_DATE to None to extract everything.
# If your data goes back years, narrowing this saves a lot of time.
START_DATE = datetime(2026, 1, 1, tzinfo=timezone.utc)                 # e.g. datetime(2023, 1, 1, tzinfo=timezone.utc)
END_DATE   = datetime.now(timezone.utc)

# Weekly chunks — safe for ~1 GB datasets. Reduce to timedelta(days=1) if
# a single measurement is very high-frequency.
CHUNK_SIZE = timedelta(weeks=1)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── helpers ────────────────────────────────────────────────────────────────

def get_client() -> DataFrameClient:
    return DataFrameClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT,
        username=INFLUX_USER,
        password=INFLUX_PASSWORD,
        database=INFLUX_DATABASE,
        ssl=INFLUX_SSL,
        verify_ssl=INFLUX_SSL,
    )


def list_measurements(client: DataFrameClient) -> list[str]:
    result = client.query("SHOW MEASUREMENTS")
    if not result:
        return []
    rows = list(result["measurements"])   # generator → list
    return [r["name"] for r in rows]


def get_time_range(client: DataFrameClient, measurement: str):
    """Return the actual (first, last) timestamps stored for a measurement."""
    q_first = f'SELECT * FROM "{measurement}" ORDER BY time ASC  LIMIT 1'
    q_last  = f'SELECT * FROM "{measurement}" ORDER BY time DESC LIMIT 1'
    r_first = client.query(q_first)
    r_last  = client.query(q_last)
    if not r_first or not r_last:
        return None, None
    df_first = list(r_first.values())[0]
    df_last  = list(r_last.values())[0]
    return df_first.index[0], df_last.index[0]


def sanitize(name: str) -> str:
    """Turn an arbitrary InfluxDB name into a safe SQLite identifier."""
    return name.replace(" ", "_").replace("-", "_").replace(".", "_")


# ── schema helpers ──────────────────────────────────────────────────────────

def ensure_table(conn: sqlite3.Connection, table: str, df: pd.DataFrame):
    """Create table if absent; add any new columns that appeared mid-stream."""
    cur = conn.cursor()

    # Check if the table already exists
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    exists = cur.fetchone() is not None

    if not exists:
        # Build CREATE TABLE from the dataframe columns
        col_defs = ["timestamp TEXT NOT NULL"]
        for col in df.columns:
            col_defs.append(f'"{sanitize(col)}" TEXT')
        ddl = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(col_defs)})'
        cur.execute(ddl)
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{table}_ts" ON "{table}" (timestamp)'
        )
        conn.commit()
        log.info("    Created table '%s' with %d columns.", table, len(df.columns) + 1)
    else:
        # Add any columns the current chunk introduced
        cur.execute(f'PRAGMA table_info("{table}")')
        existing = {row[1] for row in cur.fetchall()}
        for col in df.columns:
            safe = sanitize(col)
            if safe not in existing:
                cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{safe}" TEXT')
                log.info("    Added new column '%s' to '%s'.", safe, table)
        conn.commit()


def insert_chunk(conn: sqlite3.Connection, table: str, df: pd.DataFrame):
    """Insert a dataframe chunk into the SQLite table."""
    if df.empty:
        return

    # Reset index so the DatetimeIndex becomes a plain column
    df = df.copy()
    df.index = df.index.tz_convert("UTC") if df.index.tzinfo else df.index
    df.index = df.index.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    df.index.name = "timestamp"
    df = df.reset_index()

    # Sanitize column names
    df.columns = [sanitize(c) for c in df.columns]

    # Convert everything to strings — SQLite TEXT is universal and avoids
    # type-mismatch issues when schemas evolve across chunks.
    df = df.astype(str).replace("nan", None)

    cols = ", ".join(f'"{c}"' for c in df.columns)
    placeholders = ", ".join("?" for _ in df.columns)
    sql = f'INSERT OR IGNORE INTO "{table}" ({cols}) VALUES ({placeholders})'
    conn.executemany(sql, df.itertuples(index=False, name=None))
    conn.commit()


# ── main extraction ─────────────────────────────────────────────────────────

def extract_measurement(
    client: DataFrameClient,
    conn: sqlite3.Connection,
    measurement: str,
):
    table = sanitize(measurement)
    log.info("  Measurement: '%s'  →  table: '%s'", measurement, table)

    # Discover actual time range in the data
    data_start, data_end = get_time_range(client, measurement)
    if data_start is None:
        log.info("    Empty measurement, skipping.")
        return

    # Respect user-supplied bounds
    chunk_start = max(data_start, START_DATE) if START_DATE else data_start
    chunk_end_limit = min(data_end, END_DATE)

    log.info(
        "    Range: %s  →  %s",
        chunk_start.strftime("%Y-%m-%d"),
        chunk_end_limit.strftime("%Y-%m-%d"),
    )

    total_rows = 0
    current = chunk_start

    while current <= chunk_end_limit:
        window_end = min(current + CHUNK_SIZE, chunk_end_limit + timedelta(seconds=1))

        # RFC3339 timestamps for the query
        t0 = current.strftime("%Y-%m-%dT%H:%M:%SZ")
        t1 = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        query = (
            f'SELECT * FROM "{measurement}" '
            f"WHERE time >= '{t0}' AND time < '{t1}'"
        )

        try:
            result = client.query(query)
        except Exception as exc:
            log.warning("    Query failed for window %s–%s: %s", t0, t1, exc)
            current = window_end
            continue

        if result:
            df = list(result.values())[0]
            if not df.empty:
                ensure_table(conn, table, df)
                insert_chunk(conn, table, df)
                total_rows += len(df)
                log.info(
                    "    %s – %s : %d rows  (total so far: %d)",
                    t0[:10], t1[:10], len(df), total_rows,
                )

        current = window_end

    log.info("    Done. Total rows written: %d", total_rows)


def main():
    log.info("Connecting to InfluxDB at %s:%d / %s", INFLUX_HOST, INFLUX_PORT, INFLUX_DATABASE)
    client = get_client()

    # Quick connectivity check
    try:
        client.ping()
    except Exception as exc:
        log.error("Cannot reach InfluxDB: %s", exc)
        raise SystemExit(1)

    measurements = list_measurements(client)
    if not measurements:
        log.warning("No measurements found in database '%s'.", INFLUX_DATABASE)
        return

    log.info("Found %d measurement(s): %s", len(measurements), measurements)

    conn = sqlite3.connect(SQLITE_PATH)
    # Speed up bulk inserts
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")

    for i, measurement in enumerate(measurements, 1):
        log.info("[%d/%d] Processing measurement '%s' ...", i, len(measurements), measurement)
        try:
            extract_measurement(client, conn, measurement)
        except Exception as exc:
            log.error("  Failed on '%s': %s — continuing with next.", measurement, exc)

    conn.close()
    client.close()
    log.info("All done! SQLite database written to: %s", SQLITE_PATH)


if __name__ == "__main__":
    main()
