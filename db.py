import os
import psycopg
from psycopg.rows import dict_row

def get_conn():
    db_url = os.environ["DATABASE_URL"]
    # Render DATABASE_URL is usually compatible with psycopg3
    return psycopg.connect(db_url, row_factory=dict_row)

def init_db():
    schema_sql = """
    CREATE TABLE IF NOT EXISTS forecast_runs (
      area TEXT NOT NULL,
      run_ts TIMESTAMPTZ NOT NULL,
      payload_hash TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (area, run_ts),
      UNIQUE (area, payload_hash)
    );

    CREATE TABLE IF NOT EXISTS forecast_points (
      area TEXT NOT NULL,
      run_ts TIMESTAMPTZ NOT NULL,
      target_ts TIMESTAMPTZ NOT NULL,
      mw NUMERIC NOT NULL,
      PRIMARY KEY (area, run_ts, target_ts),
      FOREIGN KEY (area, run_ts) REFERENCES forecast_runs(area, run_ts) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_points_area_target_run
      ON forecast_points(area, target_ts, run_ts);

    CREATE INDEX IF NOT EXISTS idx_runs_area_run
      ON forecast_runs(area, run_ts DESC);
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
