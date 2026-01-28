# db.py
#
# Postgres helpers for PJM forecast convergence.
# Supports multiple feeds (e.g., "7day" hourly and "vshort" 5-min).
#
# Required env var:
#   DATABASE_URL   e.g. postgres://user:pass@host:5432/dbname
#
# Optional:
#   PGSSLMODE      e.g. "require" (Render often needs SSL; libpq defaults usually work)

import os
import psycopg2
from contextlib import contextmanager

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
  raise RuntimeError("DATABASE_URL env var is required")

def get_conn():
  # Render Postgres typically works fine with just DATABASE_URL.
  # If you hit SSL issues, set PGSSLMODE=require in Render env.
  return psycopg2.connect(DATABASE_URL)

def init_db():
  """
  Create tables if missing and apply lightweight migrations (ADD COLUMN IF NOT EXISTS).
  Safe to call on every ingest/web startup.
  """
  with get_conn() as conn:
    with conn.cursor() as cur:
      # ---- forecast_runs ----
      cur.execute("""
      CREATE TABLE IF NOT EXISTS forecast_runs (
        id SERIAL PRIMARY KEY,
        feed TEXT NOT NULL DEFAULT '7day',
        area TEXT NOT NULL,
        run_ts TIMESTAMPTZ NOT NULL,
        payload_hash TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      """)

      # lightweight migrations if table existed before feeds:
      cur.execute("ALTER TABLE forecast_runs ADD COLUMN IF NOT EXISTS feed TEXT NOT NULL DEFAULT '7day';")

      # uniqueness / dedupe: per feed + area + payload
      cur.execute("""
      CREATE UNIQUE INDEX IF NOT EXISTS uq_runs_feed_area_hash
      ON forecast_runs(feed, area, payload_hash);
      """)

      cur.execute("""
      CREATE INDEX IF NOT EXISTS idx_runs_feed_area_runts_desc
      ON forecast_runs(feed, area, run_ts DESC);
      """)

      cur.execute("""
      CREATE INDEX IF NOT EXISTS idx_runs_created_at
      ON forecast_runs(created_at);
      """)

      # ---- forecast_points ----
      cur.execute("""
      CREATE TABLE IF NOT EXISTS forecast_points (
        id SERIAL PRIMARY KEY,
        feed TEXT NOT NULL DEFAULT '7day',
        area TEXT NOT NULL,
        run_ts TIMESTAMPTZ NOT NULL,
        target_ts TIMESTAMPTZ NOT NULL,
        mw DOUBLE PRECISION NOT NULL
      );
      """)

      cur.execute("ALTER TABLE forecast_points ADD COLUMN IF NOT EXISTS feed TEXT NOT NULL DEFAULT '7day';")

      # Prevent accidental duplicates if a run is reinserted
      cur.execute("""
      CREATE UNIQUE INDEX IF NOT EXISTS uq_points_feed_area_run_target
      ON forecast_points(feed, area, run_ts, target_ts);
      """)

      cur.execute("""
      CREATE INDEX IF NOT EXISTS idx_points_feed_area_runts
      ON forecast_points(feed, area, run_ts);
      """)

      cur.execute("""
      CREATE INDEX IF NOT EXISTS idx_points_feed_area_target
      ON forecast_points(feed, area, target_ts);
      """)

    conn.commit()

@contextmanager
def db_cursor():
  """
  Convenience context manager.
  Usage:
    with db_cursor() as cur:
      cur.execute(...)
      rows = cur.fetchall()
  Commits at exit if no exception.
  """
  conn = get_conn()
  try:
    with conn.cursor() as cur:
      yield cur
    conn.commit()
  except Exception:
    conn.rollback()
    raise
  finally:
    conn.close()
