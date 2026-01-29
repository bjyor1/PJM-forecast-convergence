# db.py
#
# Postgres helpers + schema (multi-feed).
# This version:
# - Uses psycopg2-binary
# - Creates/updates tables forecast_runs and forecast_points
# - Ensures BOTH tables have a `feed` column
# - Migrates forecast_points primary key to (feed, area, run_ts, target_ts)
#   so "7day" and "vshort" can coexist without collisions
#
# Required env:
#   DATABASE_URL

import os
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env var is required")


def get_conn():
    # Use default cursor (tuples) — your app.py is already written for tuple rows.
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """
    Safe to call on every startup/ingest run. Applies idempotent migrations.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # --- forecast_runs ---
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS forecast_runs (
                  id SERIAL PRIMARY KEY,
                  feed TEXT NOT NULL DEFAULT '7day',
                  area TEXT NOT NULL,
                  run_ts TIMESTAMPTZ NOT NULL,
                  payload_hash TEXT NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            # Add feed if older table existed
            cur.execute("ALTER TABLE forecast_runs ADD COLUMN IF NOT EXISTS feed TEXT NOT NULL DEFAULT '7day';")

            # Dedupe helper (per feed+area+payload)
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_runs_feed_area_hash
                ON forecast_runs(feed, area, payload_hash);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_runs_feed_area_runts_desc
                ON forecast_runs(feed, area, run_ts DESC);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_runs_created_at
                ON forecast_runs(created_at);
                """
            )

            # --- forecast_points ---
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS forecast_points (
                  feed TEXT NOT NULL DEFAULT '7day',
                  area TEXT NOT NULL,
                  run_ts TIMESTAMPTZ NOT NULL,
                  target_ts TIMESTAMPTZ NOT NULL,
                  mw DOUBLE PRECISION NOT NULL
                );
                """
            )
            # Add feed if older table existed
            cur.execute("ALTER TABLE forecast_points ADD COLUMN IF NOT EXISTS feed TEXT NOT NULL DEFAULT '7day';")

            # Ensure forecast_points has the correct PRIMARY KEY:
            # (feed, area, run_ts, target_ts)
            # If an old PK exists (likely (area, run_ts, target_ts)), replace it.
            cur.execute(
                """
                DO $$
                DECLARE
                  pk_name text;
                  pk_def  text;
                BEGIN
                  SELECT c.conname, pg_get_constraintdef(c.oid)
                    INTO pk_name, pk_def
                  FROM pg_constraint c
                  JOIN pg_class t ON t.oid = c.conrelid
                  WHERE t.relname = 'forecast_points' AND c.contype = 'p'
                  LIMIT 1;

                  IF pk_name IS NULL THEN
                    ALTER TABLE forecast_points
                      ADD CONSTRAINT forecast_points_pkey
                      PRIMARY KEY (feed, area, run_ts, target_ts);
                  ELSIF pk_def NOT ILIKE '%feed%' THEN
                    EXECUTE format('ALTER TABLE forecast_points DROP CONSTRAINT %I', pk_name);
                    ALTER TABLE forecast_points
                      ADD CONSTRAINT forecast_points_pkey
                      PRIMARY KEY (feed, area, run_ts, target_ts);
                  END IF;
                END $$;
                """
            )

            # Helpful indexes for querying
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_points_feed_area_runts
                ON forecast_points(feed, area, run_ts);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_points_feed_area_target
                ON forecast_points(feed, area, target_ts);
                """
            )

        conn.commit()
