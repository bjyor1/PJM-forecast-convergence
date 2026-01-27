import os, json, hashlib
from datetime import datetime, timedelta, timezone
import requests

from db import get_conn, init_db

PJM_7DAY_URL = os.environ.get("PJM_7DAY_URL", "").strip()
AREA_DEFAULT = os.environ.get("AREA", "PJM RTO")
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "7"))

def floor_to_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)

def sha256_of_obj(obj) -> str:
    b = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(b).hexdigest()

def fetch_pjm_json() -> dict:
    if not PJM_7DAY_URL:
        raise RuntimeError("Set PJM_7DAY_URL env var to the Data Miner 2 endpoint you’re using.")
    r = requests.get(PJM_7DAY_URL, timeout=30)
    r.raise_for_status()
    return r.json()

def parse_points(payload: dict, area: str):
    """
    You will need to adapt this function to PJM’s exact field names for the feed response.
    Goal: yield (target_ts, mw) for the selected area.
    """
    # Common DataMiner-ish pattern: payload has 'items' list
    items = payload.get("items") or payload.get("data") or []
    for row in items:
        # TODO: adjust keys to actual response
        row_area = row.get("area") or row.get("zone") or row.get("load_area") or area
        if row_area != area:
            continue
        target = row.get("datetime_beginning") or row.get("forecast_datetime") or row.get("target_datetime")
        mw = row.get("forecast_load_mw") or row.get("mw") or row.get("load_mw")
        if target is None or mw is None:
            continue
        # Parse ISO8601 timestamps
        target_ts = datetime.fromisoformat(str(target).replace("Z", "+00:00"))
        yield target_ts, float(mw)

def ingest_once():
    init_db()

    now = datetime.now(timezone.utc)
    window_start = floor_to_hour(now)
    window_end = window_start + timedelta(hours=48)

    payload = fetch_pjm_json()
    payload_hash = sha256_of_obj(payload)

    # Use ingestion time as run_ts (you can swap to PJM publish time if present)
    run_ts = now

    # Filter to 48h window
    points = [(t, mw) for (t, mw) in parse_points(payload, AREA_DEFAULT)
              if window_start <= t < window_end]

    if not points:
        raise RuntimeError("Parsed 0 points. Update parse_points() keys to match PJM payload.")

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Dedup by payload hash
            cur.execute(
                "SELECT 1 FROM forecast_runs WHERE area=%s AND payload_hash=%s",
                (AREA_DEFAULT, payload_hash),
            )
            if cur.fetchone():
                return {"status": "skipped", "reason": "duplicate payload_hash"}

            cur.execute(
                "INSERT INTO forecast_runs(area, run_ts, payload_hash) VALUES (%s,%s,%s)",
                (AREA_DEFAULT, run_ts, payload_hash),
            )

            # Batch insert points
            cur.executemany(
                "INSERT INTO forecast_points(area, run_ts, target_ts, mw) VALUES (%s,%s,%s,%s)",
                [(AREA_DEFAULT, run_ts, t, mw) for (t, mw) in points],
            )

            # Retention cleanup
            cutoff = now - timedelta(days=RETENTION_DAYS)
            cur.execute("DELETE FROM forecast_runs WHERE created_at < %s", (cutoff,))

        conn.commit()

    return {"status": "ok", "run_ts": run_ts.isoformat(), "points": len(points)}

if __name__ == "__main__":
    res = ingest_once()
    print(json.dumps(res, indent=2))
