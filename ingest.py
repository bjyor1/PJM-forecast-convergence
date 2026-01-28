import os
import json
import hashlib
from datetime import datetime, timedelta, timezone
from io import StringIO

import pandas as pd
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from db import get_conn, init_db

DATAMINER_URL = os.environ.get("DATAMINER_URL", "https://dataminer2.pjm.com/")
PJM_7DAY_URL = os.environ["PJM_7DAY_URL"]
AREA_FILTER = os.environ.get("AREA_FILTER", "RTO_COMBINED")
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "7"))
HORIZON_HOURS = int(os.environ.get("HORIZON_HOURS", "48"))


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def get_subscription_key_via_browser() -> str:
    """
    Launch headless Chromium, load Data Miner, and capture the Ocp-Apim-Subscription-Key
    from the site's own API calls.
    """
    sub_key = {"value": None}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        def on_request(req):
            if "api.pjm.com" in req.url:
                hdrs = req.headers or {}
                key = hdrs.get("ocp-apim-subscription-key")
                if key and not sub_key["value"]:
                    sub_key["value"] = key

        context.on("request", on_request)

        page = context.new_page()
        page.goto(DATAMINER_URL, wait_until="domcontentloaded", timeout=60_000)

        # Give the app a moment to fire its API calls
        try:
            page.wait_for_timeout(3000)
        except PWTimeout:
            pass

        # Wait up to 20 seconds for the key
        deadline = datetime.now(timezone.utc) + timedelta(seconds=20)
        while not sub_key["value"] and datetime.now(timezone.utc) < deadline:
            page.wait_for_timeout(500)

        context.close()
        browser.close()

    if not sub_key["value"]:
        raise RuntimeError(
            "Could not capture Ocp-Apim-Subscription-Key from Data Miner. Site may have changed."
        )
    return sub_key["value"]


def fetch_csv_with_key(sub_key: str) -> bytes:
    headers = {
        "Ocp-Apim-Subscription-Key": sub_key,
        "Accept": "text/csv,application/octet-stream,*/*",
        "Referer": "https://dataminer2.pjm.com/",
        "Origin": "https://dataminer2.pjm.com",
    }
    r = requests.get(PJM_7DAY_URL, headers=headers, timeout=60)
    r.raise_for_status()
    return r.content


def compute_points_hash(points: list[tuple[datetime, float]]) -> str:
    """
    Hash the filtered point series (not the raw CSV).
    This makes dedupe align with what you actually store/plot.
    """
    # stable ordering
    points_sorted = sorted(points, key=lambda x: x[0])
    payload = "\n".join(f"{t.replace(tzinfo=timezone.utc).isoformat()}|{mw:.3f}" for t, mw in points_sorted).encode(
        "utf-8"
    )
    return sha256_bytes(payload)


def ingest_once():
    init_db()

    now_utc = datetime.now(timezone.utc)
    window_end = now_utc + timedelta(hours=HORIZON_HOURS)

    sub_key = get_subscription_key_via_browser()
    csv_bytes = fetch_csv_with_key(sub_key)

    # Read CSV
    df = pd.read_csv(StringIO(csv_bytes.decode("utf-8", errors="replace")))

    required = {"forecast_area", "forecast_datetime_beginning_utc", "forecast_load_mw"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(
            f"CSV missing expected columns: {missing}. Got columns: {list(df.columns)[:40]} ..."
        )

    # Filter to area
    df = df[df["forecast_area"] == AREA_FILTER].copy()

    # Parse timestamps + MW
    df["target_ts"] = pd.to_datetime(
        df["forecast_datetime_beginning_utc"], utc=True, errors="coerce"
    )
    df = df.dropna(subset=["target_ts"])
    df["mw"] = pd.to_numeric(df["forecast_load_mw"], errors="coerce")
    df = df.dropna(subset=["mw"])

    # IMPORTANT: Do NOT trim the front. Only cap to now + horizon.
    df = df[df["target_ts"] < window_end].copy()

    if df.empty:
        raise RuntimeError(
            "After filtering, 0 points remained. Check AREA_FILTER, timestamp fields, or horizon."
        )

    # Build point list
    df = df.sort_values("target_ts")
    points: list[tuple[datetime, float]] = [
        (t.to_pydatetime(), float(mw)) for t, mw in zip(df["target_ts"], df["mw"])
    ]

    # Dedupe based on what we store, not the raw CSV
    payload_hash = compute_points_hash(points)

    run_ts = now_utc

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM forecast_runs WHERE area=%s AND payload_hash=%s",
                (AREA_FILTER, payload_hash),
            )
            if cur.fetchone():
                return {
                    "status": "skipped",
                    "reason": "duplicate payload_hash (filtered points unchanged)",
                    "points": len(points),
                }

            cur.execute(
                "INSERT INTO forecast_runs(area, run_ts, payload_hash) VALUES (%s,%s,%s)",
                (AREA_FILTER, run_ts, payload_hash),
            )

            cur.executemany(
                "INSERT INTO forecast_points(area, run_ts, target_ts, mw) VALUES (%s,%s,%s,%s)",
                [(AREA_FILTER, run_ts, t, mw) for (t, mw) in points],
            )

            cutoff = now_utc - timedelta(days=RETENTION_DAYS)
            cur.execute("DELETE FROM forecast_runs WHERE created_at < %s", (cutoff,))

        conn.commit()

    return {
        "status": "ok",
        "run_ts": run_ts.isoformat(),
        "points": len(points),
        "first_target_ts": points[0][0].isoformat(),
        "last_target_ts": points[-1][0].isoformat(),
    }


if __name__ == "__main__":
    res = ingest_once()
    print(json.dumps(res, indent=2))
