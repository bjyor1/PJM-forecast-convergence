import os, json, hashlib
from datetime import datetime, timedelta, timezone
from io import StringIO

import pandas as pd
import requests
from playwright.sync_api import sync_playwright

from db import get_conn, init_db

DATAMINER_URL = os.environ.get("DATAMINER_URL", "https://dataminer2.pjm.com/")

AREA_FILTER = os.environ.get("AREA_FILTER", "RTO_COMBINED")
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "7"))

PJM_7DAY_URL = os.environ["PJM_7DAY_URL"]
PJM_VSHORT_URL = os.environ["PJM_VSHORT_URL"]

# how far out to keep per feed
HORIZON_HOURS_7DAY = int(os.environ.get("HORIZON_HOURS_7DAY", "96"))     # extend if you want
HORIZON_HOURS_VSHORT = int(os.environ.get("HORIZON_HOURS_VSHORT", "2"))  # 5-min feed is ~2h window

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def get_subscription_key_via_browser() -> str:
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
        page.wait_for_timeout(3000)

        deadline = datetime.now(timezone.utc) + timedelta(seconds=20)
        while not sub_key["value"] and datetime.now(timezone.utc) < deadline:
            page.wait_for_timeout(250)

        context.close()
        browser.close()

    if not sub_key["value"]:
        raise RuntimeError("Could not capture Ocp-Apim-Subscription-Key from Data Miner.")
    return sub_key["value"]

def fetch_csv(url: str, sub_key: str) -> bytes:
    headers = {
        "Ocp-Apim-Subscription-Key": sub_key,
        "Accept": "text/csv,application/octet-stream,*/*",
        "Referer": "https://dataminer2.pjm.com/",
        "Origin": "https://dataminer2.pjm.com",
    }
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.content

def pick_cols(df: pd.DataFrame):
    # Be robust to column naming differences across feeds.
    cols = {c.lower(): c for c in df.columns}

    def find_one(candidates):
        for cand in candidates:
            if cand in cols:
                return cols[cand]
        return None

    col_area = find_one(["forecast_area", "area", "zone"])
    col_target = find_one([
        "forecast_datetime_beginning_utc",
        "forecast_datetime_beginning_ept",
        "forecast_datetime_beginning",
        "forecast_datetime_utc",
        "forecast_datetime_ept",
        "datetime_utc",
        "datetime_ept",
    ])
    col_mw = find_one([
        "forecast_load_mw",
        "load_forecast_mw",
        "forecast_mw",
        "mw",
    ])

    if not col_area or not col_target or not col_mw:
        raise RuntimeError(f"Could not identify required columns. Got columns: {list(df.columns)}")

    return col_area, col_target, col_mw

def ingest_feed(feed: str, url: str, horizon_hours: int, sub_key: str):
    now = datetime.now(timezone.utc)
    window_end = now + timedelta(hours=horizon_hours)

    csv_bytes = fetch_csv(url, sub_key)
    payload_hash = sha256_bytes(csv_bytes)

    df = pd.read_csv(StringIO(csv_bytes.decode("utf-8", errors="replace")))

    col_area, col_target, col_mw = pick_cols(df)

    df = df[df[col_area] == AREA_FILTER].copy()
    df["target_ts"] = pd.to_datetime(df[col_target], utc=True, errors="coerce")
    df = df.dropna(subset=["target_ts"])
    df = df[(df["target_ts"] <= window_end)]

    if df.empty:
        raise RuntimeError(f"{feed}: After filtering, 0 points remained.")

    points = [(t.to_pydatetime(), float(mw)) for t, mw in zip(df["target_ts"], df[col_mw])]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM forecast_runs WHERE feed=%s AND area=%s AND payload_hash=%s",
                (feed, AREA_FILTER, payload_hash),
            )
            if cur.fetchone():
                return {"feed": feed, "status": "skipped", "reason": "duplicate payload_hash"}

            cur.execute(
                "INSERT INTO forecast_runs(feed, area, run_ts, payload_hash) VALUES (%s,%s,%s,%s)",
                (feed, AREA_FILTER, now, payload_hash),
            )

            cur.executemany(
                "INSERT INTO forecast_points(feed, area, run_ts, target_ts, mw) VALUES (%s,%s,%s,%s,%s)",
                [(feed, AREA_FILTER, now, t, mw) for (t, mw) in points],
            )

            cutoff = now - timedelta(days=RETENTION_DAYS)
            cur.execute("DELETE FROM forecast_runs WHERE created_at < %s", (cutoff,))
            cur.execute("DELETE FROM forecast_points WHERE run_ts < %s", (cutoff,))

        conn.commit()

    return {"feed": feed, "status": "ok", "run_ts": now.isoformat(), "points": len(points)}

def ingest_once():
    init_db()
    sub_key = get_subscription_key_via_browser()

    results = []
    results.append(ingest_feed("7day", PJM_7DAY_URL, HORIZON_HOURS_7DAY, sub_key))
    results.append(ingest_feed("vshort", PJM_VSHORT_URL, HORIZON_HOURS_VSHORT, sub_key))
    return {"status": "ok", "results": results}

if __name__ == "__main__":
    print(json.dumps(ingest_once(), indent=2))
