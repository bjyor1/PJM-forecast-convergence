from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request

from db import get_conn, init_db

app = FastAPI()
templates = Jinja2Templates(directory="templates")

ET = ZoneInfo("America/New_York")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


def et_midnight_utc(now_utc: datetime) -> datetime:
    """
    Return today's midnight in America/New_York, expressed as UTC (TIMESTAMPTZ safe).
    """
    now_et = now_utc.astimezone(ET)
    midnight_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight_et.astimezone(timezone.utc)


@app.get("/api/latest")
def latest(
    area: str = Query(default="RTO_COMBINED"),
    feed: str = Query(default="7day"),
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT feed, area, run_ts, created_at
                FROM forecast_runs
                WHERE feed=%s AND area=%s
                ORDER BY run_ts DESC
                LIMIT 1
                """,
                (feed, area),
            )
            row = cur.fetchone()

    if not row:
        return {"feed": feed, "area": area, "run_ts": None, "created_at": None}

    return {
        "feed": row[0],
        "area": row[1],
        "run_ts": row[2].isoformat(),
        "created_at": row[3].isoformat() if row[3] else None,
    }


@app.get("/api/runs")
def runs(
    area: str = Query(default="RTO_COMBINED"),
    feed: str = Query(default="7day"),
    since_hours: int = Query(default=12, ge=1, le=168),
    limit: int = Query(default=12, ge=1, le=200),
    clip_to_midnight_et: bool = Query(default=False),
):
    now_utc = datetime.now(timezone.utc)
    since_ts = now_utc - timedelta(hours=since_hours)
    midnight_cutoff_utc = et_midnight_utc(now_utc) if clip_to_midnight_et else None

    # 1) Pick the *newest* runs, then we’ll return them oldest->newest for charting
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_ts
                FROM forecast_runs
                WHERE feed=%s AND area=%s AND run_ts >= %s
                ORDER BY run_ts DESC
                LIMIT %s
                """,
                (feed, area, since_ts, limit),
            )
            run_rows = cur.fetchall()
            run_ts_desc = [r[0] for r in run_rows]

            if not run_ts_desc:
                return {
                    "feed": feed,
                    "area": area,
                    "runs": [],
                    "meta": {
                        "now_utc": now_utc.isoformat(),
                        "now_et": now_utc.astimezone(ET).isoformat(),
                        "midnight_et_utc": et_midnight_utc(now_utc).isoformat(),
                        "clip_to_midnight_et": clip_to_midnight_et,
                    },
                }

            # Re-order oldest->newest for display
            run_ts_list = list(reversed(run_ts_desc))

            # 2) Fetch points ONLY for those runs (no extra data)
            if midnight_cutoff_utc:
                cur.execute(
                    """
                    SELECT run_ts, target_ts, mw
                    FROM forecast_points
                    WHERE feed=%s AND area=%s
                      AND run_ts = ANY(%s)
                      AND target_ts >= %s
                    ORDER BY run_ts ASC, target_ts ASC
                    """,
                    (feed, area, run_ts_list, midnight_cutoff_utc),
                )
            else:
                cur.execute(
                    """
                    SELECT run_ts, target_ts, mw
                    FROM forecast_points
                    WHERE feed=%s AND area=%s
                      AND run_ts = ANY(%s)
                    ORDER BY run_ts ASC, target_ts ASC
                    """,
                    (feed, area, run_ts_list),
                )

            pts = cur.fetchall()

    runs_map = {}
    for run_ts, target_ts, mw in pts:
        rt = run_ts.isoformat()
        runs_map.setdefault(rt, []).append(
            {"target_ts": target_ts.isoformat(), "mw": float(mw)}
        )

    ordered_run_ts = [t.isoformat() for t in run_ts_list]

    return {
        "feed": feed,
        "area": area,
        "runs": [{"run_ts": rt, "points": runs_map.get(rt, [])} for rt in ordered_run_ts],
        "meta": {
            "now_utc": now_utc.isoformat(),
            "now_et": now_utc.astimezone(ET).isoformat(),
            "midnight_et_utc": et_midnight_utc(now_utc).isoformat(),
            "clip_to_midnight_et": clip_to_midnight_et,
        },
    }
