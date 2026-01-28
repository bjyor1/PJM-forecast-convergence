from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request

from db import get_conn, init_db

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


def _table_has_feed_column(cur, table_name: str) -> bool:
    """
    Backward compatible: detect whether the DB schema has the 'feed' column.
    """
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = %s AND column_name = 'feed'
        LIMIT 1
        """,
        (table_name,),
    )
    return cur.fetchone() is not None


@app.get("/api/latest")
def latest(
    area: str = Query(default="RTO_COMBINED"),
    feed: str = Query(default="7day", description="Dataset/feed name: 7day or vshort"),
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            has_feed_runs = _table_has_feed_column(cur, "forecast_runs")

            if has_feed_runs:
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
            else:
                # Old schema fallback (ignores feed)
                cur.execute(
                    """
                    SELECT area, run_ts, created_at
                    FROM forecast_runs
                    WHERE area=%s
                    ORDER BY run_ts DESC
                    LIMIT 1
                    """,
                    (area,),
                )

            row = cur.fetchone()
            return row or {}


@app.get("/api/runs")
def runs(
    area: str = Query(default="RTO_COMBINED"),
    feed: str = Query(default="7day", description="Dataset/feed name: 7day or vshort"),
    since_hours: int = Query(default=12, ge=1, le=168),
):
    since_ts = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    with get_conn() as conn:
        with conn.cursor() as cur:
            has_feed_runs = _table_has_feed_column(cur, "forecast_runs")
            has_feed_pts = _table_has_feed_column(cur, "forecast_points")

            # 1) Get list of run timestamps
            if has_feed_runs:
                cur.execute(
                    """
                    SELECT run_ts
                    FROM forecast_runs
                    WHERE feed=%s AND area=%s AND run_ts >= %s
                    ORDER BY run_ts ASC
                    """,
                    (feed, area, since_ts),
                )
            else:
                cur.execute(
                    """
                    SELECT run_ts
                    FROM forecast_runs
                    WHERE area=%s AND run_ts >= %s
                    ORDER BY run_ts ASC
                    """,
                    (area, since_ts),
                )

            run_rows = cur.fetchall()
            run_ts_list = [r["run_ts"] for r in run_rows]

            if not run_ts_list:
                # Keep response shape stable
                return {"feed": feed, "area": area, "runs": []}

            # 2) Fetch points for those runs
            if has_feed_pts:
                cur.execute(
                    """
                    SELECT run_ts, target_ts, mw
                    FROM forecast_points
                    WHERE feed=%s AND area=%s AND run_ts >= %s
                    ORDER BY run_ts ASC, target_ts ASC
                    """,
                    (feed, area, since_ts),
                )
            else:
                cur.execute(
                    """
                    SELECT run_ts, target_ts, mw
                    FROM forecast_points
                    WHERE area=%s AND run_ts >= %s
                    ORDER BY run_ts ASC, target_ts ASC
                    """,
                    (area, since_ts),
                )

            pts = cur.fetchall()

    # Group points by run_ts
    runs_map = {}
    for p in pts:
        rt = p["run_ts"].isoformat()
        runs_map.setdefault(rt, []).append(
            {"target_ts": p["target_ts"].isoformat(), "mw": float(p["mw"])}
        )

    # Preserve run ordering
    ordered_run_ts = [t.isoformat() for t in run_ts_list]

    return {
        "feed": feed,
        "area": area,
        "runs": [{"run_ts": rt, "points": runs_map.get(rt, [])} for rt in ordered_run_ts],
    }
