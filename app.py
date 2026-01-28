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

@app.get("/api/latest")
def latest(area: str = Query(default="RTO_COMBINED")):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT area, run_ts, created_at FROM forecast_runs WHERE area=%s ORDER BY run_ts DESC LIMIT 1",
                (area,),
            )
            row = cur.fetchone()
            return row or {}

@app.get("/api/runs")
def runs(
    area: str = Query(default="RTO_COMBINED"),
    since_hours: int = Query(default=12, ge=1, le=168),
):
    since_ts = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT run_ts FROM forecast_runs WHERE area=%s AND run_ts >= %s ORDER BY run_ts ASC",
                (area, since_ts),
            )
            run_rows = cur.fetchall()
            run_ts_list = [r["run_ts"] for r in run_rows]

            if not run_ts_list:
                return {"area": area, "runs": []}

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

    # group points by run_ts
    runs_map = {}
    for p in pts:
        rt = p["run_ts"].isoformat()
        runs_map.setdefault(rt, []).append(
            {"target_ts": p["target_ts"].isoformat(), "mw": float(p["mw"])}
        )

    return {
        "area": area,
        "runs": [{"run_ts": rt, "points": runs_map.get(rt, [])} for rt in [t.isoformat() for t in run_ts_list]],
    }

