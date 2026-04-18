from __future__ import annotations

import json
import os
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, model_validator

from app.config import database_url, load_config, pipeline_enabled, redis_url
from app.db import connect as connect_sqlite, init_schema as init_sqlite_schema
from app.pg import connect_pg, fetch_history, insert_score_history
from app.redis_store import (
    LB_DETAIL_PREFIX,
    LB_ZSET,
    OVERRIDE_PREFIX,
    OVERRIDE_TTL_SECONDS,
    all_scores as all_redis_scores,
    bump_pipeline_reset_epoch,
    clear_override,
    clear_suppress,
    client as redis_client,
    get_pipeline_enabled,
    get_pipeline_stats,
    set_detail,
    set_override,
    set_pipeline_enabled,
    set_suppress,
    state_snapshot,
    top_n,
    zadd_score,
    zrem,
)
from app.stats import compute_info_metrics

_cfg = load_config()
if os.environ.get("DATABASE_URL"):
    _cfg.database.url = os.environ["DATABASE_URL"]
if os.environ.get("REDIS_URL"):
    _cfg.redis.url = os.environ["REDIS_URL"]

_PIPELINE = pipeline_enabled(_cfg)

app = FastAPI(title="Smart City Leaderboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cfg.cors.allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_timing_samples: deque[tuple[str, float]] = deque(maxlen=_cfg.performance.max_samples)


@app.middleware("http")
async def timing_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    path = request.url.path
    _timing_samples.append((path, elapsed_ms))
    response.headers["X-Response-Time-Ms"] = f"{elapsed_ms:.3f}"
    return response


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class AddBody(BaseModel):
    district: str | None = Field(default=None, max_length=128)
    user: str | None = Field(default=None, max_length=128)
    score: float
    catastrophic: bool = False

    @model_validator(mode="after")
    def _need_label(self):
        if not (self.district and self.district.strip()) and not (self.user and self.user.strip()):
            raise ValueError("Provide district or user (district preferred).")
        return self


class SensorBody(BaseModel):
    district: str = Field(min_length=1, max_length=128)
    sensor: str = Field(min_length=1, max_length=32)
    value: float = Field(ge=0.0, le=100.0)

    @model_validator(mode="after")
    def _check_sensor(self):
        allowed = {"traffic", "aqi", "power", "water", "noise"}
        if self.sensor not in allowed:
            raise ValueError(f"sensor must be one of {sorted(allowed)}")
        return self


class RemoveBody(BaseModel):
    district: str | None = Field(default=None)
    id: str | None = Field(default=None)

    @model_validator(mode="after")
    def _need_target(self):
        if not (self.district and self.district.strip()) and not (self.id and self.id.strip()):
            raise ValueError("Provide district or id.")
        return self


def _resolve_district(body: AddBody) -> str:
    if body.district and body.district.strip():
        return body.district.strip()
    return (body.user or "").strip()


def _resolve_remove_target(body: RemoveBody) -> str:
    if body.district and body.district.strip():
        return body.district.strip()
    return (body.id or "").strip()


@app.on_event("startup")
def startup() -> None:
    if _PIPELINE:
        r = redis_client(redis_url(_cfg))
        r.ping()
        with connect_pg(database_url(_cfg)) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return
    with connect_sqlite(database_url(_cfg)) as conn:
        init_sqlite_schema(conn)


@app.post("/add", status_code=201)
def add_entry(body: AddBody) -> dict[str, Any]:
    district = _resolve_district(body)
    score = float(body.score)
    if body.catastrophic:
        score = min(score, 25.0)
    entry_id = str(uuid.uuid4())
    ts = _now_iso()

    if _PIPELINE:
        r = redis_client(redis_url(_cfg))
        clear_suppress(r, district)

        # Fire a cascade ripple to neighbors when /add looks catastrophic —
        # either explicitly flagged, or a drop >= drop_threshold vs. the current
        # Redis score (same rule the stream worker uses on window drops).
        drop_thr = float(_cfg.urban_health.cascade.drop_threshold)
        prev_raw = r.zscore(LB_ZSET, district)
        prev_score = float(prev_raw) if prev_raw is not None else None
        big_drop = prev_score is not None and (prev_score - score) >= drop_thr
        if body.catastrophic or big_drop:
            payload = {
                "type": "ripple",
                "source": district,
                "reason": "manual_catastrophic" if body.catastrophic else "manual_drop",
                "delta": round((prev_score - score) if prev_score is not None else 0.0, 4),
                "ts": ts,
            }
            try:
                _get_producer().send(_cfg.kafka.cascade_topic, payload)
            except Exception as e:
                print(f"[api] cascade publish failed for {district}: {e}", flush=True)

        set_override(r, district, score, body.catastrophic, ttl=OVERRIDE_TTL_SECONDS)
        zadd_score(r, district, score)
        set_detail(
            r,
            district,
            {
                "district_id": district,
                "score": round(score, 4),
                "components": None,
                "updated_at": ts,
                "source": "manual",
                "catastrophic": body.catastrophic,
                "override_active": True,
                "override_remaining_seconds": OVERRIDE_TTL_SECONDS,
                "override_score": round(score, 4),
            },
        )
        with connect_pg(database_url(_cfg)) as conn:
            insert_score_history(
                conn,
                entry_id=entry_id,
                district_id=district,
                score=score,
                submitted_at=ts,
                source="manual",
                meta={"catastrophic": body.catastrophic},
            )
        return {"id": entry_id, "district": district, "user": district, "score": score, "created_at": ts}

    with connect_sqlite(database_url(_cfg)) as conn:
        conn.execute(
            "INSERT INTO leaderboard_entries (id, user, score, created_at) VALUES (?, ?, ?, ?)",
            (entry_id, district, score, ts),
        )
        conn.execute(
            "INSERT INTO submission_history (id, user, score, submitted_at) VALUES (?, ?, ?, ?)",
            (entry_id, district, score, ts),
        )
    return {"id": entry_id, "district": district, "user": district, "score": score, "created_at": ts}


_kafka_producer = None


def _get_producer():
    global _kafka_producer
    if _kafka_producer is None:
        from kafka import KafkaProducer

        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", _cfg.kafka.bootstrap_servers)
        _kafka_producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=5,
        )
    return _kafka_producer


@app.post("/sensor", status_code=202)
def publish_sensor(body: SensorBody) -> dict[str, Any]:
    if not _PIPELINE:
        raise HTTPException(status_code=409, detail="Sensor publishing requires the Kafka pipeline.")
    topics = _cfg.kafka.topics.model_dump()
    topic = topics.get(body.sensor)
    if not topic:
        raise HTTPException(status_code=400, detail=f"No topic configured for sensor '{body.sensor}'")
    ts = _now_iso()
    payload = {"district_id": body.district, "sensor": body.sensor, "value": float(body.value), "ts": ts}
    try:
        producer = _get_producer()
        producer.send(topic, payload).get(timeout=5)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Kafka publish failed: {e}") from e
    return {"published": payload, "topic": topic}


@app.post("/remove")
def remove_entry(body: RemoveBody) -> dict[str, Any]:
    target = _resolve_remove_target(body)
    if _PIPELINE:
        r = redis_client(redis_url(_cfg))
        removed = zrem(r, target)
        if removed == 0:
            raise HTTPException(status_code=404, detail="District not on leaderboard")
        r.delete(LB_DETAIL_PREFIX + target)
        clear_override(r, target)
        set_suppress(r, target)
        return {"removed_district": target, "removed_id": target, "suppressed": True}

    with connect_sqlite(database_url(_cfg)) as conn:
        cur = conn.execute("DELETE FROM leaderboard_entries WHERE id = ?", (target,))
        if cur.rowcount == 0:
            cur2 = conn.execute("DELETE FROM leaderboard_entries WHERE user = ?", (target,))
            if cur2.rowcount == 0:
                raise HTTPException(status_code=404, detail="Entry not found")
    return {"removed_district": target, "removed_id": target}


@app.get("/leaderboard", response_class=HTMLResponse)
def leaderboard() -> str:
    top_n_val = _cfg.leaderboard.top_n
    rows: list[tuple[str, str, float, str, str]] = []

    if _PIPELINE:
        r = redis_client(redis_url(_cfg))
        for i, (district, score) in enumerate(top_n(r, top_n_val), start=1):
            rows.append((str(i), district, float(score), district, "Kafka → Flink → Redis"))
    else:
        with connect_sqlite(database_url(_cfg)) as conn:
            q = conn.execute(
                """
                SELECT id, user, score, created_at
                FROM leaderboard_entries
                ORDER BY score DESC, created_at ASC
                LIMIT ?
                """,
                (top_n_val,),
            ).fetchall()
            for i, row in enumerate(q, start=1):
                rows.append(
                    (
                        str(i),
                        str(row["user"]),
                        float(row["score"]),
                        str(row["id"]),
                        str(row["created_at"]),
                    )
                )

    medals = ["🥇", "🥈", "🥉"]
    rows_html = ""
    for i, (_rank, label, score, rid, extra) in enumerate(rows, start=1):
        medal = medals[i - 1] if i <= 3 else ""
        rows_html += (
            f"<tr><td class='rank'>{medal} {i}</td>"
            f"<td>{_html_escape(label)}</td>"
            f"<td class='score'>{score:,.4f}</td>"
            f"<td class='mono'>{_html_escape(rid)}</td>"
            f"<td class='muted'>{_html_escape(extra)}</td></tr>"
        )

    col_district = "District" if _PIPELINE else "User"
    title = "Urban Health Leaderboard" if _PIPELINE else "Leaderboard"
    subtitle = "Top districts by composite UHS (Kafka → Flink → Redis)" if _PIPELINE else "Top entries"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Top {top_n_val} {title}</title>
  <style>
    :root {{
      font-family: ui-sans-serif, system-ui, Segoe UI, Roboto, Helvetica, Arial;
      color: #0f172a;
      background: radial-gradient(1200px 600px at 20% 0%, #e0f2fe, #f8fafc);
    }}
    body {{ margin: 0; padding: 32px; }}
    .card {{
      max-width: 980px; margin: 0 auto; background: #fff; border-radius: 16px;
      box-shadow: 0 10px 30px rgba(15,23,42,.08); overflow: hidden; border: 1px solid #e2e8f0;
    }}
    h1 {{
      margin: 0; padding: 20px 24px; font-size: 22px;
      background: linear-gradient(90deg, #0369a1, #7c3aed); color: #fff;
    }}
    .sub {{ padding: 10px 24px; font-size: 13px; color: #64748b; background: #f8fafc; border-bottom: 1px solid #e2e8f0; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 12px 14px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
    th {{ font-size: 12px; letter-spacing: .06em; text-transform: uppercase; color: #64748b; }}
    tr:hover td {{ background: #f1f5f9; }}
    .rank {{ width: 90px; font-weight: 700; }}
    .score {{ font-variant-numeric: tabular-nums; font-weight: 700; color: #0ea5e9; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas; font-size: 12px; color: #334155; }}
    .muted {{ color: #94a3b8; font-size: 12px; }}
    .foot {{ padding: 12px 16px; font-size: 12px; color: #64748b; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>{title} — Top {top_n_val}</h1>
    <div class="sub">{subtitle}</div>
    <table>
      <thead><tr><th>Rank</th><th>{col_district}</th><th>Score</th><th>Key / ID</th><th>Meta / trace</th></tr></thead>
      <tbody>{rows_html or "<tr><td colspan='5' style='padding:18px'>No entries yet.</td></tr>"}</tbody>
    </table>
    <div class="foot">REST: pretty-printed HTML table (case requirement).</div>
  </div>
</body>
</html>"""


def _html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


@app.get("/info")
def info() -> dict[str, Any]:
    if _PIPELINE:
        r = redis_client(redis_url(_cfg))
        scores = all_redis_scores(r)
        metrics = compute_info_metrics(scores)
        return {"source": "redis_live_leaderboard", "mode": "pipeline", **metrics}

    with connect_sqlite(database_url(_cfg)) as conn:
        scores = [float(r[0]) for r in conn.execute("SELECT score FROM leaderboard_entries").fetchall()]
    metrics = compute_info_metrics(scores)
    return {"source": "sqlite_leaderboard_entries", "mode": "legacy", **metrics}


@app.get("/performance")
def performance() -> dict[str, Any]:
    base: dict[str, Any]
    if not _timing_samples:
        base = {"overall_avg_ms": None, "by_endpoint": {}, "sample_count": 0}
    else:
        total = 0.0
        by_path: defaultdict[str, list[float]] = defaultdict(list)
        for path, ms in _timing_samples:
            by_path[path].append(ms)
            total += ms
        n = len(_timing_samples)
        base = {
            "overall_avg_ms": round(total / n, 4),
            "by_endpoint": {p: round(sum(v) / len(v), 4) for p, v in sorted(by_path.items())},
            "sample_count": n,
            "window_max": _cfg.performance.max_samples,
        }

    if _PIPELINE:
        r = redis_client(redis_url(_cfg))
        pipe = get_pipeline_stats(r) or {}
        # Authoritative state lives on the control key — merge it in so the UI
        # reflects a toggle immediately, even before the worker's next stats write.
        pipe["processing_enabled"] = get_pipeline_enabled(r)
        base["pipeline"] = pipe
        base["mode"] = "pipeline"
    else:
        base["mode"] = "legacy"
    return base


@app.get("/history")
def history(
    user: str | None = None,
    district: str | None = None,
    from_date: str | None = Query(None, alias="from"),
    to_date: str | None = Query(None, alias="to"),
    limit: int = Query(200, ge=1, le=2000),
) -> dict[str, Any]:
    filt_district = (district or user or "").strip() or None

    if _PIPELINE:
        with connect_pg(database_url(_cfg)) as conn:
            rows = fetch_history(conn, district=filt_district, from_date=from_date, to_date=to_date, limit=limit)
        items = []
        for r in rows:
            items.append(
                {
                    "id": r["id"],
                    "district_id": r["district_id"],
                    "user": r["district_id"],
                    "score": float(r["score"]),
                    "submitted_at": r["submitted_at"],
                    "source": r.get("source"),
                    "meta": r.get("meta"),
                }
            )
        return {
            "count": len(items),
            "filters": {"user": filt_district, "district": filt_district, "from": from_date, "to": to_date, "limit": limit},
            "items": items,
        }

    clauses: list[str] = []
    params: list[Any] = []
    if filt_district:
        clauses.append("user = ?")
        params.append(filt_district)
    if from_date:
        clauses.append("submitted_at >= ?")
        params.append(from_date)
    if to_date:
        clauses.append("submitted_at <= ?")
        params.append(to_date)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT id, user, score, submitted_at
        FROM submission_history
        {where}
        ORDER BY submitted_at DESC
        LIMIT ?
    """
    params.append(limit)
    with connect_sqlite(database_url(_cfg)) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    items = [
        {
            "id": r["id"],
            "district_id": r["user"],
            "user": r["user"],
            "score": float(r["score"]),
            "submitted_at": r["submitted_at"],
            "source": None,
            "meta": None,
        }
        for r in rows
    ]
    return {
        "count": len(items),
        "filters": {"user": filt_district, "district": filt_district, "from": from_date, "to": to_date, "limit": limit},
        "items": items,
    }


@app.post("/pipeline/start", status_code=202)
def pipeline_start() -> dict[str, Any]:
    if not _PIPELINE:
        raise HTTPException(status_code=409, detail="Kafka pipeline disabled")
    r = redis_client(redis_url(_cfg))
    set_pipeline_enabled(r, True)
    return {"processing_enabled": True}


@app.post("/pipeline/pause", status_code=202)
def pipeline_pause() -> dict[str, Any]:
    if not _PIPELINE:
        raise HTTPException(status_code=409, detail="Kafka pipeline disabled")
    r = redis_client(redis_url(_cfg))
    set_pipeline_enabled(r, False)
    return {"processing_enabled": False}


@app.post("/pipeline/reset", status_code=202)
def pipeline_reset() -> dict[str, Any]:
    if not _PIPELINE:
        raise HTTPException(status_code=409, detail="Kafka pipeline disabled")
    r = redis_client(redis_url(_cfg))
    epoch = bump_pipeline_reset_epoch(r)
    return {"reset_epoch": epoch}


@app.get("/state")
def state() -> dict[str, Any]:
    if not _PIPELINE:
        return {"mode": "legacy", "districts": [], "note": "Redis pipeline disabled; run docker compose for live city state."}
    r = redis_client(redis_url(_cfg))
    snaps = state_snapshot(r, _cfg.districts.ids)
    return {"mode": "pipeline", "districts": snaps, "kafka_bootstrap": _cfg.kafka.bootstrap_servers}


@app.get("/openapi.yaml")
def openapi_yaml() -> FileResponse:
    from pathlib import Path

    path = Path(__file__).resolve().parent.parent / "openapi.yaml"
    return FileResponse(str(path), media_type="application/yaml", filename="openapi.yaml")


_client_dir = os.path.join(os.path.dirname(__file__), "..", "..", "client")
if os.path.isdir(_client_dir):
    app.mount("/ui", StaticFiles(directory=_client_dir, html=True), name="ui")


@app.get("/")
def root() -> Response:
    return Response(status_code=307, headers={"Location": "/ui/"})
