"""Plain-Python Kafka → UHS aggregator. Fallback when Flink isn't running."""

from __future__ import annotations

import json
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from app.config import database_url, load_config, redis_url
from app.pg import connect_pg, insert_score_history
from app.redis_store import (
    LB_DETAIL_PREFIX,
    LB_ZSET,
    OVERRIDE_PREFIX,
    client as redis_client,
    get_override,
    get_pipeline_enabled,
    get_pipeline_reset_epoch,
    set_detail,
    set_pipeline_stats,
    zadd_score,
)
from app.stats import compute_info_metrics


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def main() -> None:
    cfg = load_config()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", cfg.kafka.bootstrap_servers)
    rurl = redis_url(cfg)
    dsn = database_url(cfg)
    if not rurl or not dsn.lower().startswith("postgresql"):
        raise SystemExit("worker requires REDIS_URL and postgresql DATABASE_URL")

    topics_map = cfg.kafka.topics.model_dump()  # dim -> topic name
    topic_to_dim = {v: k for k, v in topics_map.items()}
    sensor_topics = list(topics_map.values())
    cascade_topic = cfg.kafka.cascade_topic
    all_topics = sensor_topics + [cascade_topic]

    weights = cfg.urban_health.weights.model_dump()
    window_sec = float(cfg.urban_health.aggregation_window_seconds)
    blend = float(cfg.urban_health.decay.previous_blend)
    drop_thr = float(cfg.urban_health.cascade.drop_threshold)
    penalty = float(cfg.urban_health.cascade.neighbor_penalty)
    adj = cfg.districts.adjacency

    consumer = KafkaConsumer(
        *all_topics,
        bootstrap_servers=bootstrap,
        group_id=os.environ.get("KAFKA_GROUP_ID", cfg.kafka.consumer_group),
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
    )

    r = redis_client(rurl)

    # Rolling buffer: district -> dim -> list[(ts_epoch, value)]
    buf: dict[str, dict[str, list[tuple[float, float]]]] = defaultdict(lambda: defaultdict(list))
    last_published: dict[str, float] = {}

    session_onboarded = 0   # every record pulled from Kafka (all topics)
    session_processed = 0   # records that reached the aggregator or cascade apply
    last_reset_epoch = get_pipeline_reset_epoch(r)

    last_flush = time.time()
    print(f"[worker] topics={all_topics} window={window_sec}s redis={rurl}", flush=True)

    while True:
        t_poll0 = time.perf_counter()
        batch = consumer.poll(timeout_ms=400)
        poll_ms = (time.perf_counter() - t_poll0) * 1000.0
        now = time.time()
        msgs = 0

        # Control-plane check: the API's /pipeline/{start,pause,reset} flip these
        # keys in Redis; we reread each poll so the dashboard's buttons take
        # effect within ~400 ms.
        processing_enabled = get_pipeline_enabled(r)
        cur_epoch = get_pipeline_reset_epoch(r)
        if cur_epoch != last_reset_epoch:
            session_onboarded = 0
            session_processed = 0
            last_reset_epoch = cur_epoch

        for _tp, records in batch.items():
            for rec in records:
                msgs += 1
                session_onboarded += 1
                if not processing_enabled:
                    # Pause drops records on the floor: offsets still advance
                    # (no consumer lag build-up), but nothing is buffered/applied.
                    continue
                topic = rec.topic
                val = rec.value
                if topic == cascade_topic:
                    _apply_cascade(r, dsn, val, penalty, adj)
                    session_processed += 1
                    continue
                dim = topic_to_dim.get(topic)
                if not dim:
                    continue
                district = str(val.get("district_id") or "").strip()
                if not district:
                    continue
                v = float(val.get("value") or 0.0)
                ts_s = val.get("ts")
                try:
                    ts = datetime.fromisoformat(ts_s.replace("Z", "+00:00")).timestamp() if ts_s else now
                except Exception:
                    ts = now
                buf[district][dim].append((ts, v))
                session_processed += 1

        if now - last_flush < window_sec or not processing_enabled:
            set_pipeline_stats(
                r,
                {
                    "kafka_poll_ms": round(poll_ms, 4),
                    "kafka_messages": msgs,
                    "records_onboarded": session_onboarded,
                    "records_processed": session_processed,
                    "processing_enabled": processing_enabled,
                    "window_sec": window_sec,
                    "updated_at": _now_iso(),
                },
            )
            if not processing_enabled:
                # Keep last_flush current while paused so we don't fire a
                # massive catch-up aggregation on the first window after resume.
                last_flush = now
            continue

        t_agg0 = time.perf_counter()
        last_flush = now
        window_start = now - window_sec

        # prune old samples
        for d in list(buf.keys()):
            for dim in list(buf[d].keys()):
                buf[d][dim] = [(t, v) for t, v in buf[d][dim] if t >= window_start]
                if not buf[d][dim]:
                    del buf[d][dim]
            if not buf[d]:
                del buf[d]

        agg_ms = (time.perf_counter() - t_agg0) * 1000.0
        t_write0 = time.perf_counter()

        districts_touched = set(buf.keys())
        if districts_touched:
            with connect_pg(dsn) as conn:
                for district in sorted(districts_touched):
                    # Manual override holds the leaderboard for up to
                    # OVERRIDE_TTL_SECONDS.  Don't let the stream window
                    # overwrite the operator's pinned score mid-TTL — samples
                    # still live in `buf` and will be flushed as normal on the
                    # next window after the override expires.
                    if get_override(r, district) is not None:
                        continue
                    dims = buf.get(district, {})
                    comp: dict[str, float] = {}
                    for dim_key in weights.keys():
                        samples = dims.get(dim_key, [])
                        if samples:
                            comp[dim_key] = float(sum(v for _, v in samples) / len(samples))
                        else:
                            prev = _read_prev_component(r, district, dim_key)
                            comp[dim_key] = prev if prev is not None else 70.0

                    raw_uhs = sum(float(weights[k]) * _clamp(float(comp[k])) for k in weights.keys())

                    prev_score = last_published.get(district)
                    if prev_score is not None and (prev_score - raw_uhs) >= drop_thr:
                        payload = {
                            "type": "ripple",
                            "source": district,
                            "reason": "score_drop",
                            "delta": round(prev_score - raw_uhs, 4),
                            "ts": _now_iso(),
                        }
                        producer.send(cascade_topic, payload)

                    old_redis = r.zscore(LB_ZSET, district)
                    old = float(old_redis) if old_redis is not None else None
                    blended = raw_uhs if old is None else (1.0 - blend) * raw_uhs + blend * old
                    final_score = _clamp(blended)

                    detail = {
                        "district_id": district,
                        "score": round(final_score, 4),
                        "components": {k: round(float(v), 4) for k, v in comp.items()},
                        "updated_at": _now_iso(),
                        "uhs_raw": round(raw_uhs, 4),
                    }
                    zadd_score(r, district, final_score)
                    set_detail(r, district, detail)
                    last_published[district] = final_score

                    entry_id = str(uuid.uuid4())
                    insert_score_history(
                        conn,
                        entry_id=entry_id,
                        district_id=district,
                        score=float(final_score),
                        submitted_at=detail["updated_at"],
                        source="stream",
                        meta={"components": comp, "uhs_raw": raw_uhs},
                    )

        # Emit a manual-source heartbeat row per active override so the
        # feed/history doesn't collapse to the single /add row for the full
        # 30-second pin.  Runs whether or not the district has sensor samples
        # this window (covers quick-push districts not in the simulator list).
        heartbeat_districts = [
            str(key)[len(OVERRIDE_PREFIX):]
            for key in r.scan_iter(match=OVERRIDE_PREFIX + "*", count=200)
        ]
        if heartbeat_districts:
            with connect_pg(dsn) as conn:
                for district in sorted(heartbeat_districts):
                    ov = get_override(r, district)
                    if ov is None:
                        continue
                    insert_score_history(
                        conn,
                        entry_id=str(uuid.uuid4()),
                        district_id=district,
                        score=float(ov.get("score", 0.0)),
                        submitted_at=_now_iso(),
                        source="manual",
                        meta={
                            "override_heartbeat": True,
                            "catastrophic": bool(ov.get("catastrophic", False)),
                            "remaining_seconds": int(ov.get("remaining_seconds", 0)),
                        },
                    )

        producer.flush()
        write_ms = (time.perf_counter() - t_write0) * 1000.0

        pairs = r.zrange(LB_ZSET, 0, -1, withscores=True)
        score_vals = [float(s) for _, s in pairs]
        dist = compute_info_metrics(score_vals) if score_vals else {"count": 0}

        set_pipeline_stats(
            r,
            {
                "kafka_poll_ms": round(poll_ms, 4),
                "kafka_messages": msgs,
                "records_onboarded": session_onboarded,
                "records_processed": session_processed,
                "processing_enabled": processing_enabled,
                "aggregation_ms": round(agg_ms, 4),
                "redis_pg_write_ms": round(write_ms, 4),
                "window_sec": window_sec,
                "live_districts": len(pairs),
                "score_count": dist.get("count", 0),
                "updated_at": _now_iso(),
            },
        )


def _read_prev_component(r: Any, district: str, dim: str) -> float | None:
    raw = r.get(LB_DETAIL_PREFIX + district)
    if not raw:
        return None
    try:
        d = json.loads(raw)
        c = d.get("components") or {}
        v = c.get(dim)
        return float(v) if v is not None else None
    except Exception:
        return None


def _apply_cascade(r: Any, dsn: str, val: dict[str, Any], penalty: float, adj: dict[str, list[str]]) -> None:
    src = str(val.get("source") or "").strip()
    neighbors = adj.get(src, [])
    if not neighbors:
        return
    ts = _now_iso()
    with connect_pg(dsn) as conn:
        for n in neighbors:
            if get_override(r, n) is not None:
                continue
            cur = r.zscore(LB_ZSET, n)
            base = float(cur) if cur is not None else 70.0
            new_score = _clamp(base - penalty)
            zadd_score(r, n, new_score)
            detail = {
                "district_id": n,
                "score": round(new_score, 4),
                "components": None,
                "updated_at": ts,
                "cascade_from": src,
            }
            set_detail(r, n, detail)
            entry_id = str(uuid.uuid4())
            insert_score_history(
                conn,
                entry_id=entry_id,
                district_id=n,
                score=float(new_score),
                submitted_at=ts,
                source="cascade",
                meta={"source": src, "payload": val},
            )


if __name__ == "__main__":
    main()
