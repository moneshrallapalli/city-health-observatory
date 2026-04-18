"""Flink DataStream job — per-district UHS over 5s tumbling windows."""

from __future__ import annotations

import json
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time

import redis as redis_lib
import psycopg
from psycopg.types.json import Json


# Mirrors backend/config.yaml so the Flink image stays self-contained.
WEIGHTS: dict[str, float] = {
    "traffic": 0.25,
    "aqi": 0.30,
    "power": 0.20,
    "water": 0.10,
    "noise": 0.15,
}

ADJACENCY: dict[str, list[str]] = {
    "Downtown": ["Midtown", "Harbor", "Eastside"],
    "Harbor": ["Downtown", "Industrial"],
    "Eastside": ["Downtown", "Airport"],
    "Airport": ["Eastside", "Industrial"],
    "Midtown": ["Downtown", "Northside", "Uptown"],
    "Northside": ["Midtown", "Industrial"],
    "Industrial": ["Harbor", "Airport", "Northside"],
    "Uptown": ["Midtown"],
}

BLEND = 0.15
DROP_THRESHOLD = 20.0
NEIGHBOR_PENALTY = 12.0
WINDOW_SECONDS = 5

LB_ZSET = "lb:scores"
LB_DETAIL = "lb:detail:"
PIPE_STATS_KEY = "pipeline:stats"
OVERRIDE_PREFIX = "override:"
SUPPRESS_PREFIX = "suppress:"


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class UHSWindowProcessor(ProcessWindowFunction):
    """One tumbling-window aggregation per district key."""

    def open(self, runtime_context: RuntimeContext) -> None:
        redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
        self._redis = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        self._dsn = os.environ.get(
            "DATABASE_URL", "postgresql://app:app@postgres:5432/citylb"
        )
        self._last_published: dict[str, float] = {}

    def process(
        self, key: str, context: ProcessWindowFunction.Context, elements: Iterable[str]
    ) -> Iterable[str]:
        dim_values: dict[str, list[float]] = defaultdict(list)
        count = 0
        for raw in elements:
            try:
                event = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            sensor = str(event.get("sensor", ""))
            value = float(event.get("value", 0))
            dim_values[sensor].append(value)
            count += 1

        if count == 0:
            return

        # If the district was removed via /remove, drop the window.
        if self._redis.exists(SUPPRESS_PREFIX + key):
            return

        components: dict[str, float] = {}
        for dim in WEIGHTS:
            if dim in dim_values:
                vals = dim_values[dim]
                components[dim] = sum(vals) / len(vals)
            else:
                prev = self._read_prev_component(key, dim)
                components[dim] = prev if prev is not None else 70.0

        raw_uhs = sum(WEIGHTS[k] * _clamp(components[k]) for k in WEIGHTS)

        override_raw = self._redis.get(OVERRIDE_PREFIX + key)
        override_ttl = self._redis.ttl(OVERRIDE_PREFIX + key)
        override_active = bool(override_raw) and isinstance(override_ttl, int) and override_ttl > 0

        if override_active:
            try:
                manual_score = float(json.loads(override_raw).get("score", 0.0))
            except Exception:
                manual_score = 0.0
            final_score = _clamp(manual_score)
            source_label = "manual-override"
        else:
            old_redis = self._redis.zscore(LB_ZSET, key)
            old = float(old_redis) if old_redis is not None else None
            blended = raw_uhs if old is None else (1.0 - BLEND) * raw_uhs + BLEND * old
            final_score = _clamp(blended)
            source_label = "flink"

        ts = _now_iso()

        self._redis.zadd(LB_ZSET, {key: final_score})
        detail: dict[str, Any] = {
            "district_id": key,
            "score": round(final_score, 4),
            "components": {k: round(v, 4) for k, v in components.items()},
            "updated_at": ts,
            "uhs_raw": round(raw_uhs, 4),
            "source": source_label,
        }
        if override_active:
            detail["override_active"] = True
            detail["override_remaining_seconds"] = int(override_ttl)
            detail["override_score"] = round(final_score, 4)
        self._redis.set(LB_DETAIL + key, json.dumps(detail))

        entry_id = uuid.uuid4()
        with psycopg.connect(self._dsn, autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO score_history
                       (id, district_id, score, submitted_at, source, meta)
                       VALUES (%s, %s, %s, %s::timestamptz, %s, %s)""",
                    (
                        entry_id,
                        key,
                        float(final_score),
                        ts,
                        source_label,
                        Json({"components": components, "uhs_raw": raw_uhs, "override_active": override_active}),
                    ),
                )
            conn.commit()

        prev_published = self._last_published.get(key)
        if prev_published is not None and (prev_published - raw_uhs) >= DROP_THRESHOLD:
            self._apply_cascade(key, prev_published - raw_uhs)

        self._last_published[key] = final_score
        self._update_pipeline_stats(key, final_score, count)

        yield json.dumps({"district": key, "score": round(final_score, 4)})

    def _read_prev_component(self, district: str, dim: str) -> float | None:
        raw = self._redis.get(LB_DETAIL + district)
        if not raw:
            return None
        try:
            d = json.loads(raw)
            return float(d.get("components", {}).get(dim))
        except Exception:
            return None

    def _apply_cascade(self, source: str, delta: float) -> None:
        neighbors = ADJACENCY.get(source, [])
        if not neighbors:
            return
        ts = _now_iso()
        for n in neighbors:
            cur = self._redis.zscore(LB_ZSET, n)
            base = float(cur) if cur is not None else 70.0
            new_score = _clamp(base - NEIGHBOR_PENALTY)
            self._redis.zadd(LB_ZSET, {n: new_score})
            cascade_detail: dict[str, Any] = {
                "district_id": n,
                "score": round(new_score, 4),
                "components": None,
                "updated_at": ts,
                "cascade_from": source,
                "source": "flink-cascade",
            }
            self._redis.set(LB_DETAIL + n, json.dumps(cascade_detail))
            with psycopg.connect(self._dsn, autocommit=False) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO score_history
                           (id, district_id, score, submitted_at, source, meta)
                           VALUES (%s, %s, %s, %s::timestamptz, %s, %s)""",
                        (
                            uuid.uuid4(),
                            n,
                            float(new_score),
                            ts,
                            "flink-cascade",
                            Json({"source_district": source, "delta": round(delta, 4)}),
                        ),
                    )
                conn.commit()

    def _update_pipeline_stats(self, district: str, score: float, events: int) -> None:
        self._redis.set(
            PIPE_STATS_KEY,
            json.dumps(
                {
                    "processor": "apache-flink",
                    "last_district": district,
                    "last_score": round(score, 4),
                    "window_events": events,
                    "window_sec": WINDOW_SECONDS,
                    "updated_at": _now_iso(),
                }
            ),
        )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    topics = [
        "sensor.traffic",
        "sensor.aqi",
        "sensor.power",
        "sensor.water",
        "sensor.noise",
    ]

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(*topics)
        .set_group_id("flink-uhs-processor")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "kafka-sensors"
    )

    processed = (
        ds.key_by(lambda raw: json.loads(raw).get("district_id", "unknown"))
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .process(UHSWindowProcessor(), Types.STRING())
    )

    processed.print()

    print(f"[flink-job] Submitting UHS pipeline: bootstrap={bootstrap} topics={topics}")
    env.execute("Smart City UHS Pipeline")


if __name__ == "__main__":
    main()
