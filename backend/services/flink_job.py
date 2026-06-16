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


# Single source of truth is backend/config.yaml, copied next to this job in the
# Flink image (see Dockerfile.flink).  We load it at startup and fall back to the
# constants below only if it can't be read — that keeps the districts/weights
# from silently drifting out of sync with the rest of the system.
_FALLBACK_WEIGHTS: dict[str, float] = {
    "traffic": 0.25,
    "aqi": 0.30,
    "power": 0.20,
    "water": 0.10,
    "noise": 0.15,
}
_FALLBACK_ADJACENCY: dict[str, list[str]] = {}
_FALLBACK_CASCADE = {"drop_threshold": 20.0, "neighbor_penalty": 12.0, "penalty_windows": 2}


def _load_config() -> dict[str, Any]:
    try:
        import yaml  # available in the Flink image (see Dockerfile.flink)

        cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        with open(cfg_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as exc:  # pragma: no cover - defensive: never crash the job
        print(f"[flink] config.yaml load failed ({exc}); using fallback constants", flush=True)
        return {}


_CFG = _load_config()
_uh = _CFG.get("urban_health", {})
_cascade_cfg = {**_FALLBACK_CASCADE, **_uh.get("cascade", {})}

WEIGHTS: dict[str, float] = _uh.get("weights") or _FALLBACK_WEIGHTS
ADJACENCY: dict[str, list[str]] = _CFG.get("districts", {}).get("adjacency") or _FALLBACK_ADJACENCY
BLEND = float(_uh.get("decay", {}).get("previous_blend", 0.15))
DROP_THRESHOLD = float(_cascade_cfg["drop_threshold"])
NEIGHBOR_PENALTY = float(_cascade_cfg["neighbor_penalty"])
PENALTY_WINDOWS = int(_cascade_cfg["penalty_windows"])
WINDOW_SECONDS = int(_uh.get("aggregation_window_seconds", 5))

LB_ZSET = "lb:scores"
LB_DETAIL = "lb:detail:"
PIPE_STATS_KEY = "pipeline:stats"
OVERRIDE_PREFIX = "override:"
SUPPRESS_PREFIX = "suppress:"
# Redis counter of remaining penalty windows per district.  A cascade sets it to
# PENALTY_WINDOWS; each window flush for that district applies one penalty and
# decrements — Redis-backed because Flink may key districts across task slots.
CASCADE_PENDING_PREFIX = "cascade_pending:"


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

        # Apply one window of any scheduled cascade penalty (overrides are immune
        # while pinned).  The counter persists the ripple for PENALTY_WINDOWS.
        cascade_from: str | None = None
        if not override_active:
            cascade_from = self._consume_cascade_penalty(key)
            if cascade_from is not None:
                final_score = _clamp(final_score - NEIGHBOR_PENALTY)
                source_label = "flink-cascade"

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
        if cascade_from is not None:
            detail["cascade_from"] = cascade_from
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
            self._schedule_cascade(key, prev_published - raw_uhs)

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

    def _schedule_cascade(self, source: str, delta: float) -> None:
        """Schedule a per-window penalty for each neighbor for PENALTY_WINDOWS
        windows.  The deduction itself happens in each neighbor's own window via
        ``_consume_cascade_penalty`` so the ripple decays over time rather than
        landing as a single one-shot hit."""
        neighbors = ADJACENCY.get(source, [])
        if not neighbors:
            return
        # TTL guards against a stranded counter if a neighbor stops receiving data.
        ttl = max(1, PENALTY_WINDOWS) * max(1, WINDOW_SECONDS) * 4
        for n in neighbors:
            self._redis.set(
                CASCADE_PENDING_PREFIX + n,
                json.dumps({"windows": PENALTY_WINDOWS, "source": source}),
                ex=ttl,
            )

    def _consume_cascade_penalty(self, district: str) -> str | None:
        """If a cascade penalty is scheduled for ``district``, decrement it and
        return the originating district; otherwise return None."""
        raw = self._redis.get(CASCADE_PENDING_PREFIX + district)
        if not raw:
            return None
        try:
            data = json.loads(raw)
            windows = int(data.get("windows", 0))
            source = str(data.get("source", ""))
        except Exception:
            self._redis.delete(CASCADE_PENDING_PREFIX + district)
            return None
        if windows <= 0:
            self._redis.delete(CASCADE_PENDING_PREFIX + district)
            return None
        windows -= 1
        if windows > 0:
            ttl = max(1, PENALTY_WINDOWS) * max(1, WINDOW_SECONDS) * 4
            self._redis.set(
                CASCADE_PENDING_PREFIX + district,
                json.dumps({"windows": windows, "source": source}),
                ex=ttl,
            )
        else:
            self._redis.delete(CASCADE_PENDING_PREFIX + district)
        return source or None

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
