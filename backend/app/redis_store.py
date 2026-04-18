from __future__ import annotations

import json
from typing import Any

import redis

LB_ZSET = "lb:scores"
LB_DETAIL_PREFIX = "lb:detail:"
PIPE_STATS = "pipeline:stats"
PIPE_ENABLED = "pipeline:enabled"
PIPE_RESET_EPOCH = "pipeline:reset_epoch"
OVERRIDE_PREFIX = "override:"
OVERRIDE_TTL_SECONDS = 30
SUPPRESS_PREFIX = "suppress:"


def client(url: str) -> redis.Redis:
    return redis.Redis.from_url(url, decode_responses=True)


def set_detail(r: redis.Redis, district_id: str, payload: dict[str, Any]) -> None:
    r.set(LB_DETAIL_PREFIX + district_id, json.dumps(payload))


def get_detail(r: redis.Redis, district_id: str) -> dict[str, Any] | None:
    raw = r.get(LB_DETAIL_PREFIX + district_id)
    if not raw:
        return None
    return json.loads(raw)


def zadd_score(r: redis.Redis, district_id: str, score: float) -> None:
    r.zadd(LB_ZSET, {district_id: float(score)})


def zrem(r: redis.Redis, district_id: str) -> int:
    return int(r.zrem(LB_ZSET, district_id))


def set_suppress(r: redis.Redis, district_id: str) -> None:
    r.set(SUPPRESS_PREFIX + district_id, "1")


def clear_suppress(r: redis.Redis, district_id: str) -> int:
    return int(r.delete(SUPPRESS_PREFIX + district_id))


def is_suppressed(r: redis.Redis, district_id: str) -> bool:
    return r.exists(SUPPRESS_PREFIX + district_id) > 0


def clear_override(r: redis.Redis, district_id: str) -> int:
    return int(r.delete(OVERRIDE_PREFIX + district_id))


def top_n(r: redis.Redis, n: int) -> list[tuple[str, float]]:
    rows = r.zrevrange(LB_ZSET, 0, n - 1, withscores=True)
    out: list[tuple[str, float]] = []
    for member, score in rows:
        out.append((str(member), float(score)))
    return out


def all_scores(r: redis.Redis) -> list[float]:
    rows = r.zrange(LB_ZSET, 0, -1, withscores=True)
    return [float(s) for _, s in rows]


def set_pipeline_stats(r: redis.Redis, stats: dict[str, Any]) -> None:
    r.set(PIPE_STATS, json.dumps(stats))


def get_pipeline_stats(r: redis.Redis) -> dict[str, Any] | None:
    raw = r.get(PIPE_STATS)
    if not raw:
        return None
    return json.loads(raw)


def get_pipeline_enabled(r: redis.Redis) -> bool:
    v = r.get(PIPE_ENABLED)
    return True if v is None else v == "1"


def set_pipeline_enabled(r: redis.Redis, enabled: bool) -> None:
    r.set(PIPE_ENABLED, "1" if enabled else "0")


def bump_pipeline_reset_epoch(r: redis.Redis) -> int:
    return int(r.incr(PIPE_RESET_EPOCH))


def get_pipeline_reset_epoch(r: redis.Redis) -> int:
    v = r.get(PIPE_RESET_EPOCH)
    return int(v) if v else 0


def set_override(r: redis.Redis, district_id: str, score: float, catastrophic: bool, ttl: int = OVERRIDE_TTL_SECONDS) -> None:
    payload = {"score": float(score), "catastrophic": bool(catastrophic), "ttl_seconds": ttl}
    r.set(OVERRIDE_PREFIX + district_id, json.dumps(payload), ex=ttl)


def get_override(r: redis.Redis, district_id: str) -> dict[str, Any] | None:
    raw = r.get(OVERRIDE_PREFIX + district_id)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    ttl = r.ttl(OVERRIDE_PREFIX + district_id)
    data["remaining_seconds"] = int(ttl) if ttl and ttl > 0 else 0
    return data


def state_snapshot(r: redis.Redis, district_ids: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    known = set(district_ids)
    for member in r.zrange(LB_ZSET, 0, -1):
        known.add(str(member))
    for key in r.scan_iter(match=LB_DETAIL_PREFIX + "*", count=200):
        known.add(str(key)[len(LB_DETAIL_PREFIX):])
    ordered: list[str] = list(district_ids) + sorted(known - set(district_ids))
    for d in ordered:
        det = get_detail(r, d)
        score = r.zscore(LB_ZSET, d)
        if det is None and score is None:
            entry: dict[str, Any] = {"district_id": d, "score": None, "components": None, "updated_at": None}
        elif det is not None:
            entry = det
        else:
            entry = {"district_id": d, "score": float(score), "components": None, "updated_at": None}
        ov = get_override(r, d)
        if ov is not None:
            entry["override_active"] = True
            entry["override_remaining_seconds"] = ov.get("remaining_seconds", 0)
            entry["override_score"] = ov.get("score")
        else:
            # Override TTL expired. /add bakes override_active=True into the
            # cached detail so the badge ships on the same poll as the zadd;
            # clear those fields here or the UI keeps showing "override · 30s"
            # forever once the Redis key is gone.
            entry.pop("override_active", None)
            entry.pop("override_remaining_seconds", None)
            entry.pop("override_score", None)
            # Any non-seed district whose override has expired is "dead" now —
            # the simulator only feeds config-listed ids, so there will never be
            # another pipeline update to refresh it.  Evict regardless of
            # whether the worker has since overwritten detail with stream data
            # (which strips the source=="manual" marker /add initially wrote).
            if d not in district_ids:
                r.delete(LB_DETAIL_PREFIX + d)
                r.zrem(LB_ZSET, d)
                continue
        out.append(entry)
    return out
