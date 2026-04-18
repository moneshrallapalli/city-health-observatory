from __future__ import annotations

import math
import statistics
from typing import Any


def _percentile(sorted_vals: list[float], p: float) -> float | None:
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_vals[int(k)])
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return float(d0 + d1)


def compute_info_metrics(scores: list[float]) -> dict[str, Any]:
    if not scores:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "std_dev": None,
            "min": None,
            "max": None,
            "q1": None,
            "q3": None,
            "iqr": None,
            "p10": None,
            "p90": None,
            "skewness": None,
            "histogram": [],
            "percentile_ranks_by_score": [],
        }

    s = sorted(scores)
    mean = float(statistics.mean(s))
    median = float(statistics.median(s))
    std = float(statistics.pstdev(s)) if len(s) > 1 else 0.0
    q1 = _percentile(s, 25.0)
    q3 = _percentile(s, 75.0)
    iqr = float(q3 - q1) if q1 is not None and q3 is not None else None

    # Simple moment skewness (population)
    m2 = sum((x - mean) ** 2 for x in s) / len(s)
    m3 = sum((x - mean) ** 3 for x in s) / len(s)
    skew = (m3 / (m2**1.5)) if m2 > 1e-12 else 0.0

    lo, hi = s[0], s[-1]
    bins = 10
    if hi == lo:
        histogram = [{"bin_start": lo, "bin_end": hi, "count": len(s)}]
    else:
        width = (hi - lo) / bins
        edges = [lo + i * width for i in range(bins + 1)]
        counts = [0] * bins
        for x in s:
            idx = min(bins - 1, int((x - lo) / width))
            counts[idx] += 1
        histogram = [
            {"bin_start": edges[i], "bin_end": edges[i + 1], "count": counts[i]} for i in range(bins)
        ]

    # Percentile rank for each distinct score value (tie-aware mid-rank)
    distinct: dict[float, list[int]] = {}
    for i, x in enumerate(s):
        distinct.setdefault(x, []).append(i)
    pr_by_score: list[dict[str, Any]] = []
    n = len(s)
    for val in sorted(distinct.keys()):
        ranks = [i + 1 for i in distinct[val]]  # 1-based ranks
        mid_rank = sum(ranks) / len(ranks)
        pr = 100.0 * (mid_rank - 0.5) / n if n else 0.0
        pr_by_score.append({"score": val, "percentile_rank": round(pr, 4)})

    return {
        "count": len(s),
        "mean": round(mean, 6),
        "median": round(median, 6),
        "std_dev": round(std, 6),
        "min": float(lo),
        "max": float(hi),
        "q1": None if q1 is None else round(float(q1), 6),
        "q3": None if q3 is None else round(float(q3), 6),
        "iqr": None if iqr is None else round(float(iqr), 6),
        "p10": None if (p10 := _percentile(s, 10.0)) is None else round(float(p10), 6),
        "p90": None if (p90 := _percentile(s, 90.0)) is None else round(float(p90), 6),
        "skewness": round(float(skew), 6),
        "histogram": histogram,
        "percentile_ranks_by_score": pr_by_score,
    }
