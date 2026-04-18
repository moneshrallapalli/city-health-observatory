from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Generator
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json


@contextmanager
def connect_pg(dsn: str) -> Generator[psycopg.Connection, None, None]:
    conn = psycopg.connect(dsn, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_score_history(
    conn: psycopg.Connection,
    *,
    entry_id: str,
    district_id: str,
    score: float,
    submitted_at: str,
    source: str,
    meta: dict[str, Any] | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO score_history (id, district_id, score, submitted_at, source, meta)
            VALUES (%s, %s, %s, %s::timestamptz, %s, %s)
            """,
            (UUID(entry_id), district_id, float(score), submitted_at, source, Json(meta or {})),
        )


def fetch_history(
    conn: psycopg.Connection,
    *,
    district: str | None,
    from_date: str | None,
    to_date: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if district:
        clauses.append("district_id = %s")
        params.append(district)
    if from_date:
        clauses.append("submitted_at >= %s::timestamptz")
        params.append(from_date)
    if to_date:
        clauses.append("submitted_at <= %s::timestamptz")
        params.append(to_date)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT id::text AS id, district_id, score, submitted_at::text AS submitted_at, source, meta
        FROM score_history
        {where}
        ORDER BY submitted_at DESC
        LIMIT %s
    """
    params.append(limit)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, tuple(params))
        rows = list(cur.fetchall())
    for r in rows:
        m = r.get("meta")
        if isinstance(m, (dict, list)):
            continue
        if m is None:
            r["meta"] = {}
        elif isinstance(m, str):
            r["meta"] = json.loads(m)
    return rows
