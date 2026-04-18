from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator


def ensure_sqlite_dir(database_url: str) -> None:
    if database_url.startswith("sqlite:///"):
        path = database_url.replace("sqlite:///", "", 1)
        p = Path(path).resolve()
        p.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def connect(database_url: str) -> Generator[sqlite3.Connection, None, None]:
    ensure_sqlite_dir(database_url)
    path = database_url.replace("sqlite:///", "", 1)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS leaderboard_entries (
            id TEXT PRIMARY KEY,
            user TEXT NOT NULL,
            score REAL NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_leaderboard_score ON leaderboard_entries (score DESC);
        CREATE INDEX IF NOT EXISTS idx_leaderboard_user ON leaderboard_entries (user);

        CREATE TABLE IF NOT EXISTS submission_history (
            id TEXT PRIMARY KEY,
            user TEXT NOT NULL,
            score REAL NOT NULL,
            submitted_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_history_user ON submission_history (user);
        CREATE INDEX IF NOT EXISTS idx_history_submitted_at ON submission_history (submitted_at);
        """
    )
