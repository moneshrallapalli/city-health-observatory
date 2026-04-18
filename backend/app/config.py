from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ServerCfg(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000


class DatabaseCfg(BaseModel):
    url: str = "sqlite:///./data/leaderboard.db"


class RedisCfg(BaseModel):
    url: str = ""


class KafkaTopicsCfg(BaseModel):
    traffic: str = "sensor.traffic"
    aqi: str = "sensor.aqi"
    power: str = "sensor.power"
    water: str = "sensor.water"
    noise: str = "sensor.noise"


class KafkaCfg(BaseModel):
    bootstrap_servers: str = "localhost:19092"
    consumer_group: str = "uhs-stream-worker"
    topics: KafkaTopicsCfg = Field(default_factory=KafkaTopicsCfg)
    cascade_topic: str = "city.cascade"


class UrbanWeightsCfg(BaseModel):
    traffic: float = 0.25
    aqi: float = 0.30
    power: float = 0.20
    water: float = 0.10
    noise: float = 0.15


class UrbanDecayCfg(BaseModel):
    previous_blend: float = 0.15


class UrbanCascadeCfg(BaseModel):
    drop_threshold: float = 20.0
    neighbor_penalty: float = 12.0
    penalty_windows: int = 2


class UrbanHealthCfg(BaseModel):
    weights: UrbanWeightsCfg = Field(default_factory=UrbanWeightsCfg)
    aggregation_window_seconds: int = 5
    decay: UrbanDecayCfg = Field(default_factory=UrbanDecayCfg)
    cascade: UrbanCascadeCfg = Field(default_factory=UrbanCascadeCfg)


class DistrictsCfg(BaseModel):
    ids: list[str] = Field(
        default_factory=lambda: [
            "Downtown",
            "Harbor",
            "Eastside",
            "Airport",
            "Midtown",
            "Northside",
            "Industrial",
            "Uptown",
        ]
    )
    adjacency: dict[str, list[str]] = Field(default_factory=dict)


class LeaderboardCfg(BaseModel):
    top_n: int = 10


class PerformanceCfg(BaseModel):
    max_samples: int = 500


class SimulatorCfg(BaseModel):
    events_per_tick: int = 12
    tick_interval_ms: int = 200


class CorsCfg(BaseModel):
    allow_origins: list[str] = Field(default_factory=lambda: ["*"])


class AppConfig(BaseModel):
    server: ServerCfg = Field(default_factory=ServerCfg)
    database: DatabaseCfg = Field(default_factory=DatabaseCfg)
    redis: RedisCfg = Field(default_factory=RedisCfg)
    kafka: KafkaCfg = Field(default_factory=KafkaCfg)
    urban_health: UrbanHealthCfg = Field(default_factory=UrbanHealthCfg)
    districts: DistrictsCfg = Field(default_factory=DistrictsCfg)
    leaderboard: LeaderboardCfg = Field(default_factory=LeaderboardCfg)
    performance: PerformanceCfg = Field(default_factory=PerformanceCfg)
    simulator: SimulatorCfg = Field(default_factory=SimulatorCfg)
    cors: CorsCfg = Field(default_factory=CorsCfg)


def _default_config_path() -> Path:
    return Path(__file__).resolve().parent.parent / "config.yaml"


def load_config(path: Path | None = None) -> AppConfig:
    cfg_path = path or Path(os.environ.get("CONFIG_PATH", _default_config_path()))
    raw: dict[str, Any] = {}
    if cfg_path.is_file():
        with cfg_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    return AppConfig.model_validate(raw)


def redis_url(cfg: AppConfig) -> str:
    return (os.environ.get("REDIS_URL") or cfg.redis.url or "").strip()


def database_url(cfg: AppConfig) -> str:
    return (os.environ.get("DATABASE_URL") or cfg.database.url).strip()


def pipeline_enabled(cfg: AppConfig) -> bool:
    if os.environ.get("PIPELINE_MODE", "").lower() in ("0", "false", "no"):
        return False
    return bool(redis_url(cfg)) and database_url(cfg).lower().startswith("postgresql")
