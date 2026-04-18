"""Publishes synthetic sensor readings to Kafka for the UHS pipeline."""

from __future__ import annotations

import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from app.config import load_config


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    cfg = load_config()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", cfg.kafka.bootstrap_servers)
    topics = cfg.kafka.topics.model_dump()
    districts = cfg.districts.ids
    tick_ms = int(os.environ.get("SIMULATOR_TICK_MS", str(cfg.simulator.tick_interval_ms)))
    n_events = int(os.environ.get("SIMULATOR_EVENTS_PER_TICK", str(cfg.simulator.events_per_tick)))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
    )

    dim_keys = list(topics.keys())
    print(f"[simulator] bootstrap={bootstrap} districts={len(districts)} tick_ms={tick_ms}", flush=True)

    while True:
        for _ in range(n_events):
            d = random.choice(districts)
            dim = random.choice(dim_keys)
            topic = topics[dim]
            # Occasional dip pushes the window low enough to trigger the cascade.
            if random.random() < 0.02:
                value = float(random.uniform(5, 35))
            else:
                value = float(random.uniform(45, 98))
            payload = {"district_id": d, "sensor": dim, "value": value, "ts": _now_iso()}
            producer.send(topic, payload)
        producer.flush()
        time.sleep(tick_ms / 1000.0)


if __name__ == "__main__":
    main()
