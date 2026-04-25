from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass(slots=True)
class EventEnvelope:
    """Simple event model for local streaming-compatible processing."""

    topic: str
    key: str
    payload: dict[str, Any]
    event_time: datetime = field(default_factory=lambda: datetime.now(UTC))


class LocalStreamBuffer:
    """Accumulate events and emit deterministic micro-batches."""

    def __init__(self) -> None:
        self._events: list[EventEnvelope] = []

    def publish(self, topic: str, key: str, payload: dict[str, Any]) -> EventEnvelope:
        envelope = EventEnvelope(topic=topic, key=key, payload=payload)
        self._events.append(envelope)
        return envelope

    def drain(self, topic: str | None = None) -> list[EventEnvelope]:
        if topic is None:
            events = list(self._events)
            self._events.clear()
            return events

        matched = [event for event in self._events if event.topic == topic]
        self._events = [event for event in self._events if event.topic != topic]
        return matched
