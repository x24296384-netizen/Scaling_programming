"""Incremental analytics over a recent event-time sliding window."""

from __future__ import annotations

import heapq
from collections import Counter
from datetime import datetime, timedelta, timezone
from itertools import count
from typing import Any


class SlidingWindowAnalytics:
    """
    Maintain recent events and calculate incremental web-log metrics.

    The newest event timestamp acts as the window watermark.
    Events older than the configured window are removed.
    """

    def __init__(self, window_seconds: int = 60) -> None:
        if window_seconds <= 0:
            raise ValueError(
                "window_seconds must be greater than zero"
            )

        self.window_seconds = window_seconds

        # A heap keeps events ordered by timestamp, including events that
        # arrive slightly out of order.
        self._events: list[
            tuple[datetime, int, dict[str, Any]]
        ] = []

        # The sequence number prevents Python from comparing dictionaries
        # when two events have the same timestamp.
        self._sequence = count()

        self._latest_event_time: datetime | None = None

        self._requests_per_endpoint: Counter[str] = Counter()
        self._errors_per_endpoint: Counter[str] = Counter()
        self._traffic_by_hour: Counter[int] = Counter()
        self._status_code_distribution: Counter[int] = Counter()

    def add_event(self, event: dict[str, Any]) -> None:
        """Add one event and remove records outside the window."""

        event_time = self._parse_timestamp(
            event.get("timestamp")
        )

        endpoint = str(
            event.get("endpoint") or ""
        ).strip()

        if not endpoint:
            raise ValueError(
                "event endpoint is required"
            )

        try:
            status_code = int(event["status_code"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "event status_code must be an integer"
            ) from error

        stored_event = {
            "endpoint": endpoint,
            "status_code": status_code,
            "hour": event_time.hour,
        }

        heapq.heappush(
            self._events,
            (
                event_time,
                next(self._sequence),
                stored_event,
            ),
        )

        if (
            self._latest_event_time is None
            or event_time > self._latest_event_time
        ):
            self._latest_event_time = event_time

        self._requests_per_endpoint[endpoint] += 1
        self._traffic_by_hour[event_time.hour] += 1
        self._status_code_distribution[status_code] += 1

        if status_code >= 400:
            self._errors_per_endpoint[endpoint] += 1

        self._evict_expired_events()

    def snapshot(self) -> dict[str, Any]:
        """Return the current metrics without changing the window."""

        error_rates: dict[str, dict[str, int | float]] = {}

        for endpoint in sorted(
            self._requests_per_endpoint
        ):
            total_requests = self._requests_per_endpoint[
                endpoint
            ]
            error_count = self._errors_per_endpoint.get(
                endpoint,
                0,
            )

            error_rates[endpoint] = {
                "total_requests": total_requests,
                "error_count": error_count,
                "error_rate": (
                    error_count / total_requests
                    if total_requests
                    else 0.0
                ),
            }

        if self._latest_event_time is None:
            window_start = None
            window_end = None
        else:
            window_end = self._latest_event_time.isoformat()
            window_start = (
                self._latest_event_time
                - timedelta(
                    seconds=self.window_seconds
                )
            ).isoformat()

        return {
            "window_seconds": self.window_seconds,
            "window_start": window_start,
            "window_end": window_end,
            "window_event_count": len(self._events),
            "requests_per_endpoint": dict(
                self._requests_per_endpoint
            ),
            "error_rates": error_rates,
            "traffic_by_hour": dict(
                self._traffic_by_hour
            ),
            "status_code_distribution": dict(
                self._status_code_distribution
            ),
        }

    def _evict_expired_events(self) -> None:
        """Remove events older than the current window start."""

        if self._latest_event_time is None:
            return

        cutoff = (
            self._latest_event_time
            - timedelta(
                seconds=self.window_seconds
            )
        )

        # The lower boundary is inclusive. An event exactly at the
        # cutoff remains inside the window.
        while (
            self._events
            and self._events[0][0] < cutoff
        ):
            _, _, expired_event = heapq.heappop(
                self._events
            )

            endpoint = expired_event["endpoint"]
            status_code = expired_event["status_code"]
            hour = expired_event["hour"]

            self._decrement_counter(
                self._requests_per_endpoint,
                endpoint,
            )
            self._decrement_counter(
                self._traffic_by_hour,
                hour,
            )
            self._decrement_counter(
                self._status_code_distribution,
                status_code,
            )

            if status_code >= 400:
                self._decrement_counter(
                    self._errors_per_endpoint,
                    endpoint,
                )

    @staticmethod
    def _decrement_counter(
        counter: Counter,
        key: Any,
    ) -> None:
        """Decrease a counter and remove zero-value entries."""

        counter[key] -= 1

        if counter[key] <= 0:
            del counter[key]

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        """Parse an ISO 8601 timestamp and normalise it to UTC."""

        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                "event timestamp is required"
            )

        timestamp_text = value.strip()

        if timestamp_text.endswith("Z"):
            timestamp_text = (
                timestamp_text[:-1] + "+00:00"
            )

        try:
            parsed = datetime.fromisoformat(
                timestamp_text
            )
        except ValueError as error:
            raise ValueError(
                "event timestamp must use ISO 8601 format"
            ) from error

        if (
            parsed.tzinfo is None
            or parsed.utcoffset() is None
        ):
            raise ValueError(
                "event timestamp must include a timezone"
            )

        return parsed.astimezone(timezone.utc)
