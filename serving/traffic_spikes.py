"""Compare recent endpoint traffic with historical requests-per-minute baselines."""

from __future__ import annotations

import math
from typing import Any, Iterable


DEFAULT_TRAFFIC_SPIKE_RATIO = 2.0
DEFAULT_MIN_REQUESTS_FOR_TRAFFIC_SPIKE = 10


def _nonnegative_float(value: Any) -> float | None:
    """Return a finite, non-negative float or ``None``."""

    try:
        result = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(result) or result < 0:
        return None

    return result




def _validate_thresholds(
    traffic_spike_ratio: float,
    minimum_requests: int,
) -> None:
    """Reject thresholds that cannot represent a meaningful increase."""

    if not math.isfinite(traffic_spike_ratio) or traffic_spike_ratio <= 1.0:
        raise ValueError("traffic_spike_ratio must be greater than 1.0.")

    if minimum_requests < 1:
        raise ValueError("minimum_requests must be at least 1.")


def extract_baseline_rpm(value: Any) -> dict[str, float]:
    """Normalise supported per-endpoint baseline structures.

    Supported forms are mappings such as ``{"/api": 4.2}``, mappings whose
    values contain ``avg_requests_per_minute``, and row-shaped lists such as
    the output of a small CSV or Athena loader.
    """

    if isinstance(value, dict):
        rows: Iterable[Any] = (
            {"endpoint": endpoint, "value": item}
            for endpoint, item in value.items()
        )
    elif isinstance(value, list):
        rows = value
    else:
        return {}

    baselines: dict[str, float] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue

        endpoint = row.get("endpoint")
        candidate = row.get("value")

        if isinstance(candidate, dict):
            candidate = candidate.get(
                "avg_requests_per_minute",
                candidate.get("baseline_rpm"),
            )
        elif candidate is None:
            candidate = row.get(
                "avg_requests_per_minute",
                row.get("baseline_rpm"),
            )

        baseline = _nonnegative_float(candidate)

        if endpoint is None or baseline is None:
            continue

        endpoint_name = str(endpoint).strip()
        if endpoint_name:
            baselines[endpoint_name] = baseline

    return baselines


def compare_endpoint_traffic(
    *,
    baseline_rpm: float | None,
    recent_requests: int,
    window_seconds: float | None,
    traffic_spike_ratio: float = DEFAULT_TRAFFIC_SPIKE_RATIO,
    minimum_requests: int = DEFAULT_MIN_REQUESTS_FOR_TRAFFIC_SPIKE,
) -> dict[str, Any]:
    """Compare one endpoint and classify whether its increase is significant."""

    _validate_thresholds(traffic_spike_ratio, minimum_requests)

    effective_requests = max(int(recent_requests), 0)
    minimum_requests_met = effective_requests >= minimum_requests
    effective_baseline = _nonnegative_float(baseline_rpm)
    effective_window = _nonnegative_float(window_seconds)

    recent_rpm = None
    if effective_window is not None and effective_window > 0:
        recent_rpm = round(
            effective_requests * 60.0 / effective_window,
            6,
        )

    comparison: dict[str, Any] = {
        "historical_baseline_rpm": effective_baseline,
        "recent_rpm": recent_rpm,
        "comparison_available": False,
        "rpm_difference": None,
        "recent_to_baseline_ratio": None,
        "traffic_status": "baseline unavailable",
        "significant_increase": False,
        "spike_ratio_threshold": traffic_spike_ratio,
        "minimum_requests_threshold": minimum_requests,
        "minimum_requests_met": minimum_requests_met,
    }

    if recent_rpm is None:
        comparison["traffic_status"] = "recent rate unavailable"
        return comparison

    if effective_baseline is None:
        return comparison

    difference = recent_rpm - effective_baseline
    ratio = (
        recent_rpm / effective_baseline
        if effective_baseline > 0
        else None
    )

    significant_increase = (
        minimum_requests_met
        and difference > 0
        and (ratio is None or ratio >= traffic_spike_ratio)
    )

    if significant_increase:
        status = "significant increase"
    elif difference > 0:
        status = "above baseline"
    elif difference < 0:
        status = "below baseline"
    else:
        status = "equal to baseline"

    comparison.update(
        {
            "comparison_available": True,
            "rpm_difference": round(difference, 6),
            "recent_to_baseline_ratio": (
                round(ratio, 6) if ratio is not None else None
            ),
            "traffic_status": status,
            "significant_increase": significant_increase,
        }
    )

    return comparison


def build_endpoint_traffic_view(
    *,
    baseline_rpm_by_endpoint: dict[str, float],
    recent_requests_by_endpoint: dict[str, int],
    window_seconds: float | None,
    endpoint_names: Iterable[str] | None = None,
    traffic_spike_ratio: float = DEFAULT_TRAFFIC_SPIKE_RATIO,
    minimum_requests: int = DEFAULT_MIN_REQUESTS_FOR_TRAFFIC_SPIKE,
) -> dict[str, Any]:
    """Build endpoint comparisons, a summary, and a ranked spike list."""

    _validate_thresholds(traffic_spike_ratio, minimum_requests)

    names = set(baseline_rpm_by_endpoint) | set(recent_requests_by_endpoint)
    if endpoint_names is not None:
        names.update(str(name) for name in endpoint_names)

    comparisons: dict[str, dict[str, Any]] = {}

    for endpoint in sorted(names):
        comparisons[endpoint] = compare_endpoint_traffic(
            baseline_rpm=baseline_rpm_by_endpoint.get(endpoint),
            recent_requests=recent_requests_by_endpoint.get(endpoint, 0),
            window_seconds=window_seconds,
            traffic_spike_ratio=traffic_spike_ratio,
            minimum_requests=minimum_requests,
        )

    spikes = [
        {
            "endpoint": endpoint,
            "recent_requests": recent_requests_by_endpoint.get(endpoint, 0),
            "historical_baseline_rpm": values["historical_baseline_rpm"],
            "recent_rpm": values["recent_rpm"],
            "rpm_difference": values["rpm_difference"],
            "recent_to_baseline_ratio": values[
                "recent_to_baseline_ratio"
            ],
            "traffic_status": values["traffic_status"],
        }
        for endpoint, values in comparisons.items()
        if values["significant_increase"]
    ]

    spikes.sort(
        key=lambda item: (
            item["recent_to_baseline_ratio"]
            if item["recent_to_baseline_ratio"] is not None
            else float("inf"),
            item["recent_rpm"] or 0.0,
        ),
        reverse=True,
    )

    summary = {
        "traffic_spike_ratio": traffic_spike_ratio,
        "minimum_requests_for_traffic_spike": minimum_requests,
        "endpoint_baselines_available": len(baseline_rpm_by_endpoint),
        "endpoints_compared": sum(
            1
            for values in comparisons.values()
            if values["comparison_available"]
        ),
        "significant_increase_count": len(spikes),
        "significant_increase_endpoints": [
            item["endpoint"] for item in spikes
        ],
    }

    return {
        "summary": summary,
        "comparisons": comparisons,
        "spikes": spikes,
    }
