"""Build a combined serving view from batch and speed-layer results.

The batch layer represents accurate historical processing.

The speed layer represents a recent sliding window stored in Amazon S3.

Raw request totals from these layers normally cover different time
periods. Therefore, this module displays counts side by side but only
calculates direct differences for normalised values such as error rates,
status-code shares and requests per minute.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3


DEFAULT_SPEED_BUCKET = (
    "scp-speed-results-25186396"
)

DEFAULT_SPEED_KEY = (
    "speed/global_snapshot.json"
)

DEFAULT_BATCH_FILE = (
    "results/integration/"
    "batch_stream_comparison.json"
)

DEFAULT_OUTPUT_FILE = (
    "results/serving/"
    "combined_serving_view.json"
)


def load_json_file(
    path: str | Path,
) -> dict[str, Any]:
    """Load a JSON document, including files containing a UTF-8 BOM."""

    effective_path = Path(path)

    document = json.loads(
        effective_path.read_text(
            encoding="utf-8-sig"
        )
    )

    if not isinstance(document, dict):
        raise ValueError(
            "Expected a JSON object."
        )

    return document


def load_s3_json(
    *,
    bucket: str,
    key: str,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Read one JSON object from Amazon S3."""

    client = (
        s3_client
        if s3_client is not None
        else boto3.client("s3")
    )

    response = client.get_object(
        Bucket=bucket,
        Key=key,
    )

    raw_body = response["Body"].read()

    document = json.loads(
        raw_body.decode("utf-8-sig")
    )

    if not isinstance(document, dict):
        raise ValueError(
            "Expected an S3 JSON object."
        )

    return document


def extract_batch_metrics(
    document: dict[str, Any],
) -> dict[str, Any]:
    """Extract metrics from supported batch-result structures."""

    for key in (
        "batch_metrics",
        "metrics",
        "snapshot",
    ):
        candidate = document.get(key)

        if isinstance(candidate, dict):
            return candidate

    return document


def extract_speed_metrics(
    document: dict[str, Any],
) -> dict[str, Any]:
    """Extract the recent analytics snapshot."""

    snapshot = document.get(
        "snapshot"
    )

    if isinstance(snapshot, dict):
        return snapshot

    return document


def _integer_mapping(
    value: Any,
) -> dict[str, int]:
    """Return a safe string-to-integer mapping."""

    if not isinstance(value, dict):
        return {}

    result: dict[str, int] = {}

    for key, item in value.items():
        try:
            result[str(key)] = int(item)
        except (TypeError, ValueError):
            continue

    return result


def _error_rate_mapping(
    value: Any,
) -> dict[str, float]:
    """Extract endpoint error rates."""

    if not isinstance(value, dict):
        return {}

    result: dict[str, float] = {}

    for endpoint, item in value.items():
        if isinstance(item, dict):
            rate = item.get(
                "error_rate"
            )
        else:
            rate = item

        try:
            result[str(endpoint)] = float(
                rate
            )
        except (TypeError, ValueError):
            continue

    return result


def _distribution_shares(
    distribution: dict[str, int],
) -> dict[str, float]:
    """Convert status-code counts into proportions."""

    total = sum(
        max(count, 0)
        for count in distribution.values()
    )

    if total == 0:
        return {
            code: 0.0
            for code in distribution
        }

    return {
        code: round(
            max(count, 0) / total,
            6,
        )
        for code, count in distribution.items()
    }


def _optional_float(
    value: Any,
) -> float | None:
    """Return a float when the value is numeric."""

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_combined_view(
    *,
    batch_document: dict[str, Any],
    speed_document: dict[str, Any],
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Create the historical-versus-recent serving document."""

    batch = extract_batch_metrics(
        batch_document
    )

    speed = extract_speed_metrics(
        speed_document
    )

    batch_requests = _integer_mapping(
        batch.get(
            "requests_per_endpoint"
        )
    )

    recent_requests = _integer_mapping(
        speed.get(
            "requests_per_endpoint"
        )
    )

    batch_bytes = _integer_mapping(
        batch.get(
            "response_byte_totals"
        )
    )

    recent_bytes = _integer_mapping(
        speed.get(
            "response_byte_totals"
        )
    )

    batch_errors = _error_rate_mapping(
        batch.get(
            "error_rates"
        )
    )

    recent_errors = _error_rate_mapping(
        speed.get(
            "error_rates"
        )
    )

    window_seconds = _optional_float(
        speed.get(
            "window_seconds"
        )
    )

    recent_total = int(
        speed.get(
            "total_valid_records",
            speed.get(
                "window_event_count",
                sum(
                    recent_requests.values()
                ),
            ),
        )
    )

    recent_rpm = None

    if (
        window_seconds is not None
        and window_seconds > 0
    ):
        recent_rpm = round(
            recent_total
            * 60.0
            / window_seconds,
            6,
        )

    baseline_rpm = _optional_float(
        batch.get(
            "baseline_rpm",
            batch_document.get(
                "baseline_rpm"
            ),
        )
    )

    traffic_comparison: dict[
        str,
        Any,
    ] = {
        "baseline_rpm": baseline_rpm,
        "recent_rpm": recent_rpm,
        "comparison_available": False,
        "rpm_difference": None,
        "recent_to_baseline_ratio": None,
        "traffic_status": (
            "baseline unavailable"
        ),
    }

    if (
        baseline_rpm is not None
        and recent_rpm is not None
    ):
        difference = (
            recent_rpm - baseline_rpm
        )

        ratio = (
            recent_rpm / baseline_rpm
            if baseline_rpm > 0
            else None
        )

        if difference > 0:
            status = "above baseline"
        elif difference < 0:
            status = "below baseline"
        else:
            status = "equal to baseline"

        traffic_comparison.update(
            {
                "comparison_available": True,
                "rpm_difference": round(
                    difference,
                    6,
                ),
                "recent_to_baseline_ratio": (
                    round(
                        ratio,
                        6,
                    )
                    if ratio is not None
                    else None
                ),
                "traffic_status": status,
            }
        )

    endpoints = sorted(
        set(batch_requests)
        | set(recent_requests)
        | set(batch_errors)
        | set(recent_errors)
        | set(batch_bytes)
        | set(recent_bytes)
    )

    endpoint_comparison: dict[
        str,
        dict[str, Any],
    ] = {}

    for endpoint in endpoints:
        historical_rate = (
            batch_errors.get(endpoint)
        )

        recent_rate = (
            recent_errors.get(endpoint)
        )

        rate_difference = None

        if (
            historical_rate is not None
            and recent_rate is not None
        ):
            rate_difference = round(
                recent_rate
                - historical_rate,
                6,
            )

        endpoint_recent_rpm = None

        if (
            window_seconds is not None
            and window_seconds > 0
        ):
            endpoint_recent_rpm = round(
                recent_requests.get(
                    endpoint,
                    0,
                )
                * 60.0
                / window_seconds,
                6,
            )

        endpoint_comparison[
            endpoint
        ] = {
            "historical_requests": (
                batch_requests.get(
                    endpoint,
                    0,
                )
            ),
            "recent_requests": (
                recent_requests.get(
                    endpoint,
                    0,
                )
            ),
            "recent_rpm": (
                endpoint_recent_rpm
            ),
            "historical_error_rate": (
                historical_rate
            ),
            "recent_error_rate": (
                recent_rate
            ),
            "error_rate_difference": (
                rate_difference
            ),
            "historical_response_bytes": (
                batch_bytes.get(
                    endpoint,
                    0,
                )
            ),
            "recent_response_bytes": (
                recent_bytes.get(
                    endpoint,
                    0,
                )
            ),
        }

    batch_status = _integer_mapping(
        batch.get(
            "status_code_distribution"
        )
    )

    recent_status = _integer_mapping(
        speed.get(
            "status_code_distribution"
        )
    )

    batch_status_shares = (
        _distribution_shares(
            batch_status
        )
    )

    recent_status_shares = (
        _distribution_shares(
            recent_status
        )
    )

    status_codes = sorted(
        set(batch_status)
        | set(recent_status)
    )

    status_comparison: dict[
        str,
        dict[str, Any],
    ] = {}

    for code in status_codes:
        historical_share = (
            batch_status_shares.get(
                code,
                0.0,
            )
        )

        recent_share = (
            recent_status_shares.get(
                code,
                0.0,
            )
        )

        status_comparison[code] = {
            "historical_count": (
                batch_status.get(
                    code,
                    0,
                )
            ),
            "recent_count": (
                recent_status.get(
                    code,
                    0,
                )
            ),
            "historical_share": (
                historical_share
            ),
            "recent_share": (
                recent_share
            ),
            "share_difference": round(
                recent_share
                - historical_share,
                6,
            ),
        }

    effective_generated_at = (
        generated_at
        if generated_at is not None
        else datetime.now(
            timezone.utc
        ).isoformat()
    )

    return {
        "schema_version": 1,
        "view_type": (
            "historical_and_recent"
        ),
        "generated_at": (
            effective_generated_at
        ),
        "speed_snapshot_generated_at": (
            speed_document.get(
                "generated_at"
            )
        ),
        "comparison_note": (
            "Historical batch totals and recent "
            "sliding-window totals cover different "
            "time periods. Counts are displayed side "
            "by side; direct differences use "
            "normalised rates or shares."
        ),
        "window": {
            "recent_window_seconds": (
                window_seconds
            ),
            "recent_window_start": (
                speed.get(
                    "window_start"
                )
            ),
            "recent_window_end": (
                speed.get(
                    "window_end"
                )
            ),
        },
        "totals": {
            "historical_valid_records": int(
                batch.get(
                    "total_valid_records",
                    sum(
                        batch_requests.values()
                    ),
                )
            ),
            "recent_valid_records": (
                recent_total
            ),
            "historical_response_bytes": int(
                batch.get(
                    "total_response_bytes",
                    sum(
                        batch_bytes.values()
                    ),
                )
            ),
            "recent_response_bytes": int(
                speed.get(
                    "total_response_bytes",
                    sum(
                        recent_bytes.values()
                    ),
                )
            ),
        },
        "traffic_comparison": (
            traffic_comparison
        ),
        "endpoint_comparison": (
            endpoint_comparison
        ),
        "status_code_comparison": (
            status_comparison
        ),
        "recent_anomalies": (
            speed_document.get(
                "anomalies",
                [],
            )
        ),
    }


def write_combined_view(
    *,
    document: dict[str, Any],
    output_path: str | Path,
) -> Path:
    """Write the serving document as formatted UTF-8 JSON."""

    path = Path(output_path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            document,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )

    return path


def main() -> None:
    """Build a serving view from a local batch file and S3 speed data."""

    parser = argparse.ArgumentParser(
        description=(
            "Combine historical batch metrics "
            "with recent speed-layer metrics."
        )
    )

    parser.add_argument(
        "--batch-file",
        default=DEFAULT_BATCH_FILE,
    )

    parser.add_argument(
        "--speed-file",
        default=None,
    )

    parser.add_argument(
        "--speed-bucket",
        default=DEFAULT_SPEED_BUCKET,
    )

    parser.add_argument(
        "--speed-key",
        default=DEFAULT_SPEED_KEY,
    )

    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_FILE,
    )

    arguments = parser.parse_args()

    batch_document = load_json_file(
        arguments.batch_file
    )

    if arguments.speed_file:
        speed_document = load_json_file(
            arguments.speed_file
        )
        speed_source = (
            arguments.speed_file
        )
    else:
        speed_document = load_s3_json(
            bucket=arguments.speed_bucket,
            key=arguments.speed_key,
        )
        speed_source = (
            f"s3://{arguments.speed_bucket}/"
            f"{arguments.speed_key}"
        )

    serving_view = build_combined_view(
        batch_document=batch_document,
        speed_document=speed_document,
    )

    output_path = write_combined_view(
        document=serving_view,
        output_path=arguments.output,
    )

    print(
        "=== COMBINED SERVING VIEW ==="
    )
    print(
        "Batch source:",
        arguments.batch_file,
    )
    print(
        "Speed source:",
        speed_source,
    )
    print(
        "Output:",
        output_path,
    )
    print(
        "Historical records:",
        serving_view["totals"][
            "historical_valid_records"
        ],
    )
    print(
        "Recent records:",
        serving_view["totals"][
            "recent_valid_records"
        ],
    )
    print(
        "Recent RPM:",
        serving_view[
            "traffic_comparison"
        ]["recent_rpm"],
    )
    print(
        "Recent anomalies:",
        len(
            serving_view[
                "recent_anomalies"
            ]
        ),
    )


if __name__ == "__main__":
    main()
