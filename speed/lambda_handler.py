"""AWS Lambda handler for the Kinesis speed layer.

Amazon Kinesis invokes this Lambda through an event-source mapping.
Each Lambda invocation may contain several Kinesis records.

Processing flow:

Kinesis record
    -> base64 decoding
    -> JSON decoding
    -> event validation
    -> sliding-window analytics
    -> anomaly detection
    -> partial-batch response
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import os
from typing import Any

from speed.stream_consumer import process_kinesis_record
from speed.window_analytics import SlidingWindowAnalytics


# Lambda sends application logs to Amazon CloudWatch Logs.
# Using the root logger avoids creating duplicate handlers.
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def _positive_int_environment(
    name: str,
    default: int,
) -> int:
    """Read a positive integer from an environment variable.

    Lambda configuration values are received as strings.

    When the environment variable is missing, invalid or less than
    one, the supplied default value is returned.
    """

    try:
        value = int(
            os.getenv(
                name,
                str(default),
            )
        )
    except ValueError:
        return default

    if value <= 0:
        return default

    return value


def _rate_environment(
    name: str,
    default: float,
) -> float:
    """Read a decimal rate between zero and one.

    Examples:

    0.50 means 50%.
    1.00 means 100%.

    Invalid values return the default rather than preventing the
    Lambda function from starting.
    """

    try:
        value = float(
            os.getenv(
                name,
                str(default),
            )
        )
    except ValueError:
        return default

    if 0.0 <= value <= 1.0:
        return value

    return default


# Duration of the recent analytics window.
#
# This can be configured in the Lambda environment without changing
# the source code.
WINDOW_SECONDS = _positive_int_environment(
    "WINDOW_SECONDS",
    300,
)


# An endpoint becomes a possible anomaly when its recent error rate
# reaches this value.
#
# Default: 0.50 means that at least 50% of its requests failed.
ERROR_RATE_THRESHOLD = _rate_environment(
    "ERROR_RATE_THRESHOLD",
    0.50,
)


# A single failed request should not immediately produce an anomaly.
# The endpoint must first reach this minimum request count.
MIN_REQUESTS_FOR_ANOMALY = _positive_int_environment(
    "MIN_REQUESTS_FOR_ANOMALY",
    2,
)


# Lambda may reuse the same execution environment for several
# invocations.
#
# Creating the analytics object outside lambda_handler allows a warm
# Lambda container to preserve its recent sliding-window state.
#
# Important limitation:
# this state is local to one Lambda execution environment. Different
# concurrent Lambda containers do not share this in-memory window.
_ANALYTICS = SlidingWindowAnalytics(
    window_seconds=WINDOW_SECONDS,
)


def reset_analytics(
    window_seconds: int | None = None,
) -> None:
    """Replace the current in-memory analytics window.

    This function is mainly used by unit tests. Without resetting the
    global object, one test could leave events that affect another test.

    It may also be useful during local manual validation.
    """

    global _ANALYTICS

    effective_window = (
        window_seconds
        if window_seconds is not None
        else WINDOW_SECONDS
    )

    _ANALYTICS = SlidingWindowAnalytics(
        window_seconds=effective_window,
    )


def _record_identifier(
    record: dict[str, Any],
    fallback_index: int,
) -> str:
    """Find an identifier for partial-batch failure reporting.

    Kinesis partial-batch responses normally use the sequence number
    of the failed record.

    When the sequence number is unavailable, eventID is used.
    The list position is the final fallback.
    """

    kinesis = record.get("kinesis")

    if isinstance(kinesis, dict):
        sequence_number = kinesis.get(
            "sequenceNumber"
        )

        if sequence_number:
            return str(sequence_number)

    event_id = record.get("eventID")

    if event_id:
        return str(event_id)

    return f"record-{fallback_index}"


def convert_lambda_kinesis_record(
    record: dict[str, Any],
) -> dict[str, Any] | None:
    """Convert the AWS Lambda record into the local consumer format.

    The existing local stream consumer expects a dictionary similar to:

        {
            "Data": b'{"endpoint": "/api"}',
            "PartitionKey": "127.0.0.1",
            "SequenceNumber": "123"
        }

    However, the Kinesis event-source mapping delivers data like:

        {
            "kinesis": {
                "data": "base64 encoded value",
                "partitionKey": "127.0.0.1",
                "sequenceNumber": "123"
            }
        }

    This adapter allows the Lambda to reuse the existing consumer
    instead of duplicating JSON decoding and analytics validation.
    """

    kinesis = record.get("kinesis")

    # A real Kinesis record must contain a nested kinesis object.
    if not isinstance(kinesis, dict):
        return None

    encoded_data = kinesis.get("data")

    # Lambda supplies Kinesis data as a base64 string.
    if not isinstance(encoded_data, str):
        return None

    try:
        # validate=True ensures malformed base64 is rejected instead
        # of being decoded partially.
        decoded_data = base64.b64decode(
            encoded_data,
            validate=True,
        )
    except (
        binascii.Error,
        ValueError,
    ):
        return None

    return {
        "Data": decoded_data,
        "PartitionKey": kinesis.get(
            "partitionKey"
        ),
        "SequenceNumber": kinesis.get(
            "sequenceNumber"
        ),
    }


def detect_anomalies(
    snapshot: dict[str, Any],
    error_rate_threshold: float,
    minimum_requests: int,
) -> list[dict[str, Any]]:
    """Find endpoints with a sustained recent error rate.

    An endpoint is reported only when:

    1. it has received at least `minimum_requests`; and
    2. its error rate is equal to or greater than the threshold.

    This avoids treating one isolated failed request as an anomaly.
    """

    anomalies: list[dict[str, Any]] = []

    error_rates = snapshot.get(
        "error_rates",
        {},
    )

    # Defensive validation prevents malformed snapshot data from
    # stopping the complete Lambda invocation.
    if not isinstance(error_rates, dict):
        return anomalies

    for endpoint in sorted(error_rates):
        endpoint_metrics = error_rates[endpoint]

        if not isinstance(endpoint_metrics, dict):
            continue

        total_requests = int(
            endpoint_metrics.get(
                "total_requests",
                0,
            )
        )

        error_count = int(
            endpoint_metrics.get(
                "error_count",
                0,
            )
        )

        error_rate = float(
            endpoint_metrics.get(
                "error_rate",
                0.0,
            )
        )

        has_enough_requests = (
            total_requests >= minimum_requests
        )

        reached_error_threshold = (
            error_rate >= error_rate_threshold
        )

        if (
            has_enough_requests
            and reached_error_threshold
        ):
            anomalies.append(
                {
                    "endpoint": endpoint,
                    "total_requests": total_requests,
                    "error_count": error_count,
                    "error_rate": error_rate,
                    "reason": (
                        "recent error rate reached "
                        "the configured threshold"
                    ),
                }
            )

    return anomalies


def lambda_handler(
    event: dict[str, Any],
    context: Any,
) -> dict[str, Any]:
    """Process one batch delivered by Kinesis to AWS Lambda.

    A single invocation can contain several Kinesis records.

    Valid records update the sliding-window analytics.
    Invalid records are counted and reported individually.

    Returning `batchItemFailures` allows Lambda to retry only failed
    records when partial-batch failure reporting is enabled in the
    Kinesis event-source mapping.
    """

    # The Lambda context is currently unnecessary, but it remains in
    # the function signature because AWS always provides it.
    del context

    records = event.get(
        "Records",
        [],
    )

    # A malformed event should produce an empty batch rather than
    # crashing the function.
    if not isinstance(records, list):
        records = []

    processed_records = 0
    invalid_records = 0

    # AWS expects failed Kinesis items in this structure:
    #
    # {
    #     "batchItemFailures": [
    #         {"itemIdentifier": "sequence-number"}
    #     ]
    # }
    batch_item_failures: list[
        dict[str, str]
    ] = []

    for index, lambda_record in enumerate(
        records
    ):
        # Each entry in Records should be a dictionary.
        if not isinstance(lambda_record, dict):
            invalid_records += 1

            batch_item_failures.append(
                {
                    "itemIdentifier": (
                        f"record-{index}"
                    ),
                }
            )

            # Continue with the remaining records.
            continue

        identifier = _record_identifier(
            lambda_record,
            index,
        )

        # Convert the AWS Lambda event structure into the format
        # already understood by speed.stream_consumer.
        consumer_record = (
            convert_lambda_kinesis_record(
                lambda_record
            )
        )

        if consumer_record is None:
            invalid_records += 1

            batch_item_failures.append(
                {
                    "itemIdentifier": identifier,
                }
            )

            continue

        # process_kinesis_record performs:
        #
        # 1. JSON decoding;
        # 2. shared-event validation;
        # 3. sliding-window update.
        processed = process_kinesis_record(
            record=consumer_record,
            analytics=_ANALYTICS,
        )

        if processed:
            processed_records += 1
        else:
            invalid_records += 1

            batch_item_failures.append(
                {
                    "itemIdentifier": identifier,
                }
            )

    # Snapshot returns the current metrics after all valid records in
    # this invocation have been processed.
    snapshot = _ANALYTICS.snapshot()

    anomalies = detect_anomalies(
        snapshot=snapshot,
        error_rate_threshold=(
            ERROR_RATE_THRESHOLD
        ),
        minimum_requests=(
            MIN_REQUESTS_FOR_ANOMALY
        ),
    )

    # The result contains both the AWS partial-batch response and
    # additional values useful for testing and CloudWatch evidence.
    result = {
        "batchItemFailures": (
            batch_item_failures
        ),
        "summary": {
            "received_records": len(records),
            "processed_records": (
                processed_records
            ),
            "invalid_records": (
                invalid_records
            ),
            "window_event_count": snapshot[
                "window_event_count"
            ],
            "anomaly_count": len(anomalies),
        },
        "anomalies": anomalies,
        "snapshot": snapshot,
    }

    # This summary becomes visible in CloudWatch Logs after deployment.
    LOGGER.info(
        "Kinesis Lambda batch result: %s",
        json.dumps(
            result["summary"],
            sort_keys=True,
        ),
    )

    # Anomalies use WARNING so they are easier to locate in CloudWatch.
    for anomaly in anomalies:
        LOGGER.warning(
            "Anomaly detected: %s",
            json.dumps(
                anomaly,
                sort_keys=True,
            ),
        )

    return result