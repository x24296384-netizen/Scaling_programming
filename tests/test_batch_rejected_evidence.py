"""
Tests for the rejected-record / data-quality additions to batch/batch_job.py
(read_raw_data_with_quality, write_rejected_evidence), covering exactly the
properties Mary Helen asked to be confirmed:

  1. total lines equal valid plus rejected records
  2. the data-quality totals are correct
  3. every rejected record receives exactly one rejection reason
  4. the sum of rejected_breakdown counts equals the total rejected count
  5. rejected_sample contains no more than 20 records
  6. the existing official schema and UTC timestamp normalisation still work
     (spot-checked here; the full schema/UTC suite lives in the existing
     batch metric tests, which are untouched by this change)

Run with:
  pytest tests/test_batch_rejected_evidence.py -v
"""

import os
import tempfile

import pytest
from pyspark.sql import SparkSession

from batch.batch_job import (
    read_raw_data_with_quality,
    write_rejected_evidence,
)


SAMPLE_LOG_LINES = [
    '192.168.1.10 - - [30/Jul/2025:15:57:19 +0000] "GET /index.html HTTP/1.1" 200 1024 "-" "Mozilla/5.0"',
    '192.168.1.11 - - [30/Jul/2025:15:58:02 +0000] "GET /about HTTP/1.1" 200 512 "-" "Mozilla/5.0"',
    'this is not a log line',
    '192.168.1.12 - - [not-a-date] "GET /x HTTP/1.1" 200 100 "-" "Mozilla/5.0"',
    '192.168.1.13 - - [30/Jul/2025:16:00:00 +0000] "GET /x extra tokens HTTP/1.1" 200 100 "-" "Mozilla/5.0"',
]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-rejected-evidence")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_log_path(tmp_path):
    log_file = tmp_path / "sample_access.log"
    log_file.write_text("\n".join(SAMPLE_LOG_LINES) + "\n")
    return str(log_file)


def test_total_equals_valid_plus_rejected(spark, sample_log_path):
    _, quality, _ = read_raw_data_with_quality(spark, sample_log_path)

    computed_invalid = (
        quality["invalid_format_records"]
        + quality["invalid_timestamp_records"]
        + quality["invalid_request_records"]
    )

    assert quality["total_raw_lines"] == quality["valid_records"] + computed_invalid
    assert quality["invalid_records"] == computed_invalid


def test_data_quality_totals_are_correct(spark, sample_log_path):
    _, quality, _ = read_raw_data_with_quality(spark, sample_log_path)

    assert quality["total_raw_lines"] == 5
    assert quality["valid_records"] == 2
    assert quality["invalid_format_records"] == 1
    assert quality["invalid_timestamp_records"] == 1
    assert quality["invalid_request_records"] == 1
    assert quality["invalid_records"] == 3


def test_every_rejected_record_has_one_reason(spark, sample_log_path):
    _, _, rejected_df = read_raw_data_with_quality(spark, sample_log_path)

    valid_reasons = {"invalid_format", "invalid_timestamp", "invalid_request"}
    rows = rejected_df.select("rejection_reason").collect()

    assert len(rows) == 3
    for row in rows:
        assert row["rejection_reason"] is not None
        assert row["rejection_reason"] in valid_reasons


def test_rejected_breakdown_sum_equals_total_rejected(spark, sample_log_path, tmp_path):
    _, quality, rejected_df = read_raw_data_with_quality(spark, sample_log_path)

    output_path = str(tmp_path / "output")
    write_rejected_evidence(rejected_df, output_path)

    breakdown_df = spark.read.option("header", "true").csv(
        f"{output_path}/rejected_breakdown"
    )
    breakdown_sum = sum(int(row["count"]) for row in breakdown_df.collect())

    assert breakdown_sum == quality["invalid_records"]


def test_rejected_sample_has_at_most_20_records(spark, sample_log_path, tmp_path):
    _, _, rejected_df = read_raw_data_with_quality(spark, sample_log_path)

    output_path = str(tmp_path / "output")
    write_rejected_evidence(rejected_df, output_path)

    sample_df = spark.read.option("header", "true").csv(
        f"{output_path}/rejected_sample"
    )

    assert sample_df.count() <= 20


def test_valid_records_use_official_schema_and_utc_timestamp(spark, sample_log_path):
    valid_df, _, _ = read_raw_data_with_quality(spark, sample_log_path)

    expected_columns = {
        "client_ip", "timestamp", "method", "endpoint", "protocol",
        "status_code", "response_bytes", "referrer", "user_agent",
    }
    assert expected_columns.issubset(set(valid_df.columns))

    timestamp_type = dict(valid_df.dtypes)["timestamp"]
    assert timestamp_type == "timestamp"

    rows = valid_df.select("client_ip", "endpoint").collect()
    assert {row["client_ip"] for row in rows} == {"192.168.1.10", "192.168.1.11"}
