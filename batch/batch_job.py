"""
Batch layer for the Lambda Architecture (Scalable Cloud Programming CA).

Dataset confirmed: Kaggle "Web Server Access Logs" (eliasdabbas). This is a
RAW nginx-style access log, not a clean CSV — each line looks like:

  192.168.1.10 - - [30/Jul/2025:15:57:19 +0000] "GET /index.html HTTP/1.1" 200 1024 "http://example.com/start" "Mozilla/5.0..."

That's why Mary Helen built a log parser for the producer side. This batch
job parses the same raw format with a regex (standard nginx "combined" log
format) so both sides agree on field names. IMPORTANT: once you've looked at
her actual parser in producer/, double check the field names/regex below
match hers exactly — the serving layer needs both sides speaking the same
schema.

Run on EMR with:
  spark-submit --deploy-mode cluster batch/batch_job.py --input s3://.../raw-data/ --output s3://.../batch-results/
"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# standard nginx "combined" log format regex — covers:
# remote_addr - remote_user [time_local] "method path protocol" status bytes_sent "referer" "user_agent"
LOG_PATTERN = (
    r'^(\S+) \S+ \S+ '
    r'\[([^\]]+)\] '
    r'"([^"]*)" '
    r'(\d{3}) '
    r'(\S+) '
    r'"([^"]*)" '
    r'"([^"]*)" '
    r'"([^"]*)"$'
)


def build_spark_session(app_name="scp-batch-layer"):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def _parse_raw_data(spark, input_path):
    """
    Parse every raw access-log line and retain validation columns.

    This internal DataFrame contains valid and invalid records so that
    data-quality statistics can be calculated before invalid rows are removed.
    """

    # Use UTC so local development and Amazon EMR produce identical times.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    raw = spark.read.text(input_path)

    parsed = raw.select(
        F.col("value").alias("raw_line"),
        F.regexp_extract("value", LOG_PATTERN, 1).alias("client_ip"),
        F.regexp_extract("value", LOG_PATTERN, 2).alias("timestamp_raw"),
        F.regexp_extract("value", LOG_PATTERN, 3).alias("request_raw"),
        F.regexp_extract("value", LOG_PATTERN, 4).alias("status_code_raw"),
        F.regexp_extract(
            "value",
            LOG_PATTERN,
            5,
        ).alias("response_bytes_raw"),
        F.regexp_extract("value", LOG_PATTERN, 6).alias("referrer"),
        F.regexp_extract("value", LOG_PATTERN, 7).alias("user_agent"),
        F.regexp_extract("value", LOG_PATTERN, 8).alias("extra"),
    )

    request_parts = F.split(F.col("request_raw"), " ", 3)
    valid_request = F.size(request_parts) == 3

    return (
        parsed
        .withColumn(
            "timestamp",
            F.to_timestamp(
                "timestamp_raw",
                "dd/MMM/yyyy:HH:mm:ss Z",
            ),
        )
        .withColumn(
            "method",
            F.when(
                valid_request,
                request_parts.getItem(0),
            ).otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "endpoint",
            F.when(
                valid_request,
                request_parts.getItem(1),
            ).otherwise(F.col("request_raw")),
        )
        .withColumn(
            "protocol",
            F.when(
                valid_request,
                request_parts.getItem(2),
            ).otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "status_code",
            F.col("status_code_raw").cast(IntegerType()),
        )
        .withColumn(
            "response_bytes",
            F.when(
                F.col("response_bytes_raw").rlike(r"^\d+$"),
                F.col("response_bytes_raw").cast(IntegerType()),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "request_valid",
            valid_request,
        )
        # An empty client IP means the complete regular expression failed.
        .withColumn(
            "format_valid",
            F.col("client_ip") != "",
        )
        .withColumn(
            "timestamp_valid",
            F.col("timestamp").isNotNull(),
        )
    )


def _select_valid_records(parsed):
    """Return valid records using the official project schema."""

    return (
        parsed
        .filter(
            F.col("format_valid")
            & F.col("timestamp_valid")
            & F.col("request_valid")
        )
        .select(
            "client_ip",
            "timestamp",
            "method",
            "endpoint",
            "protocol",
            "status_code",
            "response_bytes",
            "referrer",
            "user_agent",
        )
    )


def read_raw_data(spark, input_path):
    """
    Read the raw log and return only valid records.

    This preserves the existing public behaviour used by the batch metrics.
    """

    parsed = _parse_raw_data(spark, input_path)
    return _select_valid_records(parsed)


def read_raw_data_with_quality_and_rejected(spark, input_path):
    """
    Return valid records and a summary of rejected log records.
    """

    # The parsed data is reused for quality analysis and valid-record selection.
    parsed = _parse_raw_data(spark, input_path).cache()

    quality_row = parsed.agg(
        F.count("*").alias("total_raw_lines"),

        F.sum(
            F.when(
                ~F.col("format_valid"),
                1,
            ).otherwise(0)
        ).alias("invalid_format_records"),

        F.sum(
            F.when(
                F.col("format_valid")
                & ~F.col("timestamp_valid"),
                1,
            ).otherwise(0)
        ).alias("invalid_timestamp_records"),

        F.sum(
            F.when(
                F.col("format_valid")
                & F.col("timestamp_valid")
                & ~F.col("request_valid"),
                1,
            ).otherwise(0)
        ).alias("invalid_request_records"),

        F.sum(
            F.when(
                F.col("format_valid")
                & F.col("timestamp_valid")
                & F.col("request_valid"),
                1,
            ).otherwise(0)
        ).alias("valid_records"),
    ).first()

    total_raw_lines = int(quality_row["total_raw_lines"])
    valid_records = int(quality_row["valid_records"] or 0)
    invalid_format_records = int(
        quality_row["invalid_format_records"] or 0
    )
    invalid_timestamp_records = int(
        quality_row["invalid_timestamp_records"] or 0
    )

    invalid_request_records = int(
        quality_row["invalid_request_records"] or 0
    )

    invalid_records = (
        invalid_format_records
        + invalid_timestamp_records
        + invalid_request_records
    )

    invalid_percentage = (
        round(
            invalid_records / total_raw_lines * 100,
            2,
        )
        if total_raw_lines
        else 0.0
    )

    quality = {
        "total_raw_lines": total_raw_lines,
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "invalid_format_records": invalid_format_records,
        "invalid_timestamp_records": invalid_timestamp_records,
        "invalid_request_records": invalid_request_records,
        "invalid_percentage": invalid_percentage,
    }

    # Build a rejected-records view with a precise rejection reason,
    # using the same format/timestamp/request checks as the quality
    # summary above, plus the original raw line for inspection.
    rejected_df = (
        parsed
        .filter(
            ~(F.col("format_valid") & F.col("timestamp_valid") & F.col("request_valid"))
        )
        .withColumn(
            "rejection_reason",
            F.when(~F.col("format_valid"), "invalid_format")
             .when(~F.col("timestamp_valid"), "invalid_timestamp")
             .otherwise("invalid_request"),
        )
        .select("raw_line", "rejection_reason")
        .cache()
    )

    # Materialise and cache valid records before releasing the larger
    # intermediate DataFrame.
    valid_df = _select_valid_records(parsed).cache()
    valid_df.count()
    rejected_df.count()
    parsed.unpersist()

    return valid_df, quality, rejected_df


def read_raw_data_with_quality(spark, input_path):
    """Return valid records and quality data using the stable two-value API."""
    valid_df, quality, rejected_df = read_raw_data_with_quality_and_rejected(
        spark,
        input_path,
    )
    rejected_df.unpersist()
    return valid_df, quality


def compute_batch_metrics(df):
    """
    Calculate historical analytics over the complete dataset.

    These batch results provide the accurate historical view that
    will later be combined with the recent speed-layer results.
    """

    # Total number of requests received by each endpoint.
    requests_per_endpoint = (
        df.groupBy("endpoint")
        .agg(
            F.count("*").alias("total_requests")
        )
        .orderBy(
            F.desc("total_requests")
        )
    )

    # Request volume for each hour of the day.
    traffic_by_hour = (
        df.withColumn(
            "hour",
            F.hour("timestamp"),
        )
        .groupBy("hour")
        .agg(
            F.count("*").alias("request_count")
        )
        .orderBy("hour")
    )

    # Error rate for each endpoint using HTTP status codes 400 and above.
    error_rates = (
        df.withColumn(
            "is_error",
            (F.col("status_code") >= 400).cast("int"),
        )
        .groupBy("endpoint")
        .agg(
            F.count("*").alias("total_requests"),
            F.sum("is_error").alias("error_count"),
        )
        .withColumn(
            "error_rate",
            F.col("error_count")
            / F.col("total_requests"),
        )
        .orderBy(
            F.desc("error_rate")
        )
    )

    # Number of requests for each HTTP status code.
    status_code_distribution = (
        df.groupBy("status_code")
        .agg(
            F.count("*").alias("request_count")
        )
        .orderBy("status_code")
    )

    # Total response bytes returned by each endpoint.
    response_byte_totals = (
        df.groupBy("endpoint")
        .agg(
            F.sum("response_bytes").alias(
                "total_response_bytes"
            )
        )
        .orderBy("endpoint")
    )

    # Overall values used in batch-versus-stream validation.
    summary = (
        df.agg(
            F.count("*").alias(
                "total_valid_records"
            ),
            F.sum("response_bytes").alias(
                "total_response_bytes"
            ),
        )
    )

    # Average requests per minute for each endpoint.
    baseline_rpm = (
        df.withColumn(
            "minute_bucket",
            F.date_trunc(
                "minute",
                "timestamp",
            ),
        )
        .groupBy(
            "endpoint",
            "minute_bucket",
        )
        .agg(
            F.count("*").alias(
                "requests_in_minute"
            )
        )
        .groupBy("endpoint")
        .agg(
            F.avg(
                "requests_in_minute"
            ).alias(
                "avg_requests_per_minute"
            )
        )
    )

    return {
        "requests_per_endpoint": requests_per_endpoint,
        "traffic_by_hour": traffic_by_hour,
        "error_rates": error_rates,
        "status_code_distribution": status_code_distribution,
        "response_byte_totals": response_byte_totals,
        "summary": summary,
        "baseline_rpm": baseline_rpm,
    }


def write_results(results, output_path):
    for name, df in results.items():
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            f"{output_path}/{name}"
        )


def build_rejected_evidence(rejected_df, sample_size=20):
    """Build the rejection breakdown and a bounded inspection sample."""
    if sample_size < 1:
        raise ValueError("sample_size must be at least 1")

    breakdown = (
        rejected_df.groupBy("rejection_reason")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
    )
    sample = rejected_df.limit(sample_size)

    return breakdown, sample


def write_rejected_evidence(rejected_df, output_path):
    """
    Write a rejection-reason breakdown and up to 20 rejected raw lines.
    """
    breakdown, sample = build_rejected_evidence(rejected_df)

    breakdown.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        f"{output_path}/rejected_breakdown"
    )
    sample.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        f"{output_path}/rejected_sample"
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="S3 path to raw data"
        )

    parser.add_argument(
        "--output",
        required=True,
        help="S3 path for batch results"
        )

    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help=(
            "Just for logging in benchmark runs — not enforced here, worker "
            "count comes from the EMR cluster config itself"
            ),
    )

    args = parser.parse_args()

    spark = build_spark_session()
    start_time = time.time()

    # Read valid records and collect information about rejected lines.
    df, quality, rejected_df = read_raw_data_with_quality_and_rejected(
        spark,
        args.input,
    )

    # The quality summary already contains the number of valid records.
    record_count = quality["valid_records"]

    print("\n=== DATA QUALITY ===")
    print(f"Total raw lines: {quality['total_raw_lines']}")
    print(f"Valid records: {quality['valid_records']}")
    print(f"Invalid records: {quality['invalid_records']}")
    print(
        "Invalid format records: "
        f"{quality['invalid_format_records']}"
    )
    print(
        "Invalid timestamp records: "
        f"{quality['invalid_timestamp_records']}"
    )
    print(
        "Invalid request records: "
        f"{quality['invalid_request_records']}"
    )
    print(
        "Invalid percentage: "
        f"{quality['invalid_percentage']:.2f}%"
    )

    # Calculate and save the historical batch-layer results.
    results = compute_batch_metrics(df)
    write_results(results, args.output)
    write_rejected_evidence(rejected_df, args.output)

    elapsed = time.time() - start_time

    # This line will be collected by the benchmark script.
    print(
        "BENCHMARK "
        f"workers={args.workers} "
        f"records={record_count} "
        f"elapsed_sec={elapsed:.2f}"
    )

    # Release the cached DataFrame and stop Spark cleanly.
    df.unpersist()
    rejected_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()