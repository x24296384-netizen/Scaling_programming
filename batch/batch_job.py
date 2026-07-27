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
            "resource",
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
        )
        .select(
            "client_ip",
            "timestamp",
            "method",
            "resource",
            "protocol",
            "status_code",
            "response_bytes",
            "referrer",
            "user_agent",
            "extra",
        )
    )


def read_raw_data(spark, input_path):
    """
    Read the raw log and return only valid records.

    This preserves the existing public behaviour used by the batch metrics.
    """

    parsed = _parse_raw_data(spark, input_path)
    return _select_valid_records(parsed)


def read_raw_data_with_quality(spark, input_path):
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
                & F.col("timestamp_valid"),
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

    invalid_records = (
        invalid_format_records
        + invalid_timestamp_records
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
        "invalid_percentage": invalid_percentage,
    }

    # Materialise and cache valid records before releasing the larger
    # intermediate DataFrame.
    valid_df = _select_valid_records(parsed).cache()
    valid_df.count()
    parsed.unpersist()

    return valid_df, quality


def compute_batch_metrics(df):
    """
    Historical / "complete and accurate" view required by Lambda Architecture.
    This is my (Nalini's) part of the split — batch layer + benchmarking.
    """

    # total requests per resource — the baseline the speed layer compares against
    requests_per_resource = (
        df.groupBy("resource")
        .agg(F.count("*").alias("total_requests"))
        .orderBy(F.desc("total_requests"))
    )

    # average traffic by hour of day — useful for the "normal vs abnormal" story
    traffic_by_hour = (
        df.withColumn("hour", F.hour("timestamp"))
        .groupBy("hour")
        .agg(F.count("*").alias("request_count"))
        .orderBy("hour")
    )

    # historical error rate per endpoint (status codes >= 400)
    error_rates = (
        df.withColumn("is_error", (F.col("status_code") >= 400).cast("int"))
        .groupBy("resource")
        .agg(
            F.count("*").alias("total_requests"),
            F.sum("is_error").alias("error_count"),
        )
        .withColumn("error_rate", F.col("error_count") / F.col("total_requests"))
        .orderBy(F.desc("error_rate"))
    )

    # baseline requests-per-minute per endpoint — this is the number the
    # serving layer will compare live speed-layer numbers against to flag anomalies
    baseline_rpm = (
        df.withColumn("minute_bucket", F.date_trunc("minute", "timestamp"))
        .groupBy("resource", "minute_bucket")
        .agg(F.count("*").alias("requests_in_minute"))
        .groupBy("resource")
        .agg(F.avg("requests_in_minute").alias("avg_requests_per_minute"))
    )

    return {
        "requests_per_resource": requests_per_resource,
        "traffic_by_hour": traffic_by_hour,
        "error_rates": error_rates,
        "baseline_rpm": baseline_rpm,
    }


def write_results(results, output_path):
    for name, df in results.items():
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            f"{output_path}/{name}"
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
    df, quality = read_raw_data_with_quality(
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
        "Invalid percentage: "
        f"{quality['invalid_percentage']:.2f}%"
    )

    # Calculate and save the historical batch-layer results.
    results = compute_batch_metrics(df)
    write_results(results, args.output)

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
    spark.stop()


if __name__ == "__main__":
    main()