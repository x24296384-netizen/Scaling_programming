# Scalable Web Log Analytics

A Lambda Architecture project for scalable analysis of large Nginx access logs using Amazon EMR, PySpark, Amazon Kinesis Data Streams, AWS Lambda, Amazon S3 and Amazon Athena.

## Architecture

```text
Historical path
Amazon S3 raw logs
        ↓
PySpark on Amazon EMR
        ↓
Historical aggregates and baseline RPM in Amazon S3
        ↓
Amazon Athena external tables and views

Recent path
Nginx log replay
        ↓
Amazon Kinesis Data Streams
        ↓
AWS Lambda speed processors
        ↓
Immutable invocation deltas in Amazon S3
        ↓
Global sliding-window aggregator
        ↓
Global speed snapshot

Serving path
Historical baseline RPM + global speed snapshot
        ↓
Endpoint-level RPM comparison and traffic-spike classification
```

The batch layer prioritises complete historical correctness. The speed layer provides recent low-latency analytics. The serving layer compares recent endpoint traffic with the historical baseline.

## Main Capabilities

- Shared Nginx event schema across batch and streaming paths.
- PySpark batch processing on Amazon EMR.
- Kinesis producer batching with partial-failure retries.
- Event-time sliding-window analytics.
- Error-rate anomaly detection.
- Immutable Lambda batch deltas in Amazon S3.
- Global reconstruction across concurrent Lambda environments.
- Endpoint-level recent RPM versus historical baseline comparison.
- Athena SQL for historical queries and reconciliation checks.
- Automated local, integration and AWS-oriented tests.

## Shared Event Schema

The common fields are:

```text
client_ip, timestamp, method, endpoint, protocol, status_code,
response_bytes, referrer, user_agent
```

The Kinesis path additionally uses:

```text
event_id, ingested_at, source
```

Timestamps are normalised to timezone-aware ISO 8601 UTC values. `status_code` and `response_bytes` are integers, and `endpoint` is the official shared request-path field.

## Repository Structure

```text
athena/       Athena DDL, views, validation and demo queries
batch/        PySpark historical processing
benchmark/    Batch, Kinesis and Lambda/S3 benchmark scripts
deployment/   Lambda packaging helpers
docs/         Architecture, evidence and project documentation
infra/        EMR setup and infrastructure scripts
producer/     Nginx parser and Kinesis replay
results/      Machine-readable benchmark and integration outputs
serving/      Batch/speed comparison and traffic-spike logic
speed/        Sliding window, Lambda handler, S3 deltas and global aggregation
tests/        Unit and integration tests
```

## Local Setup

Requirements:

- Python 3.11
- Java 17
- `pyspark==3.5.0`
- AWS CLI for AWS executions

Create and activate a virtual environment, then install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Run the complete test suite:

```powershell
python -m unittest discover -s tests -v
```

The latest completed suite contains 64 tests and completes with `OK` and exit code `0`. Local Spark on Windows may print `winutils.exe`, native Hadoop, socket-cleanup or temporary-directory warnings. These warnings do not invalidate a run whose unittest summary ends with `OK`.

## Batch Layer: PySpark on Amazon EMR

The batch layer reads the full Nginx log from Amazon S3 and produces ten
historical and data-quality outputs:

- `requests_per_endpoint/`
- `traffic_by_hour/`
- `error_rates/`
- `baseline_rpm/`
- `data_quality/`
- `status_code_distribution/`
- `response_byte_totals/`
- `summary/`
- `rejected_breakdown/`
- `rejected_sample/`

The final single-run EMR execution used:

- Commit: `120dda8`
- EMR cluster: `j-2KIK1VQPJT200`
- EMR step: `s-09947463792EMAWECP0P`
- Total raw lines: 10,365,152
- Valid records: 10,365,077
- Rejected records: 75
- Invalid format records: 0
- Invalid timestamp records: 0
- Invalid request records: 75
- Endpoint aggregate rows: 893,048
- Total error responses: 177,634
- Total response bytes: 128,870,996,472

All ten final CSV outputs came from this same execution. They were reconciled
locally, uploaded to `s3://scp-speed-results-25186396/batch/` and validated
through Amazon Athena.

Review the AWS values in `infra/emr_setup.sh`, then launch the EMR cluster:

```bash
chmod +x infra/emr_setup.sh
./infra/emr_setup.sh
```

Upload and submit the Spark job using the paths printed or configured by the infrastructure script. Track the EMR step until it reaches `COMPLETED`.

### Batch Benchmark Results

| Workers | Execution time | Speedup | Efficiency |
|---:|---:|---:|---:|
| 1 | 410.4 s | 1.00x | 100% |
| 2 | 380.1 s | 1.08x | 54% |
| 4 | 294.1 s | 1.40x | 35% |

The measured speedup is sub-linear because Spark startup, scheduling, communication and shuffle overheads remain significant for this workload.

## Speed Layer: Kinesis and Lambda

Build the Lambda deployment package:

```powershell
python deployment/build_speed_lambda_package.py
```

Replay logs to Kinesis using the producer module and configured AWS credentials. The Lambda handler processes Kinesis batches, updates recent analytics and writes:

```text
s3://scp-speed-results-25186396/speed/latest_snapshot.json
s3://scp-speed-results-25186396/speed/batches/...
```

`latest_snapshot.json` is a diagnostic snapshot from the most recent Lambda environment. It is not treated as globally complete when Lambda concurrency creates multiple execution environments.

## Global Speed Aggregation

Rebuild the complete event-time window from immutable invocation deltas:

```powershell
python -m speed.global_aggregator `
  --bucket scp-speed-results-25186396 `
  --prefix speed/batches `
  --output-key speed/global_snapshot.json
```

The aggregator:

- reads all relevant delta objects;
- removes duplicate events;
- applies the configured event-time window;
- recalculates comparable speed-layer metrics;
- writes `speed/global_snapshot.json`.

AWS validation completed successfully for 100, 500 and 1,000 events. In the 1,000-event run, a local Lambda snapshot observed only 600 events while the global aggregator recovered all 1,000 endpoint events.

## Endpoint RPM Comparison

The serving layer calculates:

```text
recent_rpm = recent_requests x 60 / window_seconds
```

For each endpoint it exposes:

- historical baseline RPM;
- recent request count;
- recent RPM;
- RPM difference;
- recent-to-baseline ratio;
- traffic status;
- significant-increase indicator.

The default spike rule requires both:

- a recent-to-baseline ratio of at least `2.0`;
- at least `10` recent requests.

The thresholds are configurable through the serving-layer command-line arguments.

## Amazon Athena

Athena scripts are stored in `athena/` and should be executed in this order:

```text
01_create_database.sql
02_create_external_tables.sql
03_create_views.sql
04_validation_queries.sql
05_demo_queries.sql
```

Deployment status in `us-east-1`:

- Database: `scp_web_logs_25186396`
- Workgroup: `primary`
- Query results: `s3://scp-speed-results-25186396/athena-results/`
- External tables: 7
- Views: 2

The final real EMR `part-*.csv` outputs are present in the expected S3
prefixes. Athena validation confirmed:

- 893,048 endpoint rows;
- 10,365,077 requests across each main aggregate;
- 128,870,996,472 response bytes reconciled with the batch summary;
- 177,634 error responses;
- 15 HTTP status-code categories;
- no empty baseline endpoints or null RPM values.

Final query screenshots are stored in
`docs/evidence/athena/screenshots/`.

## Current Limitations

- AWS Academy Learner Lab sessions can expire and terminate EMR or Cloud9
  resources.
- The EMR auto-scaling policy was configured and evidenced, but a live
  scale-out was not observed within the available Learner Lab constraints.
- Under concurrent Lambda invocations, the latest per-invocation snapshot may
  contain only part of a large benchmark. The global aggregator reconstructs
  the complete event-time window from persisted batch deltas.
- The global speed-layer aggregation is currently executed explicitly rather
  than through a scheduled production workflow.

## Project Status

The implementation, 64-test validation, final EMR delivery, S3 upload,
Athena reconciliation and evidence collection are complete.

See [`STATUS.md`](STATUS.md) for detailed milestones, AWS identifiers,
benchmark results and evidence locations.
