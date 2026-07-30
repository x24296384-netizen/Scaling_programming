# Project Status

## Project

Scalable Web Log Analytics

Deadline: 4 August 2026 at 5:00 pm

The project analyses a large Nginx access-log dataset through two processing paths:

- PySpark batch processing on Amazon EMR
- Real-time processing using Amazon Kinesis Data Streams

The two paths form a Lambda Architecture. The batch layer provides accurate historical analytics, while the speed layer provides recent, low-latency analytics.

## Team Responsibilities

- Maryhelen: Nginx parser, Kinesis replay, speed-layer processing, integration and validation
- Nalini: PySpark batch processing, Amazon EMR, auto-scaling and batch benchmarking
- Shared: schema alignment, serving layer, evidence, report and presentation

## Shared Event Schema

The batch and real-time paths use the same base schema:

- client_ip
- timestamp
- method
- endpoint
- protocol
- status_code
- response_bytes
- referrer
- user_agent

The Kinesis path also adds:

- event_id
- ingested_at
- source

Schema rules:

- `timestamp` uses ISO 8601 and is normalised to UTC.
- `status_code` is stored as an integer.
- `response_bytes` is stored as an integer.
- A response-byte value of `-` is converted to zero.
- `endpoint` is the official field name.
- HTTP requests must contain method, endpoint and protocol.
- `event_id` is generated as a UUID.
- `ingested_at` is a timezone-aware UTC timestamp.
- `source` is set to `nginx_access_log`.

## Day 1 - Repository and Reliability Foundation

Status: Complete

Completed work:

- Integrated the initial PySpark batch implementation.
- Created the `maryhelen-integration` branch.
- Reorganised the repository into batch, speed, producer, benchmark, serving and infrastructure modules.
- Added Kinesis record construction.
- Used `client_ip` as the partition key.
- Added `PutRecords` batching.
- Added partial-failure handling.
- Retried only failed Kinesis records.
- Added data-quality reporting.
- Added automated tests for parsing, batch processing and replay reliability.

## Day 2 - Shared Schema and Kinesis Replay

Status: Complete

Completed work:

- Froze `endpoint` as the official shared field.
- Replaced previous `resource` references.
- Aligned `client_ip` and `response_bytes`.
- Normalised batch timestamps to UTC.
- Added `event_id`, `ingested_at` and `source`.
- Connected the Nginx parser to the Kinesis replay path.
- Added progressive file processing.
- Counted and skipped malformed log lines.
- Enforced Kinesis batch sizes between 1 and 500 records.
- Supported complete and final partial batches.
- Added configurable retries and retry delays.
- Added command-line replay support.
- Added tests for parser, replay, batching and permanent failures.

Important commits:

- `f0ca5c0` - Freeze endpoint schema and complete Kinesis replay
- `ce752c4` - Update batch progress and shared schema documentation

## Day 3 - Speed Layer and Benchmarks

Status: Complete

### Implementation

- Added event-time sliding-window analytics.
- Added incremental event eviction.
- Added support for out-of-order events inside the active window.
- Kept the lower window boundary inclusive.
- Added requests per endpoint.
- Added traffic by hour.
- Added error rates per endpoint.
- Added HTTP status-code distribution.
- Added a local Kinesis-style consumer.
- Added invalid JSON handling.
- Added reusable Kinesis shard discovery and record reading.
- Added throughput and latency measurements.
- Added local and AWS benchmark scripts.

### Full Local Benchmark

Dataset:

- Kaggle Web Server Access Logs
- Dataset size: 3,502,440,823 bytes
- Total lines: 10,365,152
- Valid records: 10,365,077
- Invalid lines: 75
- Failed records: 0

Measured results:

- Runtime: 352.8049 seconds
- Throughput: 29,379.27 lines per second
- Valid-record throughput: 29,379.06 records per second
- Sampled local latency mean: 0.0351 ms
- Sampled local latency p95: 0.06968 ms

The local latency covers parsing, Kinesis-style JSON creation, consumer decoding and sliding-window analytics. It does not include AWS network latency.

### Real Amazon Kinesis Smoke Test

Stream configuration:

- Mode: provisioned
- Open shards: 1
- Retention: 24 hours

Observed result:

- Records sent: 3
- Records received: 3
- Records processed: 3
- Invalid records: 0
- Failed records: 0
- Read attempts: 1
- Complete runtime: 0.30739 seconds
- Observed end-to-end latency: 300.444 ms

This was a controlled functional smoke test, not a maximum-throughput test.

Important commit:

- `bfcfef2` - Complete Day 3 Kinesis speed layer and benchmarks

## Day 4 - Batch and Streaming Integration

Status: Complete

### Controlled Input

Both processing paths were validated with:

`tests/fixtures/integration_window.log`

Controlled input result:

- Raw lines: 9
- Valid records: 7
- Rejected records: 2
- Total response bytes: 600

### Difference Identified

The producer parser rejected a malformed HTTP request, but the initial batch parser accepted it because the batch validity rule checked only the complete log format and timestamp.

The batch parser was updated to validate the HTTP request structure as well.

The two paths now reject the same malformed records.

### Comparable Metrics

The batch and streaming layers now calculate:

- Total valid records
- Total response bytes
- Requests per endpoint
- Error rates per endpoint
- Traffic by hour
- Status-code distribution
- Response-byte totals per endpoint

### Automated Comparison

The following command performs the comparison:

`python -m serving.compare_controlled_window`

Final result:

- PASS - total valid records
- PASS - total response bytes
- PASS - requests per endpoint
- PASS - error rates
- PASS - traffic by hour
- PASS - status-code distribution
- PASS - response-byte totals per endpoint

Final comparison result:

`ALL COMPARABLE METRICS MATCH`

### Automated Tests

- Total tests: 26
- Result: all tests passed
- Execution time: 15.929 seconds

### Evidence

Human-readable evidence:

- `docs/evidence/day4/01_producer_fixture_validation.txt`
- `docs/evidence/day4/02_batch_fixture_validation.txt`
- `docs/evidence/day4/03_batch_alignment_confirmation.txt`
- `docs/evidence/day4/04_batch_metrics_validation.txt`
- `docs/evidence/day4/05_stream_metrics_validation.txt`
- `docs/evidence/day4/06_batch_stream_comparison_clean.txt`
- `docs/evidence/day4/07_full_test_suite.txt`

Machine-readable evidence:

- `results/integration/batch_stream_comparison.json`

Important commit:

- `e8bcb14` - Validate batch and streaming analytics parity

## Current Git Status

Active integration branch:

- `maryhelen-integration`

Latest integration commit:

- `e8bcb14` - Validate batch and streaming analytics parity

Pull Request:

- PR #3
- Source: `maryhelen-integration`
- Target: `main`
- Status: Draft
- Merge conflicts: none
- Review requested from Nalini
- Merge must wait for confirmation of the latest EMR setup, benchmark configuration and AWS evidence

The older `nalini-batch-layer` branch must not be merged directly because its work has already been incorporated and reorganised in the integration branch.

## Local Spark Warnings

Local Spark execution on Windows may show warnings relating to:

- `winutils.exe`
- `HADOOP_HOME`
- Native Hadoop libraries
- Local socket cleanup
- Spark temporary-directory deletion

These warnings do not invalidate successful test results when the unittest summary ends with `OK`.

## Remaining Technical Tasks

- [ ] Receive Nalini's review of PR #3
- [ ] Confirm the latest EMR commands and benchmark configuration
- [ ] Collect remaining EMR, S3 and auto-scaling screenshots
- [ ] Add a Kinesis-triggered AWS Lambda handler
- [ ] Document the Kinesis event-source mapping
- [ ] Add the serving-layer comparison between historical baseline and recent traffic
- [ ] Add or document Athena queries over batch results in Amazon S3
- [ ] Update the architecture diagram
- [ ] Update the README with final execution instructions
- [ ] Prepare the technical report
- [ ] Prepare presentation slides and demonstration notes
- [ ] Run the final automated test suite
- [ ] Mark PR #3 as ready for review
- [ ] Merge PR #3 into `main`
- [ ] Confirm that `main` is clean and deployable

## Current Priority

While waiting for the batch-layer review, the next independent technical task is:

`Kinesis Data Streams -> Lambda event-source mapping -> speed/lambda_handler.py`

This will replace manual polling as the main AWS event-processing pattern while preserving the existing parser, consumer and sliding-window analytics.
