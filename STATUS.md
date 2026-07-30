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

### Batch and Streaming Alignment

The producer parser rejected a malformed HTTP request, but the initial
batch parser accepted it because the batch validity rule checked only
the complete log format and timestamp.

The batch parser was updated to validate the HTTP request structure.

The two paths now reject the same malformed records and calculate the
same comparable metrics:

- Total valid records
- Total response bytes
- Requests per endpoint
- Error rates per endpoint
- Traffic by hour
- Status-code distribution
- Response-byte totals per endpoint

### Automated Comparison

The comparison command is:

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

### Kinesis-Triggered AWS Lambda

AWS function:

`scp-speed-processor-25186396`

Implemented and validated:

- Connected Amazon Kinesis to AWS Lambda through an event-source mapping.
- Enabled the trigger with starting position `LATEST`.
- Configured a batch size of 100 records.
- Enabled `ReportBatchItemFailures`.
- Enabled batch splitting on function errors.
- Configured three retry attempts.
- Decoded real Lambda Kinesis records.
- Updated recent sliding-window analytics.
- Detected high error-rate anomalies.
- Logged processing summaries and anomalies in CloudWatch.

Controlled AWS result:

- Records sent: 2
- Failed records: 0
- Records received by Lambda: 2
- Records processed: 2
- Invalid records: 0
- Anomalies detected: 1
- Event-source mapping state: Enabled
- Last processing result: OK

### Speed-Layer Snapshot Persistence

The Lambda now persists its latest analytics view in:

`s3://scp-speed-results-25186396/speed/latest_snapshot.json`

The stored document contains:

- Schema version
- Generation timestamp
- Processing summary
- Recent anomalies
- Requests per endpoint
- Error rates
- Traffic by hour
- Status-code distribution
- Response-byte totals
- Sliding-window boundaries

Controlled persistence result:

- Records processed: 2
- Window event count: 2
- Total response bytes: 400
- HTTP 200 responses: 1
- HTTP 500 responses: 1
- Endpoint error rate: 0.50
- Anomalies detected: 1
- S3 persistence result: stored
- Content type: application/json
- Server-side encryption: AES256

A temporary S3 failure is logged without incorrectly marking already
processed Kinesis records as invalid.

### Automated Tests

- Total tests: 36
- Result: all tests passed
- Execution time: 15.934 seconds

The local Spark warnings appeared before or after the unittest output,
but the final unittest result was `OK`.

### Evidence

Controlled comparison evidence:

- `docs/evidence/day4/01_producer_fixture_validation.txt`
- `docs/evidence/day4/02_batch_fixture_validation.txt`
- `docs/evidence/day4/03_batch_alignment_confirmation.txt`
- `docs/evidence/day4/04_batch_metrics_validation.txt`
- `docs/evidence/day4/05_stream_metrics_validation.txt`
- `docs/evidence/day4/06_batch_stream_comparison_clean.txt`
- `docs/evidence/day4/07_full_test_suite.txt`

Lambda and AWS evidence:

- `docs/evidence/day4/08_lambda_handler_tests.txt`
- `docs/evidence/day4/09_full_suite_with_lambda.txt`
- `docs/evidence/day4/10_real_kinesis_lambda_e2e.txt`
- `docs/evidence/day4/11_full_suite_with_s3_persistence.txt`
- `docs/evidence/day4/12_s3_snapshot_persistence_e2e.txt`

Machine-readable evidence:

- `results/integration/batch_stream_comparison.json`

Important commits:

- `e8bcb14` - Validate batch and streaming analytics parity
- `67aa834` - Add and validate Kinesis-triggered Lambda processing
- `9bb4324` - Persist speed-layer snapshots in Amazon S3
## Current Git Status

Active integration branch:

- `maryhelen-integration`

Latest integration commit:

- `9bb4324` - Persist speed-layer snapshots in Amazon S3

Pull Request:

- PR #3
- Source: `maryhelen-integration`
- Target: `main`
- Status: Draft
- Merge conflicts: none
- Review requested from Nalini
- Merge must wait for confirmation of the latest EMR setup, benchmark
  configuration and AWS evidence.

The older `nalini-batch-layer` branch must not be merged directly
because its work has already been incorporated and reorganised in the
integration branch.
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
- [x] Add a Kinesis-triggered AWS Lambda handler
- [x] Document the Kinesis event-source mapping
- [x] Persist the latest speed-layer snapshot in Amazon S3
- [ ] Add the serving-layer comparison between historical baseline and recent traffic
- [ ] Add or document Athena queries over batch results in Amazon S3
- [ ] Run benchmarks with multiple controlled traffic volumes
- [ ] Produce final benchmark graphs
- [ ] Update the architecture diagram
- [ ] Update the README with final execution instructions
- [ ] Prepare the technical report
- [ ] Prepare presentation slides and demonstration notes
- [ ] Run the final automated test suite
- [ ] Mark PR #3 as ready for review
- [ ] Merge PR #3 into `main`
- [ ] Confirm that `main` is clean and deployable
## Current Priority

Day 4 is complete.

The next independent technical task is the serving layer:

`Amazon S3 batch results + speed/latest_snapshot.json -> combined serving view`

The serving layer should read the recent speed-layer snapshot, load the
historical batch baseline when available and expose a clear comparison
between recent and historical traffic.
