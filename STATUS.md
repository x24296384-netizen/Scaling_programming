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
## Day 5 - Global Speed Aggregation and Load Validation

Status: Complete

Completed work:

- Added immutable per-invocation Lambda delta documents under `speed/batches/`.
- Added `speed/global_aggregator.py`.
- Rebuilt the complete event-time window across concurrent Lambda environments.
- Added event deduplication and invalid-document accounting.
- Wrote the consolidated result to `speed/global_snapshot.json`.
- Updated the serving layer to use the global snapshot instead of treating one Lambda environment as global state.
- Added automated tests for global aggregation.

AWS validation results:

| Load | Successful | Failed | Local snapshot | Global endpoint count |
|---:|---:|---:|---:|---:|
| 100 | 100 | 0 | 100 | 100 |
| 500 | 500 | 0 | 500 | 500 |
| 1,000 | 1,000 | 0 | 600 | 1,000 |

The 1,000-event result demonstrates why the local Lambda snapshot cannot be treated as globally complete. Multiple Lambda execution environments processed the stream, but the immutable deltas allowed the global aggregator to recover all 1,000 events for the benchmark endpoint.

Important commits:

- `33135c8` - Add global speed-layer aggregation
- `7c8aacf` - Validate global aggregation under Lambda concurrency

## Day 6 - Athena and Endpoint RPM Comparison

Status: Functionality complete; final historical-data validation pending

Completed work:

- Added Athena database, external-table, view, validation and demonstration SQL.
- Deployed database `scp_web_logs_25186396` in `us-east-1`.
- Configured the `primary` workgroup result location as `s3://scp-speed-results-25186396/athena-results/`.
- Created seven external tables and two views.
- Validated basic Athena query execution.
- Added endpoint-level historical baseline versus recent RPM comparison.
- Added configurable traffic-spike ratio and minimum-request thresholds.
- Added ranked traffic-spike summaries.
- Validated the comparison with a controlled baseline.
- Completed a full suite of 61 automated tests.

Important commits:

- `5268911` - Add Athena tables and deployment documentation
- `75e00da` - Add endpoint RPM baseline comparison

## Batch Evidence Received

Received from the batch-layer owner:

- EMR benchmark documents and report material;
- execution-time, speedup and efficiency graphs;
- EMR step and S3 screenshots;
- auto-scaling policy evidence;
- final benchmark timings;
- data-quality totals.

Confirmed batch results:

- Total lines: 10,365,152
- Valid records: 10,364,866
- Rejected records: 286
- 1 worker: 410.4 seconds
- 2 workers: 380.1 seconds, 1.08× speedup, 54% efficiency
- 4 workers: 294.1 seconds, 1.40× speedup, 35% efficiency

Still required from the batch-layer owner:

- the actual small `part-*.csv` result files;
- a conflict-free batch data-quality and rejection-evidence commit based on the latest integration branch;
- correction of documentation that states all CSV evidence has already been delivered.

## Current Git Status

Active integration branch:

- `maryhelen-integration`

Recent completed integration milestones:

- `5268911` - Add Athena tables and deployment documentation
- `75e00da` - Add endpoint RPM baseline comparison

Repository state:

- Athena infrastructure and endpoint RPM comparison are committed and synchronised with `origin/maryhelen-integration`.
- Final batch-data and Athena-result integration remains pending.
- The older `nalini-batch-layer` branch must not be merged directly. Its later data-quality changes need to be reapplied by the batch-layer owner on top of the current integration branch.

## Local Spark Warnings

Local Spark execution on Windows may show warnings relating to:

- `winutils.exe`
- `HADOOP_HOME`
- Native Hadoop libraries
- Local socket cleanup
- Spark temporary-directory deletion

These warnings do not invalidate successful test results when the unittest summary ends with `OK`.

## Remaining Technical Tasks

- [ ] Receive the real EMR `part-*.csv` outputs
- [ ] Receive a conflict-free batch data-quality/rejection-evidence commit
- [ ] Upload the real batch result folders to the project S3 bucket
- [ ] Execute the four Athena reconciliation checks with real data
- [ ] Run the final Athena demonstration queries
- [ ] Generate the real batch serving document
- [ ] Generate the final combined serving view using real `baseline_rpm`
- [ ] Capture final Athena and serving-layer evidence
- [ ] Update the architecture diagram with immutable deltas and global aggregation
- [ ] Add final evidence files to the organised evidence folders
- [ ] Update the technical report
- [ ] Prepare presentation slides and demonstration notes
- [ ] Run and save the final automated test suite
- [ ] Mark the integration pull request ready for review
- [ ] Merge the integration branch into `main`
- [ ] Confirm that `main` is clean and deployable

Completed technical tasks:

- [x] Shared schema alignment
- [x] Reliable Kinesis replay with retries
- [x] Sliding-window speed analytics
- [x] Kinesis-triggered Lambda
- [x] S3 snapshot persistence
- [x] Immutable Lambda batch deltas
- [x] Global speed-layer aggregation
- [x] Controlled 100/500/1,000-event AWS validation
- [x] Performance graphs
- [x] Athena database, tables and views
- [x] Endpoint-level RPM baseline comparison
- [x] 61-test automated suite

## Current Priority

The remaining critical path is data integration rather than new architecture development:

```text
real EMR part CSV files
        ↓
project S3 batch prefixes
        ↓
Athena reconciliation and demo queries
        ↓
real batch serving document
        ↓
global speed snapshot + historical baseline
        ↓
final combined serving view and evidence
```

While the CSV dependency remains open, independent work can continue on the architecture diagram, evidence organisation, report, presentation and final merge preparation.
