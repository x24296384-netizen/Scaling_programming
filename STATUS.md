# Project Status

## Project

Scalable Web Log Analytics

Deadline: 4 August 2026 at 5:00 pm

The project analyses a large Nginx access-log dataset through two processing paths:

- PySpark batch processing on Amazon EMR
- Real-time processing using Amazon Kinesis Data Streams

The two paths form a Lambda architecture. The batch layer provides accurate historical results, while the speed layer provides recent, low-latency analytics.

## Team Responsibilities

- Maryhelen: real-time ingestion, Kinesis replay and speed-layer processing
- Nalini: PySpark batch processing, Amazon EMR, scaling and batch benchmarking
- Shared: schema alignment, serving-layer integration, evidence, report and demonstration

## Current Shared Schema — Frozen

The batch and real-time processing paths now use the same base schema:

- client_ip
- timestamp
- method
- endpoint
- protocol
- status_code
- response_bytes
- referrer
- user_agent

The Kinesis replay path adds the following streaming metadata:

- event_id
- ingested_at
- source

Schema rules:

- `timestamp` uses ISO 8601 format.
- `status_code` is stored as an integer.
- `response_bytes` is stored as an integer.
- `endpoint` is the official field name.
- `resource` is no longer used.
- The final raw-log field is used internally to recognise the dataset format but is not included in the official event schema.
- `event_id` is generated as a UUID.
- `ingested_at` is generated as a timezone-aware UTC timestamp.
- `source` is set to `nginx_access_log`.

## Schema Decision Completed

The shared schema was frozen on Day 2.

Both the PySpark batch path and the Kinesis replay path now use `endpoint`. This avoids unnecessary field conversion when the batch and speed results are later combined in the serving layer.

The `extra` field remains part of the regular expression used to recognise the complete raw Nginx line, but it is not included in the final shared event.

## Day 1 Completed

- Confirmed the active branch: `maryhelen-integration`
- Confirmed that batch and real-time code use `client_ip`
- Confirmed that batch and real-time code use `response_bytes`
- Added Kinesis record creation
- Used `client_ip` as the Kinesis partition key
- Added batch sending through `put_records`
- Added retry logic that resends only failed records
- Added `boto3` to the project requirements
- Added unit tests for Kinesis record creation and retry behaviour
- Corrected Spark test shutdown behaviour on Windows
- Ran the Day 1 automated test suite successfully

## Day 2 Progress

### Shared Schema

- Renamed `resource` to `endpoint` in the producer parser
- Renamed `resource` to `endpoint` in the PySpark batch parser
- Updated the PySpark batch analytics to group results by `endpoint`
- Renamed the batch result from `requests_per_resource` to `requests_per_endpoint`
- Removed `extra` from the final shared event schema
- Confirmed that parser timestamps use ISO 8601
- Confirmed that `status_code` is an integer
- Confirmed that `response_bytes` is an integer

### Streaming Metadata

- Added `event_id` during Kinesis record creation
- Added `ingested_at` as a timezone-aware UTC timestamp
- Added `source` with the value `nginx_access_log`
- Confirmed that `event_id` is a valid UUID
- Confirmed that the original parsed event is not modified when metadata is added

### Parsing and Replay

- Added safe handling for malformed timestamps
- Connected the Nginx parser to Kinesis record preparation
- Added invalid-line counting
- Ensured that invalid lines do not stop the complete replay
- Added progressive file processing without loading the complete dataset into memory
- Added replay support for `tests/fixtures/sample_access.log`
- Enforced a Kinesis batch-size range from 1 to 500 records
- Added support for full and final partial batches

### Kinesis Reliability

- Added partial-failure handling
- Retried only the records that failed
- Added a configurable maximum number of attempts
- Added configurable retry delay
- Counted records that still fail after the retry limit
- Confirmed that successful records are not sent again

### Batch Alignment

- Updated the batch output schema to use `endpoint`
- Removed `extra` from the selected valid batch records
- Updated requests-per-endpoint analytics
- Updated error-rate analytics to use `endpoint`
- Updated baseline requests-per-minute analytics to use `endpoint`
- Confirmed that the batch parser normalises timestamps to UTC

### Day 2 Verification

- Ran the complete automated test suite successfully
- Parser tests: 3 passed
- Kinesis replay tests: 6 passed
- PySpark batch tests: 3 passed
- Total: 12 tests passed
- Result: OK
- Execution time: 15.919 seconds
- Checked changed files using `git diff --check`
- No trailing whitespace or invalid blank-line errors remain

## Current Analytics

The batch path currently calculates:

- Requests per endpoint
- Traffic by hour
- Error rate per endpoint
- Average requests per minute per endpoint

The planned speed layer must calculate comparable recent analytics:

- Requests per endpoint
- Error rate
- Traffic by hour
- Status-code distribution
- Sliding-window request counts

## Test Evidence

### Day 1 Baseline

- Total tests: 7
- Result: all tests passed
- Exit code: 0

### Day 2 Parser Tests

- Valid Nginx line parsing
- Invalid log-line handling
- Invalid timestamp handling
- Result: 3 tests passed

### Day 2 Kinesis Replay Tests

- Kinesis record construction
- Streaming metadata creation
- Partial-failure retry
- Retry of failed records only
- Permanent-failure reporting
- Invalid-line rejection and counting
- Multiple-batch processing
- File replay using the sample fixture
- Result: 6 tests passed

### Day 2 Batch Tests

- Batch parser uses the frozen `endpoint` schema
- Batch timestamps are normalised to UTC
- Batch metrics use `endpoint`
- Data-quality reporting remains correct
- Result: 3 tests passed

### Full Suite

- Parser tests: 3 passed
- Kinesis replay tests: 6 passed
- PySpark batch tests: 3 passed
- Total: 12 tests passed
- Result: OK
- Execution time: 15.919 seconds

Local Spark may display Windows-specific warnings about:

- `winutils.exe`
- Native Hadoop libraries
- Local sockets
- Temporary-directory cleanup

These warnings are related to running Spark locally on Windows. They may appear during or after successful tests and do not change the final unittest result or exit code.

## Git Evidence

Active branch:

- `maryhelen-integration`

Previously pushed commits:

- `2bbbca9` — Fix Spark test shutdown on Windows
- `367d3ae` — Add Kinesis record batching with retry support
- `849e0b1` — Add project status and task list

The Day 2 schema, metadata, batch alignment and file-replay changes are currently local and must still be committed and pushed.

## Evidence to Save

Recommended Day 2 evidence files:

- `Day2_01_Initial_Git_Status.png`
- `Day2_02_Parser_Baseline_Passed.png`
- `Day2_03_Invalid_Timestamp_Handled.png`
- `Day2_04_Parser_Tests_Passed.png`
- `Day2_05_Replay_Baseline_Passed.png`
- `Day2_06_Permanent_Failure_Reported.png`
- `Day2_09_Invalid_Lines_Skipped.png`
- `Day2_12_Kinesis_Batching_Tests_Passed.png`
- `Day2_14_Replay_File_Tests_Passed.png`
- `Day2_15_Parser_And_Replay_Tests_Passed.png`
- `Day2_17_Frozen_Parser_Schema_Passed.png`
- `Day2_19_Endpoint_Metadata_Replay_Tests_Passed.png`
- `Day2_20_Resource_References_Audit.png`
- `Day2_22_Batch_Endpoint_Schema_Passed.png`
- `Day2_23_Batch_Endpoint_Metrics_Passed.png`
- `Day2_24_Complete_Test_Suite_Passed.png`

Screenshots must not expose AWS access keys, secret keys or session tokens.

## Next Tasks

- [x] Freeze `endpoint` as the official shared field name
- [x] Remove `extra` from the official event schema
- [x] Confirm ISO 8601 timestamps
- [x] Confirm integer status and byte fields
- [x] Add `event_id`, `ingested_at` and `source`
- [x] Connect the replay utility to the Nginx parser
- [x] Count and skip invalid log lines
- [x] Enforce the Amazon Kinesis maximum batch size
- [x] Test full and partial Kinesis batches
- [x] Retry only failed Kinesis records
- [x] Report permanent failures
- [x] Test replay using the sample access-log file
- [x] Align the PySpark batch path with `endpoint`
- [x] Run the complete automated test suite
- [x] Check all changed files with `git diff --check`
- [ ] Commit and push the frozen schema and replay changes
- [ ] Send the commit number and test result to Nalini
- [ ] Add command-line options for input file, stream name and replay speed
- [ ] Implement speed-layer sliding-window analytics
- [ ] Calculate requests per endpoint in recent windows
- [ ] Calculate recent error rate
- [ ] Calculate recent traffic by hour
- [ ] Calculate recent status-code distribution
- [ ] Measure speed-layer latency and throughput
- [ ] Run an initial test against an Amazon Kinesis stream
- [ ] Save final Day 2 screenshots and logs in the shared OneDrive folder