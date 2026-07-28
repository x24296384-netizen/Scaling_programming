# Project Status

## Project

Scalable Web Log Analytics

Deadline: 4 August 2026

The project analyses a large Nginx access-log dataset using two processing paths:

- PySpark batch processing on Amazon EMR
- Real-time processing using Amazon Kinesis

## Team Responsibilities

- Maryhelen: real-time and Kinesis processing
- Nalini: batch processing, Amazon EMR, scaling and benchmarking
- Shared: schema alignment, integration, evidence, report and demonstration

## Current Shared Schema

The batch and real-time parsers currently use:

- client_ip
- timestamp
- method
- resource
- protocol
- status_code
- response_bytes
- referrer
- user_agent
- extra

The following streaming metadata fields are proposed but have not yet been added:

- event_id
- ingested_at
- source

## Schema Decision Pending

The original proposal used `endpoint`, but the existing batch job, real-time parser and tests currently use `resource`.

The team must confirm one common name before further integration:

- keep `resource`; or
- rename `resource` to `endpoint` across both processing paths and all tests.

## Day 1 Completed

- Confirmed the active branch: `maryhelen-integration`
- Confirmed that batch and real-time code already use `client_ip`
- Confirmed that batch and real-time code already use `response_bytes`
- Added Kinesis record creation
- Used `client_ip` as the Kinesis partition key
- Added batch sending through `put_records`
- Added retry logic that resends only failed records
- Added `boto3` to the project requirements
- Added unit tests for Kinesis record creation and retry behaviour
- Corrected Spark test shutdown on Windows
- Ran the complete automated test suite successfully

## Test Evidence

- Total tests: 7
- Result: all tests passed
- Exit code: 0
- Spark tests now finish without manual termination

Local Spark may still display Windows-specific warnings about `winutils.exe`,
native Hadoop libraries, sockets and temporary-directory cleanup. These warnings
appear after the successful test result and do not change the exit code.

## Git Evidence

Commits pushed to `maryhelen-integration`:

- `2bbbca9` - Fix Spark test shutdown on Windows
- `367d3ae` - Add Kinesis record batching with retry support

The local and remote branches are currently aligned.

## Next Tasks

- [ ] Confirm the final shared field name: `resource` or `endpoint`
- [ ] Confirm whether `extra` remains part of the official schema
- [ ] Decide where `event_id`, `ingested_at` and `source` will be added
- [ ] Connect the replay utility to the Nginx parser
- [ ] Add command-line options for input file, stream name and replay speed
- [ ] Enforce Amazon Kinesis batch limits
- [ ] Test the producer with mocked AWS responses
- [ ] Run an initial test against an Amazon Kinesis stream
- [ ] Save screenshots and evidence in the shared OneDrive folder
