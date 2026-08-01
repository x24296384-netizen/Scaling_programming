# AWS Speed-Layer Screenshot Evidence

## 01_kinesis_stream_active.png
Shows the Kinesis data stream in Active state, using provisioned capacity with one shard.

## 02_kinesis_producer_send_success.png
Shows a real producer execution with two records sent successfully, zero failures, sequence numbers and shard identifiers.

## 03_lambda_kinesis_trigger_enabled.png
Shows the enabled Kinesis event-source mapping, batch size 100, retry configuration and last processing result OK.

## 04_lambda_environment_variables.png
Shows the five-minute window, anomaly thresholds, S3 results bucket, snapshot key and immutable batch prefix.

## 05_lambda_manual_test_success.png
Shows a successful Lambda execution with two processed records, zero invalid records and an anomaly detected.

## 06_cloudwatch_batch_processing.png
Shows consecutive Lambda batches processing 100 records each, zero invalid records and window growth from 100 to 200 events.

## 07_s3_speed_snapshots.png
Shows latest_snapshot.json, global_snapshot.json and the batches prefix in Amazon S3.

## 08_s3_immutable_batch_deltas.png
Shows multiple timestamped JSON delta files partitioned by year, month, day and hour.
