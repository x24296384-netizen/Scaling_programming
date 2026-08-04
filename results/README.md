# Generated Results

This directory contains small machine-generated outputs retained for reproducibility.

Large Amazon EMR CSV outputs are not stored in Git because of their size. Their manifests, validation evidence and screenshots are available under `docs/evidence/batch/`.

## integration

Controlled Batch-Speed comparison used to validate schema and metric consistency.

## serving

Final outputs combining real historical batch metrics with the recent global speed-layer snapshot.

## speed

Local streaming, Amazon Kinesis, AWS Lambda, S3 and global-aggregation benchmark results.

Report-ready charts, logs and screenshots are stored under `docs/evidence/`.

### Authoritative Lambda-S3 validation runs

The files named `lambda_s3_validation_*.json` under
`speed/benchmarks/` are the authoritative deployed
Kinesis-Lambda-S3 executions used in Table III of the final report and by the
performance-chart generator.

The reported results are:

- 100 events: 59.831 records/s and 3.604 s snapshot observation;
- 500 events: 297.018 records/s and 4.663 s snapshot observation;
- 1,000 events: 547.726 records/s and 180.952 s observation timeout.

Earlier exploratory executions named `lambda_s3_load_*.json` are retained
under `speed/benchmarks/superseded/` for traceability. They are not the source
of the final reported values.
