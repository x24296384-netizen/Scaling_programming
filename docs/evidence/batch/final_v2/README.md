# Final V2 EMR Batch Evidence

This directory contains compact evidence for the final, internally consistent
EMR batch execution.

## Execution

- Commit: `120dda8`
- EMR cluster: `j-2KIK1VQPJT200`
- EMR step: `s-09947463792EMAWECP0P`
- Total raw lines: 10,365,152
- Valid records: 10,365,077
- Invalid records: 75
- Rejection reason: `invalid_request`

## Files

- `01_delivery_manifest.txt`: file sizes and SHA-256 hashes for the final ZIP
  and all ten CSV outputs.
- `02_s3_before_final_v2.txt`: S3 objects from the superseded batch delivery.
- `03_s3_final_v2_listing.txt`: the ten final CSV objects uploaded to S3.
- `04_small_output_validation.txt`: corrected validation of the data-quality
  and rejected-record evidence.
- `05_final_reconciliation_summary.txt`: complete reconciliation summary for
  all final aggregates.

Earlier deliveries named `batch_csv_delivery.zip`, `rebased_extras.zip` and
`full_rebased_delivery.zip` are superseded and must not be used.

The large CSV files and delivery ZIP are intentionally excluded from Git.
