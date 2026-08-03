# Batch and EMR Evidence

This directory contains the retained evidence for the historical PySpark
batch layer, Amazon EMR execution, worker benchmarks, final output delivery
and the final automatic-scaling trigger attempt.

## Final batch execution

The final validated batch run processed the complete Nginx dataset and
generated all ten analytical and data-quality CSV outputs from the same
Amazon EMR execution.

Confirmed results:

- Total raw lines: 10,365,152
- Valid records: 10,365,077
- Rejected records: 75
- Endpoint aggregate rows: 893,048
- Error responses: 177,634
- Total response bytes: 128,870,996,472
- Final CSV outputs: 10

Final execution provenance:

- EMR cluster: `j-2KIK1VQPJT200`
- EMR step: `s-09947463792EMAWECP0P`
- Source commit: `120dda8` on `origin/nalini-batch-layer`

## Benchmark evidence

The benchmark evidence covers executions with one, two and four worker
nodes.

| Workers | Execution time | Speedup | Efficiency |
|---:|---:|---:|---:|
| 1 | 410.4 s | 1.00x | 100% |
| 2 | 380.1 s | 1.08x | 54% |
| 4 | 294.1 s | 1.40x | 35% |

The observed scaling was sub-linear because Spark startup, scheduling,
communication, shuffle and final-output serialisation overheads remained
significant.

## Final EMR automatic-scaling attempt

The final retry corrected the earlier Step-concurrency configuration.

Verified conditions:

- Step Concurrency Level: 4
- Four Steps started concurrently at approximately 21:14 UTC
- Pending containers reached 28
- Initial core capacity: 2 instances
- Configured capacity range: 1–5 core instances
- No verified scale-out occurred

The complete post-test AWS CLI inventory was collected using an unfiltered
`aws emr list-instances` command. It contained two core instances and one
master instance, with no additional core instance.

The public JSON copy has been sanitised. Network addresses, DNS names,
EC2 identifiers, EBS volume identifiers and internal instance-group
identifiers were removed. The complete unedited output is retained in the
private project evidence archive.

## Evidence index

- `01_*` to `06_*`: EMR configuration, execution and benchmark evidence
- `07_emr_autoscaling_policy.png`: automatic-scaling policy configuration
- `08_emr_steps_completed.png`: completed EMR Steps
- `09_*` and `10_*`: final batch output and data-quality evidence
- `11_s3_batch_output_folders.png`: final S3 batch-output structure
- `12_emr_autoscaling_policy_and_two_cores.png`: policy and initial two-core configuration
- `13_emr_four_concurrent_steps_events.png`: four concurrent Step start events
- `14_emr_container_pending_28.png`: pending-container pressure during the test
- `15_emr_node_status_dashboard_context.png`: node-status monitoring context
- `16_emr_post_test_instance_inventory.json`: sanitised complete post-test instance inventory

## Interpretation

The retained evidence demonstrates a correctly attached automatic-scaling
policy and a genuine concurrent trigger attempt. It does not demonstrate
completed elastic scale-out. The result is therefore reported as no verified
scale-out rather than as successful automatic scaling.

