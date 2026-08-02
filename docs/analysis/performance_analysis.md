# Performance and Scalability Analysis

## 1. Evaluation Scope

The evaluation considered both processing paths of the Lambda Architecture:

- the historical batch path implemented with PySpark on Amazon EMR;
- the near-real-time path implemented with Amazon Kinesis Data Streams, AWS Lambda and Amazon S3.

The objective was not only to confirm correct execution, but also to measure how the system behaved when more processing resources or larger event volumes were used.

## 2. Batch-Layer Scalability

The same full Nginx access-log dataset was processed with one, two and four EMR worker nodes.

| Workers | Execution time | Speedup | Efficiency |
|---:|---:|---:|---:|
| 1 | 410.4 s | 1.00x | 100% |
| 2 | 380.1 s | 1.08x | 54% |
| 4 | 294.1 s | 1.40x | 35% |

Speedup was calculated as:

```text
speedup = execution_time_with_1_worker / execution_time_with_n_workers
```

Efficiency was calculated as:

```text
efficiency = speedup / number_of_workers
```

The two-worker configuration reduced execution time by 30.3 seconds, which corresponds to an improvement of approximately 7.4%. The four-worker configuration reduced execution time by 116.3 seconds compared with one worker, which corresponds to an improvement of approximately 28.3%.

The results therefore show that the workload benefited from additional workers, but the improvement was sub-linear. Four workers did not produce a four-times speedup; they produced a speedup of 1.40x.

This behaviour is expected for a distributed Spark workload of this size because some costs do not decrease when workers are added. These costs include cluster and executor start-up, task scheduling, serialisation, shuffle, communication and final output writing. As worker count increases, these fixed and coordination costs represent a larger proportion of the total execution time.

The fall in efficiency from 100% to 54% and then 35% shows diminishing returns. In this experiment, four workers achieved the shortest execution time, but not the best cost efficiency. The results support the use of horizontal scaling, while also showing that increasing cluster size should be justified by the required completion time rather than assuming linear improvement.

## 3. Batch Data Volume and Correctness

The final single-run EMR execution processed:

- 10,365,152 raw lines;
- 10,365,077 valid records;
- 75 rejected records;
- 893,048 endpoint aggregate rows;
- 177,634 error responses;
- 128,870,996,472 response bytes.

All ten final CSV outputs came from the same EMR execution. The main aggregates were reconciled through Amazon Athena, which confirmed that requests, errors and response-byte totals were internally consistent.

This is important for scalability analysis because faster execution is only useful when the distributed result remains correct. The final evaluation therefore considered both runtime and output reconciliation.

## 4. Local Streaming Benchmark

The local streaming benchmark processed the full dataset through parsing, Kinesis-style record construction, consumer decoding and sliding-window analytics.

Measured results were:

- runtime: 352.8049 seconds;
- throughput: 29,379.27 lines per second;
- valid-record throughput: 29,379.06 records per second;
- sampled mean local latency: 0.0351 ms;
- sampled p95 local latency: 0.06968 ms;
- failed records: 0.

The latency values represent local in-process work and do not include AWS network transfer, Kinesis service delay or Lambda scheduling. They should therefore be interpreted as implementation-level processing latency rather than complete cloud end-to-end latency.

## 5. Kinesis and Lambda Validation

A controlled real Kinesis smoke test confirmed the cloud path:

```text
producer -> Kinesis -> consumer
```

The test sent and received three records with no failures. The observed end-to-end latency was approximately 300.444 ms. Because this was a small functional smoke test, it confirms connectivity and processing behaviour rather than maximum service throughput.

The Lambda path was later validated with real Kinesis-triggered executions. The function processed records, updated sliding-window metrics, detected configured error-rate anomalies and persisted speed-layer outputs to Amazon S3.

## 6. Speed-Layer Load Validation

The Kinesis-to-Lambda-to-S3 path was tested with controlled loads of 100, 500 and 1,000 events.

| Load | Successfully sent | Failed | Local Lambda snapshot | Global endpoint count |
|---:|---:|---:|---:|---:|
| 100 | 100 | 0 | 100 | 100 |
| 500 | 500 | 0 | 500 | 500 |
| 1,000 | 1,000 | 0 | 600 | 1,000 |

All submitted events were successfully sent and no producer failures were recorded.

For 100 and 500 events, the most recent Lambda snapshot contained the complete endpoint count. For 1,000 events, the most recent local snapshot contained only 600 events even though all 1,000 events had been accepted.

This was caused by Lambda concurrency. Multiple execution environments processed different parts of the stream, and each environment maintained only its own in-memory sliding-window state. Therefore, a single latest snapshot could not be treated as a globally complete result.

The design was improved by storing immutable per-invocation event deltas under `speed/batches/`. The global aggregator then:

- read the persisted deltas;
- removed duplicate retry events;
- discarded invalid documents and events;
- rebuilt the 300-second event-time window;
- wrote `speed/global_snapshot.json`.

The global aggregator recovered all 1,000 events. This result demonstrates that the speed layer can remain correct under concurrent Lambda execution when immutable deltas are used to reconstruct global state.

## 7. Real Batch-Speed Serving Validation

A final end-to-end validation compared the historical batch baseline with a real recent speed-layer window for `/settings/logo`.

Historical values:

- historical endpoint requests: 352,047;
- historical baseline RPM: 52.25575181831676.

Recent values:

- recent events requested: 600;
- successfully sent: 600;
- failed: 0;
- window length: 300 seconds;
- recent RPM: 120.0;
- duplicate events: 0;
- invalid documents: 0;
- invalid events: 0.

The recent RPM was calculated as:

```text
recent_rpm = 600 x 60 / 300 = 120.0
```

The comparison result was:

```text
rpm_difference = 120.0 - 52.25575181831676
               = 67.744248

recent_to_baseline_ratio = 120.0 / 52.25575181831676
                         = 2.296398
```

The configured traffic-spike rule required:

- a recent-to-baseline ratio of at least 2.0;
- at least 10 recent requests.

Both conditions were satisfied, so the endpoint was classified as a significant traffic increase.

The recent error rate was 0.05, which was below the separate error-anomaly threshold of 0.50. Therefore, the same execution produced one traffic spike and zero error-rate anomalies. This confirms that the serving layer distinguishes traffic-volume anomalies from error-rate anomalies.

## 8. Interpretation

The combined results support four main conclusions.

First, the batch workload benefited from horizontal scaling, but the measured speedup was sub-linear. Additional workers reduced completion time, although efficiency decreased as coordination and fixed Spark costs became more significant.

Second, the speed layer accepted the tested event volumes without producer failures. However, the 1,000-event test exposed an important distributed-state limitation: one Lambda execution environment cannot represent global stream state under concurrency.

Third, immutable deltas and global event-time reconstruction corrected this limitation. The aggregator recovered the complete recent window without duplicate or invalid events.

Fourth, the final serving validation successfully combined real historical EMR results with a real Kinesis-Lambda-S3 recent window and correctly detected a request-rate increase of approximately 2.30 times the historical baseline.

## 9. Limitations

The evaluation has several limitations:

- each EMR worker configuration was measured with a limited number of final runs;
- the AWS Academy Learner Lab restricted execution time and infrastructure availability;
- a live EMR auto-scaling event was not observed during the available session;
- the Kinesis and Lambda load tests were controlled validations rather than service-saturation tests;
- local latency measurements exclude AWS network and managed-service delay;
- explicit global aggregation was used instead of a scheduled production workflow;
- the results demonstrate behaviour for this dataset and configuration, not a production service-level agreement.

These limitations mean that the results demonstrate correct scalable behaviour under the evaluated conditions, but they should not be presented as proof of linear or production-scale performance.
