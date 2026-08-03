# Project Evidence

The evidence is organised by technical purpose rather than by development day.
Each folder contains screenshots, terminal outputs or report-ready artefacts
supporting the implementation and evaluation described in the project report.

## Evidence structure

### [Athena](athena/screenshots/)

Screenshots showing the Athena database, external tables, views, validation
queries and serving-layer analysis over the batch and speed results.

### [Batch](batch/)

Evidence from the PySpark batch layer executed on Amazon EMR, including:

- cluster and step execution;
- worker-count benchmarks;
- data-quality results;
- Amazon S3 output folders;
- auto-scaling configuration and workload-pressure evaluation;
- sanitised post-test instance inventory.

See the [batch evidence index](batch/README.md) for the complete file list and
interpretation.

### [Charts](charts/)

Performance charts generated from the recorded EMR, Kinesis and AWS Lambda
benchmark results.

### [Integration](integration/)

Evidence of shared-schema validation and controlled comparison between the
batch and speed layers.

### [Serving](serving/)

Evidence from the combined serving layer, including historical baseline,
recent request rate, error-rate comparison and traffic-spike classification.

### [Speed](speed/)

Evidence from the deployed Kinesis, AWS Lambda and Amazon S3 speed layer,
including end-to-end ingestion, snapshot generation and global aggregation.

### [Tests](tests/)

Targeted component tests and complete automated test-suite results.

## Machine-generated results

Raw JSON, CSV and benchmark outputs are stored under
[`results/`](../../results/). Keeping these outputs separate from screenshots
allows the calculations and performance charts to be inspected and reproduced.

## Public evidence

Public screenshots and infrastructure inventories have been sanitised to remove
account-specific network and instance identifiers. The measured results and
technical interpretation have been preserved.
