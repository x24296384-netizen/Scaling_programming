# Scalable Web Log Analytics

A scalable cloud programning project for processing and analysins web server access logs using sequential and parallel approaches.
## Batch Layer (PySpark on Amazon EMR)

### Overview
The batch layer implements the correctness-over-history side of the Lambda
Architecture. It reads the complete web server access log from Amazon S3,
parses it with PySpark, and computes four aggregate views over the full
historical dataset.

### Prerequisites
- AWS CLI configured with valid credentials (AWS Academy Learner Lab or
  standard AWS account)
- An existing S3 bucket with the raw log uploaded to `raw-data/access.log`
- An EC2 key pair created in your target region
- A subnet ID in your target VPC

### Setup

1. **Fill in your AWS values** in `infra/emr_setup.sh`:
```bash
   S3_LOGS_BUCKET="s3://<your-bucket>/emr-logs/"
   S3_RAW_DATA_BUCKET="s3://<your-bucket>/raw-data/"
   KEY_PAIR="<your-key-pair-name>"
   SUBNET_ID="<your-subnet-id>"
```

2. **Launch the EMR cluster:**
```bash
   chmod +x infra/emr_setup.sh
   ./infra/emr_setup.sh
```
   This creates a cluster (Spark 3.5.0 on EMR 7.1.0, m5.xlarge instances)
   with a managed auto-scaling policy attached to the core instance group
   (scale out below 15% available YARN memory, scale in above 75%, 300s
   cooldown, bounded between 1 and 5 core nodes). It prints the cluster ID
   — track its status with:
```bash
   aws emr describe-cluster --cluster-id <CLUSTER_ID> --query 'Cluster.Status.State' --output text
```
   Wait for `WAITING` before submitting jobs.

3. **Upload the batch script to S3** (EMR reads it from there, not locally):
```bash
   aws s3 cp batch/batch_job.py s3://<your-bucket>/scripts/batch_job.py
```

4. **Submit the batch job as an EMR step:**
```bash
   aws emr add-steps --cluster-id <CLUSTER_ID> --steps Type=Spark,Name="BatchJob",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,s3://<your-bucket>/scripts/batch_job.py,--input,s3://<your-bucket>/raw-data/access.log,--output,s3://<your-bucket>/batch-results/,--workers,1]
```
   Track the step:
```bash
   aws emr describe-step --cluster-id <CLUSTER_ID> --step-id <STEP_ID> --query 'Step.Status.State' --output text
```

5. **Outputs**, once `COMPLETED`, land in `s3://<your-bucket>/batch-results/`:
   - `requests_per_endpoint/` — total requests per endpoint
   - `traffic_by_hour/` — request volume by hour of day
   - `error_rates/` — error rate per endpoint (status codes ≥ 400)
   - `baseline_rpm/` — average requests-per-minute per endpoint (the
     baseline the speed layer's live traffic is compared against)
   - `data_quality/` — total/valid/malformed record counts

6. **Terminate the cluster** when done to avoid ongoing cost:
```bash
   aws emr terminate-clusters --cluster-id <CLUSTER_ID>
```

### Benchmarking

To reproduce the 1/2/4-worker speedup and efficiency measurements, resize
the core instance group between runs and re-submit the same job:
```bash
aws emr modify-instance-groups --instance-groups InstanceGroupId=<CORE_GROUP_ID>,InstanceCount=<N>
```
Confirm the resize completed (`aws emr list-instances ...`) before
submitting the next run, and confirm each step is `COMPLETED` before
resizing again — resizing mid-run gives an invalid timing.

Reference results from this project (m5.xlarge, full ~3.5GB dataset):

| Workers | Execution Time | Speedup | Efficiency |
|---|---|---|---|
| 1 | 410.4s | 1.00× | 100% |
| 2 | 380.1s | 1.08× | 54% |
| 4 | 294.1s | 1.40× | 35% |

### Known Limitations
- Development on this project used the AWS Academy Learner Lab, which
  periodically resets active sessions, terminating running EMR clusters
  and the Cloud9 development environment without warning. If a cluster
  disappears unexpectedly mid-task, this is the most likely cause —
  relaunch with `infra/emr_setup.sh` and resubmit any in-progress steps.
- The auto-scaling policy is configured and verified via the EMR console,
  but a live scale-out trigger could not be demonstrated within the
  Learner Lab's resource constraints during testing.
