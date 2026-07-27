#!/bin/bash
# EMR cluster setup for the batch layer (Scalable Cloud Programming CA)
# This doesn't depend on which dataset we end up using — same cluster, same
# scaling policy either way. Fill in the bucket/subnet/key values before running.

set -e

# --- config, change these to match our AWS setup ---
CLUSTER_NAME="scp-batch-layer"
RELEASE_LABEL="emr-7.1.0"          # check what's actually available in our region
S3_LOGS_BUCKET="s3://REPLACE-ME/emr-logs/"
S3_RAW_DATA_BUCKET="s3://REPLACE-ME/raw-data/"   # where the batch job reads from
KEY_PAIR="REPLACE-ME-keypair"
SUBNET_ID="REPLACE-ME-subnet"
INSTANCE_TYPE="m5.xlarge"

# min/desired/max — good starting point, tune once we see real benchmark numbers
MIN_CAPACITY=1
DESIRED_CAPACITY=2
MAX_CAPACITY=5

echo "Creating EMR cluster: $CLUSTER_NAME"

CLUSTER_ID=$(aws emr create-cluster \
  --name "$CLUSTER_NAME" \
  --release-label "$RELEASE_LABEL" \
  --applications Name=Spark \
  --log-uri "$S3_LOGS_BUCKET" \
  --ec2-attributes KeyName="$KEY_PAIR",SubnetId="$SUBNET_ID" \
  --service-role EMR_DefaultRole \
  --instance-groups '[
    {
      "InstanceGroupType":"MASTER",
      "InstanceType":"'"$INSTANCE_TYPE"'",
      "InstanceCount":1
    },
    {
      "InstanceGroupType":"CORE",
      "InstanceType":"'"$INSTANCE_TYPE"'",
      "InstanceCount":'"$DESIRED_CAPACITY"',
      "AutoScalingPolicy": {
        "Constraints": {
          "MinCapacity": '"$MIN_CAPACITY"',
          "MaxCapacity": '"$MAX_CAPACITY"'
        },
        "Rules": [
          {
            "Name": "ScaleOutOnHighYARNMemory",
            "Description": "Add a core node when YARN memory is tight",
            "Action": {
              "SimpleScalingPolicyConfiguration": {
                "AdjustmentType": "CHANGE_IN_CAPACITY",
                "ScalingAdjustment": 1,
                "CoolDown": 300
              }
            },
            "Trigger": {
              "CloudWatchAlarmDefinition": {
                "ComparisonOperator": "LESS_THAN",
                "EvaluationPeriods": 1,
                "MetricName": "YARNMemoryAvailablePercentage",
                "Namespace": "AWS/ElasticMapReduce",
                "Period": 300,
                "Threshold": 15.0,
                "Statistic": "AVERAGE"
              }
            }
          },
          {
            "Name": "ScaleInOnLowYARNMemory",
            "Description": "Remove a core node when YARN memory is plentiful",
            "Action": {
              "SimpleScalingPolicyConfiguration": {
                "AdjustmentType": "CHANGE_IN_CAPACITY",
                "ScalingAdjustment": -1,
                "CoolDown": 300
              }
            },
            "Trigger": {
              "CloudWatchAlarmDefinition": {
                "ComparisonOperator": "GREATER_THAN",
                "EvaluationPeriods": 1,
                "MetricName": "YARNMemoryAvailablePercentage",
                "Namespace": "AWS/ElasticMapReduce",
                "Period": 300,
                "Threshold": 75.0,
                "Statistic": "AVERAGE"
              }
            }
          }
        ]
      }
    }
  ]' \
  --visible-to-all-users \
  --query 'ClusterId' \
  --output text)

echo "Cluster launching, ID: $CLUSTER_ID"
echo "Track status with:"
echo "  aws emr describe-cluster --cluster-id $CLUSTER_ID"
echo ""
echo "For the report/demo video, screenshot the auto-scaling policy in the console"
echo "and later screenshot a scale-out event actually happening under load —"
echo "that's the 'evidence scaling occurred' the marking scheme asks for."