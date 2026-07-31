# Generated Results

This directory contains machine-generated outputs produced by the
batch, speed, integration and serving layers.

## speed/benchmarks

Raw JSON results from local streaming, Kinesis and deployed
Kinesis-Lambda-S3 benchmarks.

## batch

Final CSV, JSON and data-quality outputs from the PySpark and EMR
batch layer.

## integration

Controlled outputs used to validate schema and metric consistency
between the batch and speed layers.

## serving

Combined serving-layer outputs generated from the available batch
and speed results.

Report-ready charts, logs and screenshots are stored separately
under docs/evidence/.
