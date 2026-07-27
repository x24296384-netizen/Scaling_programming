\# Batch layer / EMR / benchmarking — progress notes



\## Done

\- Dataset downloaded from Kaggle (access.log, client\_hostname.csv)

\- Uploaded to S3: s3://scp-nalini-logs-2026/raw-data/

\- batch\_job.py updated to parse raw Nginx log format (regex-based, not CSV)

\- Cloud9 environment set up on Learner Lab for dev/testing



\## Pending

\- EC2 key pair creation

\- EMR cluster launch (emr\_setup.sh needs real subnet ID filled in)

\- First batch job run against real data

\- Benchmarking across worker counts (1/2/4+)

\- Auto-scaling trigger demonstration + screenshot



\## Notes for integration

\- Field names used: ip, timestamp, method, endpoint, protocol, status\_code, bytes\_sent, referrer, user\_agent

\- Please confirm these match your log parser's output before we build the serving layer join

