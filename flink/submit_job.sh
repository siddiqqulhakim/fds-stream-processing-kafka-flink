#!/bin/bash

# Wait for Flink to be ready
sleep 10

# Submit the fraud detection job
sql-client.sh -f /opt/flink/sql/fraud_job.sql

# Keep the script running to prevent job termination
tail -f /dev/null

