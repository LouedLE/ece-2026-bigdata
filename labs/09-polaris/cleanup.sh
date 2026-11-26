#!/bin/bash
# Lab 09.2 cleanup: drop the tables created through Polaris, keep silver.orders for the Trino lab.
set -u
cd /home/onyxia/work/ece-2026-bigdata
OUT=labs/09-polaris
export AWS_CONFIG_FILE=/home/onyxia/work/.aws/config AWS_SHARED_CREDENTIALS_FILE=/home/onyxia/work/.aws/credentials
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE" POLARIS_URI="http://polaris:8181" POLARIS_CATALOG="$KUBERNETES_NAMESPACE"
export SPARK_LOCAL_DIRS=/home/onyxia/work/.tmp TMPDIR=/home/onyxia/work/.tmp
set -a; . /home/onyxia/work/.polaris-credentials.env; set +a
spark-submit --master "local[2]" --driver-memory 1g --conf spark.jars.ivy=/home/onyxia/work/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  "$OUT/lab_polaris.py" cleanup > "$OUT/output_cleanup.txt" 2> "$OUT/stderr_cleanup.log"
echo "exit code: $?"
cat "$OUT/output_cleanup.txt"
