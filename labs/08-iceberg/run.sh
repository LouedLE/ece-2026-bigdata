#!/bin/bash
# Lab 08 driver: run from anywhere
set -u
cd /home/onyxia/work/ece-2026-bigdata
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
B="$LAB_BUCKET_NAME"
OUT=labs/08-iceberg
export SPARK_LOCAL_DIRS=/home/onyxia/work/.tmp TMPDIR=/home/onyxia/work/.tmp
export AWS_CONFIG_FILE=/home/onyxia/work/.aws/config AWS_SHARED_CREDENTIALS_FILE=/home/onyxia/work/.aws/credentials
s(){ echo; echo "===== $1 ====="; }

s "warehouse before"
aws s3 --profile default ls "s3://$B/warehouse/" --recursive

s "spark"
spark-submit --master "local[2]" --driver-memory 1g --conf spark.jars.ivy=/home/onyxia/work/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  "$OUT/lab_iceberg.py" > "$OUT/output.txt" 2> "$OUT/stderr.log"
echo "spark exit code: $?"
tail -n 5 "$OUT/stderr.log" | grep -iE 'error|exception' || true

s "layout of the explore tables"
aws s3 --profile default ls "s3://$B/warehouse/explore/" --recursive > "$OUT/layout.txt"
cat "$OUT/layout.txt"

s "parquet directory of lab 07"
aws s3 --profile default ls "s3://$B/silver/spark/orders.parquet/" --recursive
