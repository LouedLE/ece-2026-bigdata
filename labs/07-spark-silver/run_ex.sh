#!/bin/bash
# Lab 07 exercises: run from anywhere, after run.sh and commit.sh
set -u
# The node disk is nearly full: Spark temp files and the S3 profile live on the persistent volume
export SPARK_LOCAL_DIRS=/home/onyxia/work/.tmp TMPDIR=/home/onyxia/work/.tmp
export AWS_CONFIG_FILE=/home/onyxia/work/.aws/config AWS_SHARED_CREDENTIALS_FILE=/home/onyxia/work/.aws/credentials
export SPARK_LOCAL_DIRS=/home/onyxia/work/.tmp TMPDIR=/home/onyxia/work/.tmp
cd /home/onyxia/work/ece-2026-bigdata
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
uv pip list 2>/dev/null | grep -i -E '^(pandas|pyarrow) ' || true
spark-submit --master "local[2]" --driver-memory 1g --conf spark.jars.ivy=/home/onyxia/work/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  labs/07-spark-silver/exercises.py > labs/07-spark-silver/exercises_output.txt 2> labs/07-spark-silver/exercises_stderr.log
echo "spark exit code: $?"
