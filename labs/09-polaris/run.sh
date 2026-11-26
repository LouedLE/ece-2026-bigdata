#!/bin/bash
# Lab 09.2 driver: run from anywhere, after deploy.sh.
set -u
cd /home/onyxia/work/ece-2026-bigdata
OUT=labs/09-polaris
export AWS_CONFIG_FILE=/home/onyxia/work/.aws/config AWS_SHARED_CREDENTIALS_FILE=/home/onyxia/work/.aws/credentials
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE" POLARIS_URI="http://polaris:8181" AWS_REGION=us-east-1
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export SPARK_LOCAL_DIRS=/home/onyxia/work/.tmp TMPDIR=/home/onyxia/work/.tmp
set -a; . /home/onyxia/work/.polaris-credentials.env; set +a
B="$LAB_BUCKET_NAME"
s(){ echo; echo "===== $1 ====="; }
ls_meta(){ aws s3 --profile default ls "s3://$B/warehouse/silver/$1/" --recursive | awk '{print $4}'; }
stage(){
  s "spark stage $1"
  spark-submit --master "local[2]" --driver-memory 1g --conf spark.jars.ivy=/home/onyxia/work/.ivy2 \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
    "$OUT/lab_polaris.py" "$1" > "$OUT/output_$1.txt" 2> "$OUT/stderr_$1.log"
  echo "exit code: $?"
  cat "$OUT/output_$1.txt"
}

s "current metadata file of silver.orders"
V=$(aws s3 --profile default cp "s3://$B/warehouse/silver/orders/metadata/version-hint.text" -)
echo "version-hint: $V"
export ORDERS_METADATA_FILE="s3a://$B/warehouse/silver/orders/metadata/v$V.metadata.json"
s "objects under warehouse/silver/orders before registration"
ls_meta orders | tee "$OUT/orders_before.txt" | wc -l

stage dev

s "objects under warehouse/silver/orders after registration"
ls_meta orders | tee "$OUT/orders_after.txt" | wc -l
diff "$OUT/orders_before.txt" "$OUT/orders_after.txt" && echo "no new object"
s "layout of warehouse/silver"
aws s3 --profile default ls "s3://$B/warehouse/silver/" --recursive | awk '{print $4}'

stage analyst
stage dev-check

s "exercise 2: RBAC"
bash "$OUT/rbac_silver_reader.sh"
set -a; . /home/onyxia/work/.polaris-credentials.env; set +a
stage reader

s "metadata sync"
aws s3 --profile default sync "s3://$B/warehouse/" ./warehouse/ --only-show-errors
git status --short warehouse
