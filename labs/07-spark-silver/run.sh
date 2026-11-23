#!/bin/bash
# Lab 07 driver: run from the root of the project
set -u
cd /home/onyxia/work/ece-2026-bigdata
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
B="$LAB_BUCKET_NAME"
OUT=labs/07-spark-silver
PKG=org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
# Keep caches on the persistent volume, not on the node disk
export UV_CACHE_DIR=/home/onyxia/work/.cache/uv
IVY="--conf spark.jars.ivy=/home/onyxia/work/.ivy2"
# The node disk is nearly full: Spark temp files and the S3 profile live on the persistent volume
export SPARK_LOCAL_DIRS=/home/onyxia/work/.tmp TMPDIR=/home/onyxia/work/.tmp
export AWS_CONFIG_FILE=/home/onyxia/work/.aws/config AWS_SHARED_CREDENTIALS_FILE=/home/onyxia/work/.aws/credentials
s(){ echo; echo "===== $1 ====="; }

s "versions"
spark-submit --version 2>&1 | grep -E 'version|Scala'
ls "$SPARK_HOME/jars" | grep -i hadoop-aws

s "duckdb secret refresh"
duckdb -c "
  DROP PERSISTENT SECRET IF EXISTS s3_onyxia_connection;
  CREATE PERSISTENT SECRET s3_onyxia_connection (
    TYPE s3, PROVIDER credential_chain, ENDPOINT 's3.seaweedfs.adm.adaltas.cloud',
    REGION 'us-east-1', URL_STYLE 'path');"

s "dbt silver layer for the comparison"
if aws s3 --profile default ls "s3://$B/silver/dbt/stg_orders.parquet" >/dev/null 2>&1; then
  echo "dbt exports already present, skipped"
else
(cd lab_medallion && uv run dbt build -q 2>&1 | tail -3)
duckdb -readonly lab_medallion/lab_medallion.duckdb -c "
  COPY (SELECT * FROM main_silver.stg_orders) TO 's3://$B/silver/dbt/stg_orders.parquet' (FORMAT parquet);
  COPY (SELECT * FROM main_silver.stg_users) TO 's3://$B/silver/dbt/stg_users.parquet' (FORMAT parquet);"
fi

s "spark notebook"
spark-submit --master "local[2]" --driver-memory 1g $IVY --packages "$PKG" "$OUT/lab_silver.py" > "$OUT/output.txt" 2> "$OUT/stderr.log"
echo "spark exit code: $?"
grep -vE '^\s|^:: |^\s*found|downloading|^Ivy|resolution report|confs:|artifacts copied' "$OUT/output.txt" | tail -5

s "silver objects"
aws s3 --profile default ls "s3://$B/silver/" --recursive
s "warehouse objects"
aws s3 --profile default ls "s3://$B/warehouse/" --recursive

s "compare with dbt"
duckdb -c "
  SELECT 'dbt' AS engine, count(*) AS orders, sum(quantity) AS quantity
  FROM 's3://$B/silver/dbt/stg_orders.parquet'
  UNION ALL
  SELECT 'spark', count(*), sum(quantity) FROM 's3://$B/silver/spark/orders.parquet/*.parquet';
  SELECT 'dbt' AS engine, count(*) AS users FROM 's3://$B/silver/dbt/stg_users.parquet'
  UNION ALL
  SELECT 'spark', count(*) FROM 's3://$B/silver/spark/users.parquet/*.parquet';"
duckdb -c "DESCRIBE SELECT * FROM 's3://$B/silver/dbt/stg_orders.parquet'"
duckdb -c "DESCRIBE SELECT * FROM 's3://$B/silver/spark/orders.parquet/*.parquet'"
