"""Spark session with S3A and the Iceberg catalogs, shared by labs 07, 08 and 09.

Onyxia sets the AWS_* variables when the service starts, and they expire. The credentials
are read from the `default` profile instead, refreshed from the Onyxia portal.
"""
import json
import os
import subprocess

from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
BUCKET = os.environ.get("LAB_BUCKET_NAME") or os.environ["KUBERNETES_NAMESPACE"]


def profile_credentials():
    out = subprocess.run(
        ["aws", "configure", "export-credentials", "--profile", "default", "--format", "process"],
        check=True, capture_output=True, text=True,
    ).stdout
    creds = json.loads(out)
    return creds["AccessKeyId"], creds["SecretAccessKey"], creds.get("SessionToken", "")


def build_session(app_name, polaris_principal=None):
    access_key, secret_key, session_token = profile_credentials()
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.session.token", session_token)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hadoop")
        .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    )
    if polaris_principal:
        credential = os.environ[f"POLARIS_{polaris_principal.upper()}_CREDENTIAL"]
        builder = (
            builder.config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.polaris.type", "rest")
            .config("spark.sql.catalog.polaris.uri", f"{os.environ['POLARIS_URI']}/api/catalog")
            .config("spark.sql.catalog.polaris.warehouse", os.environ["POLARIS_CATALOG"])
            .config("spark.sql.catalog.polaris.credential", credential)
            .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
            .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def section(title):
    print(f"\n===== {title} =====", flush=True)
