"""Lab 07 exercises, run after the main notebook and the metadata commit."""
import sys
import timeit
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from session import BUCKET, build_session, section  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StringType  # noqa: E402

spark = build_session("lab-silver-exercises")
users_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True, multiLine=True)
stg_users = spark.table("lab.silver.users")
stg_orders = spark.table("lab.silver.orders")

section("Exercise 1: birthdate_is_valid, DataFrame API")
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))
users_valid_df = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn(
        "birthdate_is_valid",
        F.when(F.col("birthdate").isNull(), F.lit(False))
        .when(F.col("first_order_at").isNull(), F.lit(True))
        .otherwise(F.col("birthdate") <= F.to_date("first_order_at")),
    )
    .drop("first_order_at")
)
users_valid_df.groupBy("birthdate_is_valid").count().orderBy("birthdate_is_valid").show()

section("Exercise 1: birthdate_is_valid, Spark SQL")
stg_users.createOrReplaceTempView("stg_users")
stg_orders.createOrReplaceTempView("stg_orders")
users_valid_sql = spark.sql("""
    WITH first_orders AS (
        SELECT user_id, min(ordered_at) AS first_order_at FROM stg_orders GROUP BY user_id
    )
    SELECT u.*,
           CASE WHEN u.birthdate IS NULL THEN false
                WHEN f.first_order_at IS NULL THEN true
                ELSE u.birthdate <= to_date(f.first_order_at) END AS birthdate_is_valid
    FROM stg_users u LEFT JOIN first_orders f ON u.user_id = f.user_id
""")
diff = users_valid_df.select("user_id", "birthdate_is_valid").exceptAll(
    users_valid_sql.select("user_id", "birthdate_is_valid"))
print("rows that differ between DataFrame and SQL:", diff.count())
print("invalid birthdates:", users_valid_sql.filter("NOT birthdate_is_valid").count())

section("Exercise 2: zip_code with regexp_extract, Python UDF and pandas_udf")
ZIP = r"([A-Z]{2}) ([0-9]{5})$"
second_line = F.element_at(F.split("address", "\n"), 2)


@F.udf(StringType())
def zip_udf(address):
    import re
    m = re.search(r"([A-Z]{2}) ([0-9]{5})$", address.split("\n")[-1]) if address else None
    return m.group(2) if m else ""


variants = {
    "regexp_extract": lambda: users_bronze.select(F.regexp_extract(second_line, ZIP, 2).alias("zip")),
    "python udf": lambda: users_bronze.select(zip_udf("address").alias("zip")),
}
try:
    import pandas as pd
    from pyspark.sql.functions import pandas_udf

    @pandas_udf(StringType())
    def zip_pandas_udf(address: pd.Series) -> pd.Series:
        return address.str.split("\n").str[-1].str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1].fillna("")

    variants["pandas_udf"] = lambda: users_bronze.select(zip_pandas_udf("address").alias("zip"))
except ImportError as exc:
    print("pandas_udf unavailable:", exc)

reference = sorted(r.zip for r in variants["regexp_extract"]().collect())
for name, build in variants.items():
    assert sorted(r.zip for r in build().collect()) == reference, name
    seconds = min(timeit.repeat(lambda: build().collect(), number=3, repeat=3)) / 3
    print(f"{name:15s} {seconds * 1000:8.1f} ms per run")

section("Exercise 3: cache the bronze DataFrames")
users_bronze.cache()
orders_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True).cache()
for label in ("first action (reads S3, fills the cache)", "second action (served from memory)"):
    t = timeit.timeit(lambda: (users_bronze.count(), orders_bronze.groupBy("product").count().collect()), number=1)
    print(f"{label}: {t * 1000:.0f} ms")
print("storage level:", orders_bronze.storageLevel)
orders_bronze.groupBy("product").count().explain()

section("Exercise 4: append and time travel")
before = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at").collect()
new_order = stg_orders.limit(1).withColumn("order_id", F.expr("uuid()"))
new_order.writeTo("lab.silver.orders").append()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at") \
    .show(truncate=False)
first = before[0]["snapshot_id"]
spark.sql(f"SELECT count(*) AS orders_before FROM lab.silver.orders VERSION AS OF {first}").show()
spark.sql("SELECT count(*) AS orders_now FROM lab.silver.orders").show()
parquet_path = f"s3a://{BUCKET}/silver/spark/orders.parquet"
new_order.write.mode("append").parquet(parquet_path)
print("parquet directory now:", spark.read.parquet(parquet_path).count(), "rows, no older version to read")
spark.stop()
