"""Lab 07: bronze to silver with Spark DataFrames, Parquet and Iceberg."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from session import BUCKET, build_session, section  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

spark = build_session("lab-silver-pyspark")
print(spark)

section("Bronze layer")
users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
users_bronze.printSchema()
users_bronze.show(3, truncate=False)
users_bronze.select("address").show(3, truncate=False)

section("Silver users, DataFrame API")
users_typed = (
    users_bronze.select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)
stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)
stg_users = stg_users.withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))
stg_users.show(5, truncate=False)

section("Silver orders, window deduplication")
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())
duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show()
dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates.withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()

section("Spark SQL")
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql("""
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
""")
stg_orders_sql.show(5)
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("DataFrame and SQL deduplication agree")
stg_users.createOrReplaceTempView("stg_users")
spark.sql("""
    SELECT state, count(*) AS users, sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users GROUP BY state ORDER BY users DESC
""").show(10)
print("empty strings left in state:", stg_users.filter(F.col("state") == "").count(),
      "zip_code:", stg_users.filter(F.col("zip_code") == "").count())

section("Write the silver layer as Parquet")
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("output partitions users / orders:", stg_users.rdd.getNumPartitions(), stg_orders.rdd.getNumPartitions())
print("Parquet round trip OK:", users_silver.count(), orders_silver.count())

section("Write the silver layer as Iceberg tables")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("""
    SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files
""").show(truncate=False)
users_iceberg = spark.table("lab.silver.users")
orders_iceberg = spark.table("lab.silver.orders")
assert users_iceberg.count() == users_bronze.count()
assert orders_iceberg.count() == stg_orders.count()
print("Iceberg tables OK:", users_iceberg.count(), orders_iceberg.count())
spark.stop()
