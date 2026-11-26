"""Lab 08: explore Iceberg tables with Spark.

The lab runs on copies of the silver tables, in the namespace lab.explore, so that
lab.silver stays unchanged for the Polaris and Trino labs.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "07-spark-silver"))
from session import BUCKET, build_session, section  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

spark = build_session("lab-iceberg")
NS = "lab.explore"

section("Catalog")
spark.sql("SHOW NAMESPACES IN lab").show()
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
spark.table("lab.silver.orders").writeTo(f"{NS}.orders").createOrReplace()
spark.sql(f"SELECT count(*) AS orders, sum(quantity) AS quantity FROM {NS}.orders").show()

section("Metadata tables")
spark.sql(f"SELECT snapshot_id, parent_id, operation, committed_at, summary['added-records'] AS added "
          f"FROM {NS}.orders.snapshots").show(truncate=False)
spark.sql(f"SELECT file_path, file_format, record_count, file_size_in_bytes FROM {NS}.orders.files") \
    .show(truncate=False)

section("Second snapshot")
stg_orders = spark.table(f"{NS}.orders")
stg_orders.show(5)
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo(f"{NS}.orders").append()
spark.sql(f"SELECT snapshot_id, operation, committed_at FROM {NS}.orders.snapshots ORDER BY committed_at") \
    .show(truncate=False)
spark.sql(f"SELECT count(*) AS orders FROM {NS}.orders").show()
spark.sql(f"SELECT file_path, record_count FROM {NS}.orders.files").show(truncate=False)

section("Time travel")
snapshots = spark.sql(f"SELECT snapshot_id FROM {NS}.orders.snapshots ORDER BY committed_at").collect()
print([s["snapshot_id"] for s in snapshots])
first = snapshots[0]["snapshot_id"]
spark.sql(f"SELECT count(*) AS orders_first_snapshot FROM {NS}.orders VERSION AS OF {first}").show()
spark.sql(f"SELECT count(*) AS orders_now FROM {NS}.orders").show()

section("Schema evolution")
spark.sql(f"ALTER TABLE {NS}.orders ADD COLUMN source STRING")
spark.sql(f"DESCRIBE {NS}.orders").show()
spark.sql(f"""
    INSERT INTO {NS}.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM {NS}.orders LIMIT 1
""")
spark.sql(f"SELECT source, count(*) AS orders FROM {NS}.orders GROUP BY source").show()
spark.sql(f"SELECT file_path, record_count FROM {NS}.orders.files").show(truncate=False)
spark.sql(f"SELECT snapshot_id, operation FROM {NS}.orders.snapshots ORDER BY committed_at").show(truncate=False)

section("Exercise 1: users as an Iceberg table")
spark.table("lab.silver.users").writeTo(f"{NS}.users").createOrReplace()
spark.sql(f"SELECT count(*) AS users FROM {NS}.users").show()

section("Exercise 2: append a batch of users")
batch = spark.table(f"{NS}.users").limit(5).withColumn("user_id", F.expr("uuid()"))
batch.writeTo(f"{NS}.users").append()
spark.sql(f"SELECT snapshot_id, operation, summary['added-records'] AS added FROM {NS}.users.snapshots "
          "ORDER BY committed_at").show(truncate=False)

section("Exercise 3: time travel on users")
first_users = spark.sql(f"SELECT snapshot_id FROM {NS}.users.snapshots ORDER BY committed_at LIMIT 1") \
    .collect()[0]["snapshot_id"]
spark.sql(f"SELECT count(*) AS users_before FROM {NS}.users VERSION AS OF {first_users}").show()
spark.sql(f"SELECT count(*) AS users_after FROM {NS}.users").show()

section("Exercise 4: add a country column")
spark.sql(f"ALTER TABLE {NS}.users ADD COLUMN country STRING")
spark.sql(f"""
    INSERT INTO {NS}.users
    SELECT uuid(), username, name, sex, email, birthdate, street, city, state, zip_code, 'US'
    FROM {NS}.users LIMIT 2
""")
spark.sql(f"SELECT country, count(*) AS users FROM {NS}.users GROUP BY country").show()
spark.sql(f"SELECT file, latest_schema_id FROM {NS}.users.metadata_log_entries").show(truncate=False)
spark.stop()
