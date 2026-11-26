"""Lab 09.2: govern the Iceberg tables with Polaris.

Usage: spark-submit ... lab_polaris.py <stage>
Each stage runs in its own Spark session, like a kernel restart in the notebook:
  dev       register silver.orders, create silver.users, schema change (principal dev)
  analyst   read, then try to write (principal analyst)
  dev-check snapshots after the failed writes, exercise 1, gold namespace (principal dev)
  reader    exercise 2, principal restricted to the silver namespace
  cleanup   drop the tables created through Polaris (principal dev)
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "07-spark-silver"))
from session import BUCKET, build_session, section  # noqa: E402

STAGE = sys.argv[1]
PRINCIPAL = {"analyst": "analyst", "reader": "silver_analyst"}.get(STAGE, "dev")


def attempt(label, statement):
    """Run a statement that may be denied and print the outcome."""
    try:
        result = spark.sql(statement)
        result.show(truncate=False)
        print(f"{label}: allowed")
    except Exception as error:  # Py4J wraps the Iceberg exception
        lines = [line.strip() for line in str(error).splitlines() if line.strip()]
        reason = next((line for line in lines if "Forbidden" in line or "NotAuthorized" in line
                       or "not authorized" in line.lower()), lines[0] if lines else repr(error))
        print(f"{label}: denied -> {reason[:300]}")


spark = build_session(f"lab-polaris-{STAGE}", polaris_principal=PRINCIPAL)
print("Polaris principal:", PRINCIPAL)
print("fs.s3.impl:", spark.sparkContext.getConf().get("spark.hadoop.fs.s3.impl", None))

if STAGE == "dev":
    section("Catalogs")
    spark.sql("SHOW NAMESPACES IN lab").show()
    spark.sql("SHOW NAMESPACES IN polaris").show()

    section("Register silver.orders")
    metadata_file = os.environ["ORDERS_METADATA_FILE"]
    print("metadata file:", metadata_file)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    tables = [r["tableName"] for r in spark.sql("SHOW TABLES IN polaris.silver").collect()]
    if "orders" in tables:
        print("silver.orders is already registered")
    else:
        spark.sql(f"""
            CALL polaris.system.register_table(
              table => 'silver.orders',
              metadata_file => '{metadata_file}'
            )
        """).show(truncate=False)
    spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
    spark.sql("SELECT count(*) AS orders_lab FROM lab.silver.orders").show()

    section("Create silver.users as dev")
    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots") \
        .show(truncate=False)
    spark.table("lab.silver.users").writeTo("polaris.silver.users").createOrReplace()
    spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
    spark.sql("SELECT file FROM polaris.silver.users.metadata_log_entries").show(truncate=False)

    section("Schema change as dev")
    spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
    spark.sql("DESCRIBE polaris.silver.users").show()
    spark.sql("SELECT file, latest_schema_id FROM polaris.silver.users.metadata_log_entries") \
        .show(truncate=False)

elif STAGE == "analyst":
    section("Read as the analyst")
    attempt("SHOW TABLES IN silver", "SHOW TABLES IN polaris.silver")
    spark.sql("SELECT product, count(*) AS orders FROM polaris.silver.orders GROUP BY product ORDER BY product") \
        .show()
    section("Write as the analyst")
    attempt("INSERT INTO silver.orders",
            "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
    attempt("ALTER TABLE silver.users ADD COLUMN phone",
            "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")

elif STAGE == "dev-check":
    section("Snapshots of silver.orders after the analyst writes")
    spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots") \
        .show(truncate=False)
    spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()

    section("Exercise 1: drop the country column")
    spark.sql("SELECT snapshot_id, operation FROM polaris.silver.users.snapshots").show(truncate=False)
    spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
    spark.sql("DESCRIBE polaris.silver.users").show()
    spark.sql("SELECT snapshot_id, operation FROM polaris.silver.users.snapshots").show(truncate=False)
    spark.sql("SELECT file, latest_schema_id FROM polaris.silver.users.metadata_log_entries") \
        .show(truncate=False)

    section("Exercise 2: gold namespace as dev")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
    spark.sql("""
        CREATE OR REPLACE TABLE polaris.gold.orders_by_product AS
        SELECT product, count(*) AS orders, sum(quantity) AS quantity
        FROM polaris.silver.orders GROUP BY product
    """)
    spark.sql("SELECT * FROM polaris.gold.orders_by_product ORDER BY product").show()

elif STAGE == "reader":
    section("Exercise 2: principal silver_analyst (catalog role silver_reader)")
    attempt("SHOW TABLES IN silver", "SHOW TABLES IN polaris.silver")
    attempt("SELECT silver.orders", "SELECT count(*) AS orders FROM polaris.silver.orders")
    attempt("SELECT silver.users", "SELECT count(*) AS users FROM polaris.silver.users")
    attempt("SHOW TABLES IN gold", "SHOW TABLES IN polaris.gold")
    attempt("SELECT gold.orders_by_product", "SELECT * FROM polaris.gold.orders_by_product")
    attempt("INSERT INTO silver.orders",
            "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")

elif STAGE == "cleanup":
    section("Cleanup")
    spark.sql("DROP TABLE IF EXISTS polaris.silver.users")
    spark.sql("DROP TABLE IF EXISTS polaris.gold.orders_by_product")
    spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
    spark.sql("SHOW NAMESPACES IN polaris").show()
    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()

spark.stop()
