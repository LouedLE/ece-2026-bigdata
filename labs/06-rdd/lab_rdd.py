"""Lab 06: Spark RDD API on the bronze datasets (local copies)."""
import json
import re
import time
import urllib.request
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
UI = sc.uiWebUrl


def section(title):
    print(f"\n===== {title} =====")


def stages():
    """Stages of the application, read from the Spark UI REST API."""
    app = sc.applicationId
    with urllib.request.urlopen(f"{UI}/api/v1/applications/{app}/stages") as r:
        return json.load(r)


def shuffle_of_last_job():
    jobs_url = f"{UI}/api/v1/applications/{sc.applicationId}/jobs"
    with urllib.request.urlopen(jobs_url) as r:
        job = json.load(r)[0]
    ids = set(job["stageIds"])
    out = []
    for s in stages():
        if s["stageId"] in ids and s["status"] == "COMPLETE":
            out.append((s["stageId"], s["shuffleWriteBytes"], s["shuffleReadBytes"]))
    return sorted(out)


section("RDD creation and partitions")
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("users partitions:", users_lines.getNumPartitions())
print("orders partitions:", orders_lines.getNumPartitions())
print("defaultMinPartitions:", sc.defaultMinPartitions, "defaultParallelism:", sc.defaultParallelism)
orders_repartitioned = orders_lines.repartition(8)
print("after repartition(8):", orders_repartitioned.getNumPartitions())
print("after coalesce(1):", orders_repartitioned.coalesce(1).getNumPartitions())

section("Lazy evaluation and lineage")
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
n_stages = len(stages())
print("count 1:", orders_data.count(), "new stages:", len(stages()) - n_stages)
n_stages = len(stages())
print("count 2:", orders_data.count(), "new stages:", len(stages()) - n_stages)

section("The cost of no schema: the multiline address")
for line in users_lines.take(6):
    print(line)
print("users_lines.count():", users_lines.count())
UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("users_records.count():", users_records.count())


section("Manual parsing of orders")
def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


orders_rdd = orders_data.map(parse_order).cache()
for o in orders_rdd.take(3):
    print(o)
try:
    sc.parallelize(["a,b,2020-01-01,1,cookie,extra"]).map(parse_order).collect()
except Exception as exc:  # noqa: BLE001
    print("extra comma ->", type(exc).__name__, str(exc).splitlines()[-1][:120] if str(exc) else "")

section("Key-value RDDs: total quantity per user")
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print(user_quantity.take(5))
print("reduceByKey shuffle (stage, write, read):", shuffle_of_last_job())
user_quantity_grouped = (
    orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).groupByKey().mapValues(sum)
)
print(user_quantity_grouped.take(5))
print("groupByKey shuffle (stage, write, read):", shuffle_of_last_job())
assert dict(user_quantity.collect()) == dict(user_quantity_grouped.collect())


section("Manual deduplication")
def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


orders_dedup = orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()
print("orders_rdd:", orders_rdd.count(), "orders_dedup:", orders_dedup.count())
with_dup = orders_rdd.union(sc.parallelize(orders_rdd.take(3)))
print("with 3 duplicates:", with_dup.count(), "dedup:",
      with_dup.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).count())


section("Joining orders with users")
def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username


users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)
for e in enriched.take(3):
    print(e)
print("enriched:", enriched.count())
left = orders_kv.leftOuterJoin(users_kv)
print("leftOuterJoin:", left.count(), "without user:", left.filter(lambda kv: kv[1][1] is None).count())
p = 4
co_orders = orders_kv.partitionBy(p).cache()
co_users = users_kv.partitionBy(p).cache()
co_orders.count(), co_users.count()
print("co-partitioned join lineage (no extra shuffle stage):")
print(co_orders.join(co_users).toDebugString().decode())


section("Exercise 1: aggregateByKey")
count_and_sum = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(
    (0, 0),
    lambda acc, q: (acc[0] + 1, acc[1] + q),
    lambda a, b: (a[0] + b[0], a[1] + b[1]),
)
avg_quantity = count_and_sum.mapValues(lambda cs: round(cs[1] / cs[0], 2))
for user_id, stats in sorted(count_and_sum.collect())[:3]:
    print(user_id, stats)
print(sorted(avg_quantity.collect())[:3])
assert dict(count_and_sum.mapValues(lambda cs: cs[1]).collect()) == dict(user_quantity.collect())

section("Exercise 2: orphan order with leftOuterJoin")
fake = dict(orders_rdd.first(), order_id="00000000-0000-0000-0000-000000000000",
            user_id="ffffffff-ffff-ffff-ffff-ffffffffffff")
orders_with_fake = orders_rdd.union(sc.parallelize([fake]))
orphans = (
    orders_with_fake.map(lambda o: (o["user_id"], o))
    .leftOuterJoin(users_kv)
    .filter(lambda kv: kv[1][1] is None)
)
print("orphan orders:", orphans.count())
print(orphans.map(lambda kv: kv[1][0]["order_id"]).collect())

section("Exercise 3: DataFrame version")
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
user_quantity_df = orders_df.groupBy(F.col("user_uuid").alias("user_id")).sum("quantity")
user_quantity_df.show(5, truncate=False)
user_quantity_df.explain()
print(user_quantity.toDebugString().decode())
assert {r[0]: r[1] for r in user_quantity_df.collect()} == dict(user_quantity.collect())


section("Exercise 4: cache vs no cache")
def timed(label, rdd_count):
    t0 = time.perf_counter()
    n = rdd_count()
    print(f"{label}: {n} rows in {time.perf_counter() - t0:.3f}s")


def storage():
    with urllib.request.urlopen(f"{UI}/api/v1/applications/{sc.applicationId}/storage/rdd") as r:
        return [(s["name"] or s["id"], s["numCachedPartitions"], s["memoryUsed"]) for s in json.load(r)]


timed("cached orders_rdd.count", orders_rdd.count)
timed("cached user_quantity.count", user_quantity.count)
print("storage tab (cached):", storage())
orders_rdd.unpersist(blocking=True)
co_orders.unpersist(blocking=True)
co_users.unpersist(blocking=True)
orders_nocache = orders_data.map(parse_order)
uq_nocache = orders_nocache.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
timed("uncached orders.count", orders_nocache.count)
timed("uncached user_quantity.count", uq_nocache.count)
print("storage tab (uncached):", storage())

spark.stop()
