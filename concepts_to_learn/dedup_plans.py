"""
Sample script: dropDuplicates and reading query plans.

Run with:
    spark-submit dedup_plans.py
or paste into the `pyspark` shell.

While it waits at the end, open http://localhost:4040 -> SQL / DataFrame tab.
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("dropDuplicates-plan-demo")
    # Small number of shuffle partitions so the demo is fast and plans are easy to read
    .config("spark.sql.shuffle.partitions", "4")
    # Uncomment to turn off Adaptive Query Execution and see the "raw" plan
    # .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

# --- Sample data with duplicates -------------------------------------------
data = [
    (1, "Alice", "London"),
    (1, "Alice", "London"),    # exact duplicate
    (2, "Bob",   "Leeds"),
    (2, "Bob",   "Bristol"),   # same id, different city
    (3, "Cara",  "York"),
    (3, "Cara",  "York"),      # exact duplicate
    (4, "Dan",   "Bath"),
]
df = spark.createDataFrame(data, ["id", "name", "city"])

# --- 1. Deduplicate on ALL columns -----------------------------------------
full_dedup = df.dropDuplicates()

print("\n===== 1. dropDuplicates() on all columns =====")
full_dedup.explain("formatted")
full_dedup.show()            # action -> shows up in Spark UI

# --- 2. Deduplicate on a SUBSET of columns ---------------------------------
subset_dedup = df.dropDuplicates(["id"])

print("\n===== 2. dropDuplicates(['id']) =====")
subset_dedup.explain(True)   # extended: parsed, analyzed, optimized, physical
subset_dedup.show()

# --- 3. Compare with distinct() --------------------------------------------
print("\n===== 3. distinct() for comparison =====")
df.distinct().explain()      # should match plan 1

# --- Keep the app alive so you can browse the Spark UI ---------------------
input("\nOpen http://localhost:4040 (SQL / DataFrame tab). Press Enter to exit...")
spark.stop()