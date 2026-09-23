from functools import reduce
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (SparkSession.builder
         .appName("Analysis of Customers and Transactions.")
         .getOrCreate())


def read_csv(path):
    return (spark.read
            .option("header", True)
            .option("ignoreLeadingWhiteSpace", True)
            .option("ignoreTrailingWhiteSpace", True)
            .csv(path))


def clean_keys(df, key_cols):
    """Normalise key columns, drop rows with blank keys, then dedupe."""
    valid = reduce(lambda a, b: a & b,
                   [F.col(c).isNotNull() & (F.col(c) != "") for c in key_cols])
    return (df
            .select(*[F.upper(F.trim(F.col(c))).alias(c) if c in key_cols else F.col(c)
                      for c in df.columns])
            .filter(valid)
            .dropDuplicates(key_cols))


def assert_unique(df, col):
    dupes = df.groupBy(col).count().filter(F.col("count") > 1)
    n = dupes.count()
    print(f"duplicate {col}s remaining: {n}")
    if n:
        dupes.show(10, truncate=False)


customers_df = read_csv("data/customers")
transactions_df = read_csv("data/transactions")

customers_clean = clean_keys(customers_df, ["customer_id"])
transactions_clean = clean_keys(transactions_df, ["transaction_id", "customer_id"])

assert_unique(customers_clean, "customer_id")
assert_unique(transactions_clean, "transaction_id")

print(f"customers:    {customers_df.count()} -> {customers_clean.count()}")
print(f"transactions: {transactions_df.count()} -> {transactions_clean.count()}")