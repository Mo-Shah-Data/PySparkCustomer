"""
CSV -> inspect -> temp views -> SQL for everything else.

Python does four things only: read, printSchema, summary, drop duplicate rows.
After stage 4 the work is all SQL.
"""

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("Customers and transactions analysis")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")


# ===========================================================================
# STAGE 1 — PYTHON: READ
# ===========================================================================

customers = (spark.read
             .option("header", True)
             .option("ignoreLeadingWhiteSpace", True)
             .option("ignoreTrailingWhiteSpace", True)
             .csv("data/customers"))

transactions = (spark.read
                .option("header", True)
                .option("ignoreLeadingWhiteSpace", True)
                .option("ignoreTrailingWhiteSpace", True)
                .csv("data/transactions"))


# ===========================================================================
# STAGE 2 — PYTHON: INSPECT
#
# printSchema is free — it reads metadata, runs no job.
# Expect every column to be `string`: that is what CSV gives you, and it is
# why the SQL below has to cast amount, quantity and the dates.
#
# summary() IS a job, and on raw strings it is of limited use: mean and
# stddev come back null, and min/max sort as text ("9.99" beats "10000.00").
# Run it again after casting in stage 7 to get numbers that mean something.
# ===========================================================================

print("\n=== customers schema ===")
customers.printSchema()

print("\n=== transactions schema ===")
transactions.printSchema()

print("\n=== customers summary (raw) ===")
customers.summary("count", "min", "max").show(truncate=False)

print("\n=== transactions summary (raw) ===")
transactions.summary("count", "min", "max").show(truncate=False)


# ===========================================================================
# STAGE 3 — PYTHON: DROP EXACT DUPLICATE ROWS
#
# No column list = compare whole rows, so this removes only rows that are
# byte-for-byte identical. That is always safe and does not depend on
# cleaning having happened yet.
#
# Deduping on the KEY is deliberately NOT done here. " A1 " and "a1" are
# different strings, so a key-level dedupe now would keep both, and they
# would collapse into duplicates once SQL lowercases them — which is what
# inflates the join. The key dedupe happens in stages 5 and 6, AFTER
# normalisation.
# ===========================================================================

customers = customers.dropDuplicates()
transactions = transactions.dropDuplicates()


# ===========================================================================
# STAGE 4 — PYTHON: REGISTER THE VIEWS
#
# This is the handover. From here down, every statement is SQL.
# A view is a saved QUERY, not a copy of the data, so these two names must
# stay registered for the whole script — do not drop them.
# ===========================================================================

customers.createOrReplaceTempView("customers_raw")
transactions.createOrReplaceTempView("transactions_raw")

spark.sql("SHOW TABLES").show(truncate=False)


# ===========================================================================
# STAGE 5 — SQL: CLEAN CUSTOMERS
#
# Three things happen here, and the order is fixed:
#   1. inner query  — normalise the key and cast the date
#   2. WHERE        — drop rows with no usable key
#   3. QUALIFY      — one row per customer, keeping the newest registration
#
# nullif(x, '') turns an empty string into a real null, so a value that was
# only whitespace is caught by the IS NOT NULL test rather than surviving
# as "".
# ===========================================================================

spark.sql("""
    CREATE OR REPLACE TEMP VIEW customers_clean AS
    SELECT customer_id,
           customer_name,
           city,
           state,
           registration_date
    FROM (
        SELECT nullif(lower(trim(customer_id)), '') AS customer_id,
               customer_name,
               city,
               state,
               cast(registration_date AS date)      AS registration_date
        FROM customers_raw
    )
    WHERE customer_id IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY customer_id
        ORDER BY registration_date DESC
    ) = 1
""")


# ===========================================================================
# STAGE 6 — SQL: CLEAN TRANSACTIONS
#
# Same shape. Note customer_id is normalised with exactly the same
# expression as in stage 5 — if the two sides differ, the join quietly
# matches fewer rows instead of raising an error.
#
# The dedupe is on transaction_id alone, because that is the primary key.
# Partitioning by both ids would dedupe on the PAIR and keep two rows that
# share a transaction_id.
# ===========================================================================

spark.sql("""
    CREATE OR REPLACE TEMP VIEW transactions_clean AS
    SELECT transaction_id,
           customer_id,
           product,
           transaction_date,
           quantity,
           amount
    FROM (
        SELECT nullif(lower(trim(transaction_id)), '') AS transaction_id,
               nullif(lower(trim(customer_id)), '')    AS customer_id,
               product,
               cast(transaction_date AS date)          AS transaction_date,
               cast(quantity AS int)                   AS quantity,
               cast(amount AS double)                  AS amount
        FROM transactions_raw
    )
    WHERE transaction_id IS NOT NULL
      AND customer_id IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY transaction_id
        ORDER BY transaction_date DESC
    ) = 1
""")


# ===========================================================================
# STAGE 7 — SQL: CHECK THE CLEANING WORKED
#
# Row count must equal distinct key count. If they differ, the dedupe
# failed and the join will fan out.
# ===========================================================================

print("\n=== customers: rows vs distinct keys ===")
spark.sql("""
    SELECT count(*)                   AS rows,
           count(DISTINCT customer_id) AS distinct_ids
    FROM customers_clean
""").show()

print("\n=== transactions: rows vs distinct keys ===")
spark.sql("""
    SELECT count(*)                       AS rows,
           count(DISTINCT transaction_id) AS distinct_ids,
           count(DISTINCT customer_id)    AS distinct_customers
    FROM transactions_clean
""").show()

print("\n=== how many rows did cleaning remove ===")
spark.sql("""
    SELECT (SELECT count(*) FROM customers_raw)       AS customers_before,
           (SELECT count(*) FROM customers_clean)     AS customers_after,
           (SELECT count(*) FROM transactions_raw)    AS transactions_before,
           (SELECT count(*) FROM transactions_clean)  AS transactions_after
""").show()

# now that amount and quantity are real numbers, summary says something
print("\n=== transactions summary (cleaned) ===")
spark.table("transactions_clean").select("amount", "quantity").summary().show()


# ===========================================================================
# STAGE 8 — SQL: JOIN
#
# USING (customer_id) collapses the two key columns into one, so the result
# has no ambiguous column name.
# ===========================================================================

spark.sql("""
    CREATE OR REPLACE TEMP VIEW sales AS
    SELECT *
    FROM transactions_clean
    JOIN customers_clean USING (customer_id)
""")


# ===========================================================================
# STAGE 9 — SQL: CACHE THE JOIN
#
# Every query below reads `sales`. Without this, each one re-reads both
# CSVs, re-cleans, re-shuffles and re-sorts.
#
# CACHE TABLE is eager: this line does the work rather than deferring it.
# No storageLevel given, so it uses the default MEMORY_AND_DISK, which
# spills instead of recomputing when memory runs short.
# ===========================================================================

spark.sql("CACHE TABLE sales")

print("\n=== did the join fan out? ===")
spark.sql("""
    SELECT (SELECT count(*) FROM transactions_clean) AS transactions_in,
           (SELECT count(*) FROM sales)              AS rows_out
""").show()


# ===========================================================================
# STAGE 10 — SQL: ANALYSIS
# ===========================================================================

print("\n=== spend per customer ===")
spark.sql("""
    SELECT customer_id,
           customer_name,
           state,
           count(*)              AS txn_count,
           round(sum(amount), 2) AS total_spend,
           round(avg(amount), 2) AS avg_spend,
           min(transaction_date) AS first_purchase,
           max(transaction_date) AS last_purchase
    FROM sales
    GROUP BY customer_id, customer_name, state
    ORDER BY total_spend DESC
""").show(10, truncate=False)


print("\n=== top 10 customers in each state ===")
spark.sql("""
    SELECT state,
           customer_id,
           customer_name,
           total_spend,
           rank() OVER (PARTITION BY state ORDER BY total_spend DESC) AS rnk
    FROM (
        SELECT state,
               customer_id,
               customer_name,
               round(sum(amount), 2) AS total_spend
        FROM sales
        GROUP BY state, customer_id, customer_name
    )
    QUALIFY rank() OVER (PARTITION BY state ORDER BY total_spend DESC) <= 10
    ORDER BY state, rnk
""").show(20, truncate=False)


print("\n=== monthly totals ===")
spark.sql("""
    SELECT date_trunc('month', transaction_date) AS month,
           count(*)                              AS txn_count,
           round(sum(amount), 2)                 AS total_spend,
           round(avg(amount), 2)                 AS avg_basket
    FROM sales
    GROUP BY date_trunc('month', transaction_date)
    ORDER BY month
""").show(24, truncate=False)


print("\n=== running total per customer over time ===")
spark.sql("""
    SELECT customer_id,
           transaction_date,
           amount,
           row_number() OVER w                    AS txn_number,
           round(sum(amount) OVER w_running, 2)   AS running_total,
           lag(transaction_date) OVER w           AS prev_purchase,
           datediff(transaction_date,
                    lag(transaction_date) OVER w) AS days_since_last
    FROM sales
    WINDOW
        w         AS (PARTITION BY customer_id ORDER BY transaction_date),
        w_running AS (PARTITION BY customer_id ORDER BY transaction_date
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
""").show(20, truncate=False)


print("\n=== best selling products ===")
spark.sql("""
    SELECT product,
           count(*)                   AS txn_count,
           sum(quantity)              AS units,
           round(sum(amount), 2)      AS revenue
    FROM sales
    GROUP BY product
    ORDER BY revenue DESC
""").show(20, truncate=False)


# ===========================================================================
# STAGE 11 — WRITE RESULTS
#
# spark.sql() returns a DataFrame, so .write works on it directly.
# ===========================================================================

spark.sql("""
    SELECT customer_id, customer_name, state,
           count(*)              AS txn_count,
           round(sum(amount), 2) AS total_spend
    FROM sales
    GROUP BY customer_id, customer_name, state
""").write.mode("overwrite").parquet("output/customer_summary")

spark.sql("""
    SELECT date_trunc('month', transaction_date) AS month,
           count(*)                              AS txn_count,
           round(sum(amount), 2)                 AS total_spend
    FROM sales
    GROUP BY date_trunc('month', transaction_date)
""").write.mode("overwrite").parquet("output/monthly_totals")


# ===========================================================================
# STAGE 12 — TIDY UP
# ===========================================================================

spark.sql("UNCACHE TABLE sales")
spark.stop()
