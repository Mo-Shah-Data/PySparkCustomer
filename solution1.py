from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.getOrCreate()

spark = (SparkSession
         .builder
         .appName("Analysis of Customers and Transactions.")
         .getOrCreate())

customers_df_path = "data/customers"
transactions_df_path = "data/transactions"

customers_df = (spark.read.option("header",True).option("ignoreLeadingWhiteSpace", True)
                .option("ignoreTrailingWhiteSpace", True).csv(customers_df_path))
transactions_df = (spark.read.option("header",True).option("ignoreLeadingWhiteSpace", True)
                   .option("ignoreTrailingWhiteSpace", True).csv(transactions_df_path))

customers_df.show()
transactions_df.show()
transactions_df.filter(F.col("customer_id").isNull()).count()

customers_df.createOrReplaceTempView("customers")
transactions_df.createOrReplaceTempView("transactions")

## Customer Table
# customers needs further cleaning
spark.sql("select * from customers order by customer_id").show(20)
# row counts for both tables
spark.sql("select count(*) AS customers from customers").show()
spark.sql("select count(*) AS transactions from transactions").show()


# ToDo - Use Summary functions
# count nulls in primary keys
spark.sql("SELECT count(customer_id) AS cust_id_null FROM customers WHERE customer_id IS NULL").show(100)
spark.sql("SELECT count(customer_id) AS cust_id_not_null FROM customers WHERE customer_id IS NOT NULL").show(100)

spark.sql("SELECT customer_id FROM customers WHERE customer_id IN (' ','NULL','CUST-0599901','N/A','NA','-','none') ").show(100)

# shows a mixture of cases and possibly leading and trailing spaces.
spark.sql("SELECT customer_id FROM customers ORDER BY customer_id DESC").show(100)

spark.sql("""
    CREATE OR REPLACE TEMP VIEW customers_clean AS
    SELECT *
    FROM (
        SELECT
            * EXCEPT (customer_id),
            nullif(lower(trim(customer_id)), '') AS customer_id
        FROM customers
    )
    WHERE customer_id IS NOT NULL
""")

spark.sql("SELECT * FROM customers_clean").show()
# drop old temp view - causing issues
# spark.catalog.dropTempView("customers")

spark.sql("SELECT count(customer_id) AS cust_id_null FROM customers_clean WHERE customer_id IS NULL").show(100)


# Transactions Table
# transactions table has a lot of nulls in customer_id col
spark.sql("""
    SELECT  count(*) - count(customer_id) AS NULL_cust_id,
            count(*) - count(transaction_id) AS NULL_trans_id
            FROM transactions WHERE customer_id IS NULL
            """).show(100)

spark.sql("select * from transactions where customer_id IS NOT NULL order by customer_id").show(100)

spark.sql("""
    CREATE OR REPLACE TEMP VIEW transactions_clean AS
    SELECT *
    FROM (
        SELECT
            * EXCEPT (customer_id, transaction_id),
            nullif(lower(trim(customer_id)), '')    AS customer_id,
            nullif(lower(trim(transaction_id)), '') AS transaction_id
        FROM transactions
    )
    WHERE transaction_id IS NOT NULL
      AND customer_id IS NOT NULL
""")

spark.sql("""
    SELECT count(*) AS rows,
           count(*) - count(customer_id)    AS null_customer_id,
           count(*) - count(transaction_id) AS null_transaction_id
    FROM transactions_clean
""").show()

# join
spark.sql("""
    CACHE TABLE all_customers_transactions
    OPTIONS ('storageLevel' = 'MEMORY_ONLY')
    AS SELECT * FROM customers_clean LEFT JOIN transactions_clean USING (customer_id)
""")

df = spark.table("all_customers_transactions")

# Standard Physical Plan (Default)
df.explain()

# ToDo - How do these work and what are the stages?
# The Full Journey (Parsed, Analyzed, Optimized, Physical)
df.explain(mode="extended")

# A Cleaner, Formatted View of the Physical Plan
df.explain(mode="formatted")

# Cleaning in spark sql
# customer table



# ToDo: Raise with Ali that teh cleaning of the key columns can also be done with a utils.py script that makes it simpler - don't use yet
# This avoids duplicate code and allows testing but is this better?
# SQL is below

# Todo - Create and  use window functions, and transactions min and max based on the day - get creative on advanced functions/ from previous weeks

# spark.catalog
#
spark.catalog.listTables()

spark.sql("SELECT * FROM all_customers_transactions").show(100)



# spark.sql("CREATE TABLE customers_tbl AS SELECT * FROM customers")
# spark.catalog.analyzeTable("customers_tbl")
#
# spark.sql("ANALYZE TABLE customers_tbl COMPUTE STATISTICS FOR COLUMNS customer_id")
# spark.sql("DESCRIBE EXTENDED customers_tbl customer_id").show(truncate=False)
#
# spark.catalog.dropTempView("customers")
#
# spark.catalog.listTables()

# Data Cleaning
# Lower case all text values
# Check date columns have dates
# Check all numeric columns have numeric values.
# Make sure to check numeric columns don't have outliers.