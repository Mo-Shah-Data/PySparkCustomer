from os import truncate

from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.getOrCreate()

spark = (SparkSession
         .builder
         .appName("Analysis of Customers and Transactions.")
         .getOrCreate())

# setting error level as we dont want to see too many unimportant errors
spark.sparkContext.setLogLevel("ERROR")

# Step 1 - read in all raw data from CSVs and show result
customers_df_path = "data/customers"
transactions_df_path = "data/transactions"

customers_df = (spark.read.option("header",True).option("ignoreLeadingWhiteSpace", True)
                .option("ignoreTrailingWhiteSpace", True).csv(customers_df_path))
transactions_df = (spark.read.option("header",True).option("ignoreLeadingWhiteSpace", True)
                   .option("ignoreTrailingWhiteSpace", True).csv(transactions_df_path))

print("\n=== customers schema ===")
customers_df.printSchema()

print("\n=== transactions schema ===")
transactions_df.printSchema()

# Stage 2 - get into tempview and apply sql to clean data
## table summaries - shows counts of rows with mean and other output values.

print("\n=== customers summary ===")
customers_df.summary().show()

print("\n=== transactions summary ===")
transactions_df.summary().show()

## dropDuplicates - safe to run now as it is looking for completely identical whole rows
customers = customers_df.dropDuplicates()
transactions = transactions_df.dropDuplicates()

print("\n=== customers summary post duplicate removal ===")
customers_df.summary().show()

print("\n=== transactions summary post duplicate removal ===")
transactions_df.summary().show()

## create temp views for dataframes
customers_df.createOrReplaceTempView("customers")
transactions_df.createOrReplaceTempView("transactions")

## truncate and show table names
### truncate true can applies a 20 char max limit and false ignores any limits and you can also enter a fixed number.
### True also right aligns and False left aligns the resultset
### vertical changes the view of the output to vertical and you can also set n number of outputs.
### Instead of "SHOW TABLES" other options such as "DESCRIBE TABLE" also exist
spark.sql("SHOW TABLES").show(truncate=True, vertical=True)

## Clean customers temp view

spark.sql("""
    CREATE OR REPLACE TEMP VIEW customers_clean AS
    SELECT customer_id,
           customer_name,
           city,
           state,
           registration_date
    FROM (
        SELECT nullif(trim(customer_id), '') AS customer_id,

               nullif(initcap(regexp_replace(trim(customer_name), '\\s+', ' ')), '')
                   AS customer_name,

               nullif(lower(regexp_replace(trim(city), '\\s+', ' ')), '')
                   AS city,

               nullif(lower(regexp_replace(trim(state), '\\s+', ' ')), '')
                   AS state,

               cast(registration_date AS date) AS registration_date
        FROM customers
    )
    WHERE customer_id IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY customer_id
        ORDER BY registration_date DESC
    ) = 1
""")
## show/confirm creation of this new temp view.
spark.sql("SHOW TABLES").show(truncate=True, vertical=True)

print("\n=== customers_clean summary after cleaning ===")

## run the sql below and you can see it has removed all nulls from name and registration date and all duplicates removed
spark.sql("""
    SELECT count(*)                                  AS total_rows,

           count(customer_id)                        AS customer_id_non_null,
           count(DISTINCT customer_id)               AS customer_id_distinct,

           count(customer_name)                      AS name_non_null,
           count(*) - count(customer_name)           AS name_nulls,

           count(city)                               AS city_non_null,
           count(DISTINCT city)                      AS city_distinct,
           count(*) - count(city)                    AS city_nulls,

           count(state)                              AS state_non_null,
           count(DISTINCT state)                     AS state_distinct,
           count(*) - count(state)                   AS state_nulls,

           min(registration_date)                    AS earliest_registration,
           max(registration_date)                    AS latest_registration,
           count(*) - count(registration_date)       AS registration_date_nulls
    FROM customers_clean
""").show(vertical=True)

# for the above we can alternatively see summary output after converting to spark dataframe, but it is not as good
spark.table("customers_clean").summary().show(truncate=False)

## show random 10 entries
spark.sql("""
    SELECT *
    FROM customers_clean
    ORDER BY rand()
    LIMIT 10
""").show(truncate=False)

## Analysis of customers
## states with counts

spark.sql("""
    SELECT state, count(*) 
    FROM customers_clean
    GROUP BY state
    ORDER BY count(*) 
""").show(truncate=False)

## earliest registration date by state
spark.sql("""
    SELECT customer_id, customer_name, state, registration_date,
           row_number() OVER (
               PARTITION BY state
               ORDER BY registration_date ASC NULLS LAST, customer_id
           ) AS signup_order
    FROM customers_clean
    QUALIFY signup_order = 1
    ORDER BY state
""").show(truncate=False)

# latest registration date by state
spark.sql("""
    SELECT customer_id, customer_name, state, registration_date,
           row_number() OVER (
               PARTITION BY state
               ORDER BY registration_date DESC, customer_id
           ) AS signup_order
    FROM customers_clean
    QUALIFY signup_order = 1
    ORDER BY state
""").show(51, truncate=False)

## This time for dropping duplicates in transactions we will use the spark sql instead of dropDuplicates

# Cleaning of transactions
spark.sql("""
    SELECT *
    FROM transactions
    LIMIT 10
""").show(truncate=True)

## drop duplicates using distinct sql statement instead of dropDuplicates on dataframe
spark.sql("SELECT DISTINCT * FROM transactions").createOrReplaceTempView("transactions_duplicates_removed")
#confirm new temp view creation
spark.sql("SHOW TABLES").show(truncate=True, vertical=True)

# below summaries show the difference after dropping complete identical rows
spark.table("transactions_duplicates_removed").summary().show(truncate=False)
spark.table("transactions").summary().show(truncate=False)

# this is just to show how to remove duplicates based on a Single column
(spark.table("transactions_duplicates_removed")
 .dropDuplicates(["product"]).createOrReplaceTempView("transactions_dedup_single_col"))

(spark.table("transactions_duplicates_removed")
 .dropDuplicates(["product", "customer_id"])
 .createOrReplaceTempView("transactions_dedup_multi_col"))

# keeping the latest by transaction date with window function

spark.sql("""
    CREATE OR REPLACE TEMP VIEW transactions_latest AS
    SELECT *
    FROM transactions_duplicates_removed
    QUALIFY row_number() OVER (
        PARTITION BY product, customer_id
        ORDER BY transaction_date DESC, transaction_id DESC
    ) = 1
""")

spark.sql("""
    SELECT customer_id, count(*) AS entries
    FROM transactions_duplicates_removed
    GROUP BY customer_id
    HAVING count(*) < 10
    ORDER BY entries DESC
""").show(100, truncate=False)

spark.sql("""
    SELECT *
    FROM transactions_dedup_single_col
    ORDER BY product
    LIMIT 100
""").show(truncate=True)

# now to clean the transactions table like customers was done

spark.sql("""
    SELECT *
    FROM transactions_duplicates_removed
    LIMIT 10
""").show(truncate=True)

spark.sql("""
    CREATE OR REPLACE TEMP VIEW transactions_clean AS
    SELECT transaction_id,
           customer_id,
           product,
           transaction_date,
           quantity,
           amount
    FROM (
        SELECT nullif(upper(trim(transaction_id)), '')          AS transaction_id,

               nullif(trim(customer_id), '')                    AS customer_id,

               nullif(lower(regexp_replace(trim(product), '\\s+', ' ')), '')
                                                                AS product,

               try_cast(trim(transaction_date) AS date)         AS transaction_date,

               try_cast(trim(quantity) AS int)                  AS quantity,

               try_cast(trim(amount) AS decimal(12, 2))         AS amount
        FROM transactions_duplicates_removed
    )
    WHERE transaction_id IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY transaction_id
        ORDER BY transaction_date DESC
    ) = 1
""")

# now show result

spark.sql("""
    SELECT count(*)                                  AS total_rows,

           count(transaction_id)                     AS transaction_id_non_null,
           count(DISTINCT transaction_id)            AS transaction_id_distinct,

           count(customer_id)                        AS customer_id_non_null,
           count(DISTINCT customer_id)               AS customer_id_distinct,
           count(*) - count(customer_id)             AS customer_id_nulls,

           count(product)                            AS product_non_null,
           count(DISTINCT product)                   AS product_distinct,
           count(*) - count(product)                 AS product_nulls,

           min(transaction_date)                     AS earliest_transaction,
           max(transaction_date)                     AS latest_transaction,
           count(*) - count(transaction_date)        AS transaction_date_nulls,

           min(quantity)                             AS quantity_min,
           max(quantity)                             AS quantity_max,
           sum(quantity)                             AS quantity_total,
           count(*) - count(quantity)                AS quantity_nulls,
           count_if(quantity <= 0)                   AS quantity_zero_or_negative,

           min(amount)                               AS amount_min,
           max(amount)                               AS amount_max,
           round(avg(amount), 2)                     AS amount_avg,
           sum(amount)                               AS amount_total,
           count(*) - count(amount)                  AS amount_nulls,
           count_if(amount < 0)                      AS amount_negative
    FROM transactions_clean
""").show(vertical=True)



## OLD Code from here on


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