from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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

# .createOrReplaceTempView("employees") - DONE

customers_df.show()
transactions_df.show()

# Define your own schema and pass it to the read function when reading csv file. Define schema in string format.
customers_df.printSchema()
transactions_df.printSchema()

customers_df.summary().show()
transactions_df.summary().show()

## Cleaning of customers table
customer_id_counts = customers_df.groupby(F.col("customer_id")).count()
# checking to make sure customers id is unique - explain how groupby works under the hood
# how will spark distibtute the data over three nodes
customer_id_counts.orderBy(F.col("count").desc()).show()
# Explain how order by works under the hood?

# count nulls in primary keys before cleaning

# ToDo This in spark sql
customers_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in ["customer_id"]
]).show()
transactions_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in ["customer_id", "transaction_id"]
]).show()



# normalise keys and drop nulls/blanks FIRST, so that "a123" and " A123 "
# become the same value before we dedupe

# ToDo - go through spark sql functions and learn them - focus on string, and aggregate functions.
cust_key_cols = ["customer_id"]
customers_nonull = customers_df.select(*[
    F.upper(F.trim(F.col(c))).alias(c) if c in cust_key_cols else F.col(c)
    for c in customers_df.columns
]).filter(
    F.col("customer_id").isNotNull() & (F.col("customer_id") != "")
)
transaction_key_cols = ["customer_id", "transaction_id"]
transactions_nonull = transactions_df.select(*[
    F.upper(F.trim(F.col(c))).alias(c) if c in transaction_key_cols else F.col(c)
    for c in transactions_df.columns
]).filter(
    F.col("customer_id").isNotNull() & (F.col("customer_id") != "") &
    F.col("transaction_id").isNotNull() & (F.col("transaction_id") != "")
)

#drop duplicate customers
#Todo: how does this work - this is a common interview question
# ToDo: if i want to drop duplicate and pick which duplicate to drop, maybe earliest maybe one with a empty column?
# ToDo: what if iwant to drop duplicates based on multiple columns? how to do this
print("dropping duplicates")
customers_clean = customers_nonull.dropDuplicates(["customer_id"])
cust_duplicates = (customers_clean
         .groupBy("customer_id")
         .count()
         .filter(F.col("count") > 1))

# Todo explain parameters such as those available like truncate
print("duplicate customer_ids remaining:", cust_duplicates.count())  # expect 0
cust_duplicates.show(10, truncate=False)        # expect empty

#drop duplicate transactions
# dedupe on transaction_id only - the pair ["transaction_id","customer_id"]
# would keep two rows sharing a transaction_id
transactions_clean = transactions_nonull.dropDuplicates(["transaction_id"])
transactions_duplicates = (transactions_clean
         .groupBy("transaction_id")
         .count()
         .filter(F.col("count") > 1))
print("duplicate transactions_ids remaining:", transactions_duplicates.count())  # expect 0
transactions_duplicates.show(10, truncate=False)                             # expect empty

# these get counted several times below, so avoid recomputing the parse+shuffle
# Todo: find out when and where and why to use cache options - what are they for?
# Todo: what is the differnec between cache and persist(differnet kinds of persists)
customers_clean.cache()
transactions_clean.cache()
print("customers:", customers_df.count(), "->", customers_clean.count())
print("transactions:", transactions_df.count(), "->", transactions_clean.count())

# inner join datasets
# Todo: how does this join work under the hood
# Todo: learn all different kinds of joins in sql
customers_with_transactions = transactions_clean.join(customers_clean, on="customer_id", how="inner")
print(transactions_clean.count(), "->", customers_with_transactions.count())

# ToDo every spark job should be finished with an output - output in json.
# Todo Stretch goal  - learn about binary output - parquet and avro

# all transactions, matched or not
transactions_withorwithout_customer = transactions_clean.join(customers_clean, on="customer_id", how="left")

# all customers, with or without transactions
customers_withorwithout_transactions = customers_clean.join(transactions_clean, on="customer_id", how="left")