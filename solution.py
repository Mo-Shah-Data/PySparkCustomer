from pyspark.pandas.missing import groupby
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession
         .builder
         .appName("Analysis of Customers and Transactions.")
         .getOrCreate())

sc = spark.sparkContext

customers_df_path = "data/customers"
transactions_df_path = "data/transactions"

customers_df = (spark.read.option("header",True).option("ignoreLeadingWhiteSpace", True)
                .option("ignoreTrailingWhiteSpace", True).csv(customers_df_path))
transactions_df = (spark.read.option("header",True).option("ignoreLeadingWhiteSpace", True)
                   .option("ignoreTrailingWhiteSpace", True).csv(transactions_df_path))

customers_df.show()
transactions_df.show()

customers_df.printSchema()
transactions_df.printSchema()

## Cleaning of customers table
group_customer_id = customers_df.groupby(col("customer_id"))
print(group_customer_id)

customer_id_counts = customers_df.groupby(col("customer_id")).count()
print(customer_id_counts)


customer_id_counts.orderBy(col("count").desc()).show()
