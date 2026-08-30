from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("Analysis of Customers and Transactions.")
         .getOrCreate())

sc = spark.sparkContext

customers_df_path = "data/customers"
transactions_df_path = "data/transactions"

customers_df = spark.read.csv(customers_df_path)
transactions_df = spark.read.csv(transactions_df_path)

customers_df.show()
transactions_df.show()

