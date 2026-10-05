from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (SparkSession
         .builder
         .appName("Analysis of Customers and Transactions.")
         .getOrCreate())

customers_df_path = "./data/customers"
transactions_df_path = "./data/transactions"

customers_df = (spark.read.option("header",True).option("ignoreLeadingWhiteSpace", True)
                .option("ignoreTrailingWhiteSpace", True).csv(customers_df_path))
transactions_df = (spark.read.option("header",True).option("ignoreLeadingWhiteSpace", True)
                   .option("ignoreTrailingWhiteSpace", True).csv(transactions_df_path))

# customers_df.explain()             # physical plan only
customers_df.explain(extended=True)         # extended: parsed, analyzed, optimized logical + physical plans
customers_df.explain(True)
customers_df.explain(mode='cost')
transactions_df.explain(mode='codegen')
customers_df.explain(mode='simple')

customers_df.explain(mode='codegen')
customers_df.explain("formatted")  # Spark 3.0+: modes are "simple", "extended", "codegen", "cost", "formatted"

