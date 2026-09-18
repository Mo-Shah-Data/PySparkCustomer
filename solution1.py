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


# customers needs further cleaning
spark.sql("select * from customers order by customer_id").show(20)
# transactions table has a lot of nulls in customer_id col
spark.sql("select * from transactions where customer_id IS NOT NULL order by customer_id").show(100)
# join
spark.sql("""
    CACHE TABLE all_customers_transactions
    OPTIONS ('storageLevel' = 'MEMORY_ONLY')
    AS SELECT * FROM customers LEFT JOIN transactions USING (customer_id)
""")
# spark.catalog
#
spark.catalog.listTables()
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