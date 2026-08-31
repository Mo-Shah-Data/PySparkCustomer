spark.read

dir(spark.read.text)

book.show()# chec its input parameters to change shape of output and sample of data.

## The task in chapter 2

from pyspark.sql.functions import col,split, explode, lower, regexp_extract
from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("Analyzing the vocabulary of Pride and Prejudice.")
         .getOrCreate())

book = spark.read.text("DataAnalysisWithPythonAndPySpark-trunk/data/DataAnalysisWithPythonAndPySpark-Data-trunk/gutenberg_books/1342-0.txt")

lines = book.select(split(col("value"), " ").alias("line"))

lines.printSchema()

lines.show(5)

words = lines.select(explode(col("line")).alias("word"))
words.show(15)

words_lower = words.select(lower(col("word")).alias("word_lower"))
words_lower.show(15)


words_clean = words_lower.select(regexp_extract(col("word_lower"), "[a-z]+", 0).alias("word"))
words_clean.show(15)

words_nonull = words_clean.filter(col("word") != "")
words_nonull.show()

from pyspark.sql import functions as F

path = "/tmp/pushdown"

(spark.range(0, 20_000_000, numPartitions=16)
 .select(
     F.col("id"),
     (F.col("id") % 1000).alias("k"),
     F.rand().alias("v"))
 .write.mode("overwrite")
 .parquet(path))

df = spark.read.parquet(path)
df.where(F.col("id") < 1000).explain(True)