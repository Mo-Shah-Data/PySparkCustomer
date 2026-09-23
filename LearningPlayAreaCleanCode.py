from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.appName(
    "Counting word occurences from a book."
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# If you need to read multiple text files, replace `1342-0` by `*`.
results = (
    spark.read.text("DataAnalysisWithPythonAndPySpark-trunk/data/DataAnalysisWithPythonAndPySpark-Data-trunk/gutenberg_books/*.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
)

results.orderBy("count", ascending=False).show(10)
results.coalesce(1).write.mode("overwrite").csv("./results_single_partition.csv")


### old code
# book = spark.read.text("DataAnalysisWithPythonAndPySpark-trunk/data/DataAnalysisWithPythonAndPySpark-Data-trunk/gutenberg_books/1342-0.txt")
#
# lines = book.select(split(col("value"), " ").alias("line"))
#
# lines.printSchema()
#
# lines.show(5)
#
# words = lines.select(explode(col("line")).alias("word"))
# words.show(15)
#
# words_lower = words.select(lower(col("word")).alias("word_lower"))
# words_lower.show(15)
#
#
# words_clean = words_lower.select(regexp_extract(col("word_lower"), "[a-z]+", 0).alias("word"))
# words_clean.show(15)
#
# words_nonull = words_clean.filter(col("word") != "")
# words_nonull.show()
#
# # words_nonull.select(length(col("word")).alias("length")).groupby("length").count()
#
# results = words_nonull.groupby(col("word")).count()
#
# results.orderBy(col("count").desc()).show()
#
# results.coalesce(1).write.csv("simple_count.csv")