# Test 1
# Import correct libraries


# Test 2 - Create Spark Session


# read this ile as variabel book "DataAnalysisWithPythonAndPySpark-trunk/data/DataAnalysisWithPythonAndPySpark-Data-trunk/gutenberg_books/1342-0.txt"

# split text by each value in the text block and rename the column to line

# print the schema of each line and show command
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

# words_nonull.select(length(col("word")).alias("length")).groupby("length").count()

results = words_nonull.groupby(col("word")).count()

results.orderBy(col("count").desc()).show()

results.coalesce(1).write.csv("simple_count.csv")
