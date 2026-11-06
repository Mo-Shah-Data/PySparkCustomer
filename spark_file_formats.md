# Spark File Formats: In-Depth Review

Below is a review of each format: how it stores data, how Spark handles it, its pros and cons, and when to use it. A comparison table and a short experiment come at the end.

---

## How Spark handles any file format

These mechanics apply to all formats, and the differences between formats come down to how well each one fits them.

**Reading**
- Spark lists the files in the folder, then splits them into chunks of up to 128 MB each (`spark.sql.files.maxPartitionBytes`). Each chunk becomes one task.
- A format is **splittable** if Spark can start reading from the middle of a file. If it isn't, the whole file goes to a single task, however big it is.
- Spark asks the format to skip work where it can:
  - **Column pruning:** read only the columns the query uses.
  - **Predicate pushdown:** skip data that can't match a `WHERE` filter.

  Only some formats support these.

**Writing**
- Each task writes **one file per output partition**, which is why you get `part-00000...` files.
- Files are first written to a `_temporary` folder, then moved into place. A `_SUCCESS` marker is added when everything has finished.
- `partitionBy("year")` adds a folder level per value, such as `year=2024/`. That lets Spark skip whole folders when you filter on that column, whatever the format.

You can see what Spark pushed down to the file format with `df.explain()`: look for `ReadSchema` (the columns read) and `PushedFilters` (the filters applied at the file level).

---

## 1. Parquet

**What it is:** a binary, columnar format, and Spark's default.

### How it stores data

```
file
├── Row group 1 (~128 MB of rows)
│   ├── Column chunk: customer_id → pages (~1 MB each)
│   ├── Column chunk: customer_name → pages
│   └── Column chunk: amount → pages
├── Row group 2 ...
└── Footer: schema + min/max/null-count stats for every column chunk
```

- **Columnar:** all values of one column are stored together. Similar values sit side by side, which makes them compress very well.
- **Encodings, applied before compression:**
  - *Dictionary encoding:* repeated strings like `"London"` are stored once and referenced by small integer IDs.
  - *Run-length encoding:* `A,A,A,A` becomes "A × 4".
  - *Bit-packing:* small integers use fewer bits.
  - *Delta encoding:* stores the differences between sorted values.

  The compression codec (snappy, zstd or gzip) is applied on top of these, page by page.
- **Footer metadata:** the schema and the min/max statistics per row group live at the end of the file.

### How Spark uses it

- **Column pruning:** for `SELECT amount FROM t`, Spark reads only the `amount` column chunks. The rest of the file is never touched.
- **Predicate pushdown:** for `WHERE amount > 1000`, Spark checks each row group's min/max in the footer. A row group whose maximum is 500 is skipped entirely.
- **Vectorized reader:** Spark decodes columns in batches straight into its in-memory columnar format, which is much faster than reading row by row.
- **Splittable:** at row-group boundaries, whatever codec you use.
- **Schema stored in the file:** types are exact, with no inference needed.

### Pros

- Smallest files and fastest analytical reads of any format here.
- Exact types, including nested structs, arrays and maps.
- Readable by nearly every data tool: Pandas, DuckDB, Snowflake, BigQuery, Athena, Polars.

### Cons

- Not human-readable; you need a tool to look inside.
- Slower to write than CSV or Avro, because of the encoding and statistics work.
- Files are immutable. You can't update a row in place, only rewrite the file.
- Many small files hurt performance, because each one has a footer and metadata overhead.
- Schema changes between files need `mergeSchema`, which is off by default because it's slow.

### Use it for

Almost everything Spark reads and writes internally: data lakes, analytics, and intermediate pipeline outputs.

---

## 2. ORC (Optimized Row Columnar)

**What it is:** a columnar format from the Hive world, and Parquet's closest relative.

### How it stores data

```
file
├── Stripe 1 (~64 MB)
│   ├── Index data: min/max per column, every 10,000 rows
│   ├── Row data: one stream per column
│   └── Stripe footer
├── Stripe 2 ...
└── File footer + postscript: schema, stripe stats, codec
```

- It is columnar like Parquet, but the indexes are finer-grained: statistics every 10,000 rows instead of per ~128 MB row group.
- It supports optional **bloom filters**, which answer "is this value definitely not here?" quickly. That makes them good for lookups like `WHERE customer_id = 123`.

### How Spark uses it

It gets the same benefits as Parquet: column pruning, predicate pushdown, a vectorized reader, and splitting at stripe boundaries.

### Pros

- Excellent compression, often slightly better than Parquet.
- The fine-grained indexes and bloom filters can skip more data on selective filters.
- It's the native format for Hive ACID tables.

### Cons

- Far less supported outside the Hadoop and Hive world. Pandas, cloud warehouses and most modern tools favour Parquet.
- Less community attention in the Spark ecosystem.

### Use it for

Existing Hive or Hadoop environments. For new projects, Parquet is the safer default.

---

## 3. Avro

**What it is:** a binary, **row-based** format designed for data exchange and streaming.

### How it stores data

```
file
├── Header: the schema as JSON + codec + a random 16-byte sync marker
├── Data block 1: N complete rows, compressed together, then the sync marker
├── Data block 2 ...
```

- **Row-based:** each record's fields are stored together, one record after another.
- **The schema is embedded** in the header as JSON, so every file describes itself.
- The **sync markers** between blocks let a reader jump into the middle of a file and find the next block. That's what makes Avro splittable.

### How Spark uses it

- It needs the `spark-avro` package, which is part of the Spark project but not loaded by default.
- **No column pruning benefit:** Spark has to decode every field of every record, even for a one-column query.
- **No row-group statistics,** so filters can't skip data.
- `from_avro()` and `to_avro()` convert Avro bytes to and from columns. This is common when reading from Kafka.

### Pros

- **The best schema evolution** of these formats. A reader schema and a writer schema are resolved against each other, so you can add fields with defaults, remove fields, or rename fields with aliases, and old and new data still read correctly.
- Fast to write, because there's no columnar reorganisation.
- Compact binary format, splittable with any codec.
- The standard for Kafka and event streaming, with Schema Registry.

### Cons

- Slow for analytics: it reads whole rows even when you need one column.
- Not human-readable.
- Needs the extra package in Spark.

### Use it for

Streaming and event data, Kafka, and landing raw data whose schema changes often. A common pattern is to land data as Avro, then convert it to Parquet for analysis.

---

## 4. CSV

**What it is:** plain text, one row per line, with values separated by a delimiter.

### How it stores data

```
transaction_id,customer_id,customer_name,amount
1001,42,Jane Smith,250.00
1002,17,"Smith, John",80.50
```

There's no schema, no types and no metadata. Everything is text.

### How Spark uses it

- It uses the Univocity parser to handle quoting, escaping and delimiters.
- **Schema:** without `inferSchema`, every column is a string. With `inferSchema=true`, Spark makes an **extra full pass** over the data to guess the types, which is slow on large data. The best practice is to pass an explicit schema with `.schema(...)`.
- **Splittable** when uncompressed or bzip2-compressed, because Spark can split at line breaks. It is **not splittable** with gzip, or with `multiLine=true` (quoted fields containing newlines).
- **Bad rows** are handled by the `mode` option:
  - `PERMISSIVE` (the default) sets bad fields to null.
  - `DROPMALFORMED` drops the bad rows.
  - `FAILFAST` throws an error on the first bad row.
- There's no column pruning at the file level. Every line is fully read, though Spark only parses the columns it needs.

### Pros

- Universal: Excel, every database, every language and every person can use it.
- Human-readable and easy to edit or inspect.
- Simple to append to.

### Cons

- **No types.** Dates, decimals and nulls are all ambiguous. Is an empty field null or an empty string?
- Large files and slow parsing.
- Fragile, because of commas inside values, quote escaping, encodings and line endings.
- No nested data.
- gzip, the most common codec, kills parallelism.

### Use it for

Exchanging data with people or with systems that need it, such as Excel exports or reports. Avoid it for anything Spark will read again.

---

## 5. JSON

**What it is:** plain text. By default Spark uses **JSON Lines**: one complete JSON object per line.

### How it stores data

```
{"transaction_id":1001,"customer_id":42,"customer_name":"Jane Smith","amount":250.0}
{"transaction_id":1002,"customer_id":17,"customer_name":"Smith, John","amount":80.5}
```

Column names are repeated on **every row**.

### How Spark uses it

- It uses the Jackson parser.
- **Schema inference scans the data** by default to work out the fields and types. You can sample instead with `samplingRatio`, or pass `.schema(...)`.
- **Splittable** in JSON Lines format, uncompressed or bzip2. **Not splittable** with gzip, or with `multiLine=true`, which is needed for one big JSON document or pretty-printed JSON.
- Nested objects become `struct` columns and JSON arrays become `array` columns.
- There's no column pruning or filter pushdown at the file level.

### Pros

- Handles nested and semi-structured data naturally.
- Self-describing: field names are in the data.
- Human-readable, and native to web APIs and JavaScript.
- Tolerant of fields being missing from some records.

### Cons

- The largest files of any format here, because the key names repeat on every row.
- The slowest to parse.
- Limited types: there's no date type, so dates are stored as strings, and numbers can lose precision.
- Inferring the schema on large data is expensive.

### Use it for

API data, logs from web services, and handing data to applications. Convert it to Parquet before doing analytics on it.

---

## 6. Text

**What it is:** raw lines of text. Spark adds no structure at all.

### How it works in Spark

- **Reading:** each line becomes one row in a single string column named `value`. With `wholetext=true`, each *file* becomes one row instead.
- **Writing:** the DataFrame **must have exactly one string column**. That's why you used `concat_ws` earlier.
- It's splittable at line breaks when uncompressed or bzip2-compressed.

### Pros

- Total flexibility. You decide the format, and can parse it with `split()`, `regexp_extract()` or anything else.
- The best fit for unstructured data like log files.

### Cons

- No schema, no types, no structure. All the parsing work is yours.
- No optimizations of any kind.

### Use it for

Reading log files or other custom formats, and writing output for systems that expect plain lines.

---

## 7. XML (Spark 4.0+)

**What it is:** a nested, tag-based text format, built into Spark since 4.0. Before that it was the `spark-xml` package.

- You tell Spark which tag marks a row with `rowTag`, for example `<transaction>`. Nested tags become structs.
- Parsing is slow and the files are verbose.
- Use it only when a source system gives you XML, and convert it to Parquet straight away.

---

## Side-by-side comparison

| | Parquet | ORC | Avro | CSV | JSON | Text |
|---|---|---|---|---|---|---|
| Layout | Columnar | Columnar | Row | Row | Row | Line |
| Binary or text | Binary | Binary | Binary | Text | Text | Text |
| Schema stored | Yes, in the footer | Yes, in the footer | Yes, in the header | No | Field names only | No |
| Exact types | Yes | Yes | Yes | No | Partial | No |
| Nested data | Yes | Yes | Yes | No | Yes | No |
| Column pruning | Yes | Yes | No | No | No | No |
| Filter pushdown | Yes, via row-group stats | Yes, via stripe and row index, plus bloom filters | No | No | No | No |
| Splittable with gzip | Yes | Yes | Yes | **No** | **No** | **No** |
| Schema evolution | Good (add columns) | Good | **Best** | None | Flexible but untyped | None |
| File size | Smallest | Smallest | Small | Large | Largest | Large |
| Read speed for analytics | Fastest | Fastest | Medium | Slow | Slowest | Slow |
| Write speed | Medium | Medium | Fast | Fast | Medium | Fast |
| Human-readable | No | No | No | Yes | Yes | Yes |
| Tool support | Excellent | Hadoop and Hive | Streaming tools | Universal | Universal | Universal |

### Choosing a format

- **Spark or another analytics tool will read it again:** Parquet.
- **It's going to a person or to Excel:** CSV.
- **It's going to an app or an API:** JSON.
- **It's streaming or Kafka data, or the schema changes often:** Avro.
- **You're in an existing Hive environment:** ORC.
- **It's logs or a custom format:** Text.

---

## Beyond file formats: table formats

**Delta Lake**, **Apache Iceberg** and **Apache Hudi** are not new file formats. They store data as **Parquet files** and add a **transaction log** alongside. The log gives you:

- ACID transactions, so readers never see half-written data.
- `UPDATE`, `DELETE` and `MERGE` on individual rows.
- Time travel, so you can read the table as it was yesterday.
- Safe schema changes.

They fix Parquet's biggest weakness, immutability, and they're the standard in modern data platforms like Databricks and Snowflake.

---

## Try it yourself

Run this in your notebook to see the differences on your own data:

```python
import os

for fmt in ["sql_parquet", "sql_csv", "sql_json", "sql_text"]:
    folder = PROJECT_DIR / "output" / fmt
    if folder.exists():
        size = sum(f.stat().st_size for f in folder.glob("part-*"))
        print(f"{fmt:12} {size / 1024 / 1024:8.2f} MB")
```

To see column pruning and pushdown in action:

```python
spark.read.parquet(f"{OUTPUT_DIR}/sql_parquet") \
     .select("amount").filter("amount > 1000").explain()
```

In the output, `ReadSchema` should list only `amount`, and `PushedFilters` should include `GreaterThan(amount,1000)`. Run the same query on the CSV output and compare.
