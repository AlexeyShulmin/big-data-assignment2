#!/usr/bin/env python3
"""
prepare_docs.py: PySpark job to prepare documents for indexing.
- Reads input Parquet file of documents.
- Selects and cleans at least 1000 documents.
- Writes each document as <doc_id>_<doc_title>.txt to HDFS /data.
- Creates a unified RDD of documents and saves to HDFS /index/data (tab-separated: id, title, text).
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import re
import os
from tqdm import tqdm

spark = SparkSession.builder.appName("DocumentPrep").master("local").config("spark.sql.parquet.enableVectorizedReader", "true").getOrCreate()

# 1. Read input Parquet file
df = spark.read.parquet("/user/root/app/a.parquet")
docs_df = df.select("id", "title", "text").limit(1000)

# 2. Clean document text and titles: remove non-alphanumeric characters, convert to lowercase, normalize whitespace.
docs_df = docs_df.withColumn(
    "clean_text", 
    F.regexp_replace(F.lower(F.col("text")), "[^a-z0-9\\s]", " ")
)
docs_df = docs_df.withColumn(
    "clean_text", 
    F.regexp_replace(F.col("clean_text"), "\\s+", " ")
)
# Remove any tab/newline in title to preserve formatting in output
docs_df = docs_df.withColumn(
    "clean_title",
    F.regexp_replace(F.col("title"), "[\\t\\n]", " ")
)

# 3. Write documents to HDFS /data as individual .txt files
docs = docs_df.select("id", "clean_title", "clean_text").collect()
os.system("hdfs dfs -mkdir -p /data")  # ensure /data directory exists in HDFS
for row in tqdm(docs):
    doc_id = row["id"]
    title = row["clean_title"]
    text = row["clean_text"]
    # Construct filename as "<doc_id>_<doc_title>.txt", with unsafe chars replaced by underscore
    safe_title = re.sub(r"[^A-Za-z0-9]+", "_", title)[:50]  # limit length for safety
    filename = f"{doc_id}_{safe_title}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text if text is not None else "")
    # Put the file to HDFS /data (overwrite if exists)
    os.system(f"hdfs dfs -put -f {filename} /data/")
    os.remove(filename)

# 4. Save unified documents RDD to HDFS /index/data
docs_rdd = spark.sparkContext.parallelize(docs) \
    .map(lambda row: f"{row['id']}\t{row['clean_title']}\t{row['clean_text']}")
docs_rdd.saveAsTextFile("/index/data")

spark.stop()
