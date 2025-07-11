#!/usr/bin/env python3
"""
query.py: Spark-based query engine that uses BM25 to rank documents.
Reads a query from stdin or argument, retrieves relevant postings from Cassandra, and prints top 10 doc IDs and titles.
"""
import re
import sys, math
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession with Cassandra connector support
spark = SparkSession.builder.appName("QueryEngine").config("spark.cassandra.connection.host", "cassandra-server").config("spark.cassandra.connection.port", "9042").config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0").getOrCreate()

# Read query from stdin if not provided as argument
if len(sys.argv) > 1:
    query_text = " ".join(sys.argv[1:])
else:
    # If running in a pipeline, the query might be fed via stdin
    query_text = sys.stdin.read().strip()
if not query_text:
    print("No query provided.")
    sys.exit(0)

# Clean the query similar to docs (lowercase, remove non-alphanumeric, split into terms)
query_terms = [t for t in re.sub(r'[^a-z0-9\s]', ' ', query_text.lower()).split() if t]

if not query_terms:
    print("No valid query terms.")
    sys.exit(0)

# Fetch global stats from meta table (total docs, average doc length):
meta_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(keyspace="search", table="meta").load() \
    .filter(F.col("key").isin("total_docs", "avg_dl"))
meta = {row['key']: row['value'] for row in meta_df.collect()}
# Convert to numeric
N = float(meta.get("total_docs", "0"))
avg_dl = float(meta.get("avg_dl", "0"))

# Fetch document frequency for each query term from vocab table (to compute IDF):
vocab_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(keyspace="search", table="vocab").load() \
    .filter(F.col("term").isin(query_terms))
vocab_map = {row['term']: row['doc_freq'] for row in vocab_df.collect()}

# Fetch postings for query terms from inverted_index table:
# Only retrieve rows where term is in the query set
postings_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(keyspace="search", table="inverted_index").load() \
    .filter(F.col("term").isin(query_terms))
# Join with doc_stats to get document lengths and titles
docstats_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(keyspace="search", table="doc_stats").load() \
    .select("doc_id", "title", "length")
postings_joined = postings_df.join(docstats_df, on="doc_id")

# Convert to RDD for scoring
posts_rdd = postings_joined.rdd
# Broadcast BM25 parameters and vocab map for efficiency
bm25_params = {'k1': 1.2, 'b': 0.75, 'N': N, 'avg_dl': avg_dl}
vocab_bcast = spark.sparkContext.broadcast(vocab_map)
params_bcast = spark.sparkContext.broadcast(bm25_params)

# Map each term occurrence to a partial BM25 score
def compute_score(row):
    term = row['term']
    tf = row['tf']
    doc_len = row['length']
    title = row['title']
    df = vocab_bcast.value.get(term, 0)
    if df == 0:
        return None
    N = params_bcast.value['N']
    idf = math.log(1 + (N - df + 0.5) / (df + 0.5))
    k1 = params_bcast.value['k1']
    b = params_bcast.value['b']
    avg_dl = params_bcast.value['avg_dl']
    norm_tf = tf * (k1 + 1) / (tf + k1 * (1 - b + b * (doc_len / avg_dl))) 
    score = idf * norm_tf
    # Output as (doc_id, (title, score))
    return (row['doc_id'], (title, score))

scores_rdd = posts_rdd.map(compute_score).filter(lambda x: x is not None)
# Reduce by doc_id to sum scores of multiple terms in the same document
def merge_scores(x, y):
    return (x[0] if x[0] else y[0], x[1] + y[1])

doc_scores = scores_rdd.reduceByKey(merge_scores)

# Get top 10 documents by score
# Use takeOrdered with negative score for descending sort
top10 = doc_scores.takeOrdered(10, key=lambda item: -item[1][1])

# Print top 10 results
for rank, (doc_id, (title, score)) in enumerate(top10, start=1):
    print(f"{rank}\t{doc_id}\t{title}\t{score}")
