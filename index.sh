#!/bin/bash
# index.sh: Run data preparation, indexing MapReduce jobs, and load index into Cassandra.

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

# 1. Start the PySpark document preparation job to create /data and /index/data
echo "Running PySpark document preparation..."
spark-submit --master yarn prepare_data.py 
# (Adjust the path to prepare_docs.py as needed; here /app is assumed to contain the code)

# 2. Remove any old output directories in HDFS for clean run
hdfs dfs -rm -r -f /index/docstats_out
hdfs dfs -rm -r -f /index/index_out

# 3. Run Hadoop Streaming job for Document Statistics (single reducer for total calc)
echo "Running Hadoop MapReduce for document statistics..."
STREAMING_JAR=$(find $HADOOP_HOME/share/hadoop/tools/lib -name 'hadoop-streaming*.jar' | head -n1)
hadoop jar "$STREAMING_JAR" \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -archives /app/.venv.tar.gz \
  -D mapreduce.job.name="DocStatsJob" \
  -D mapreduce.job.reduces=1 \
  -input /index/data \
  -output /index/docstats_out \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py"

# 4. Run Hadoop Streaming job for Inverted Index
echo "Running Hadoop MapReduce for inverted index..."
# (This job can use multiple reducers to parallelize by term; we'll let Hadoop decide or set a number)
hadoop jar "$STREAMING_JAR" \
  -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
  -archives /app/.venv.tar.gz \
  -D mapreduce.job.name="IndexJob" \
  -D mapreduce.job.reduces=4 \
  -input /index/data \
  -output /index/index_out \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py"

# 5. Retrieve MapReduce outputs to local for Cassandra loading
hdfs dfs -get -f /index/docstats_out/part-* docstats_output.txt
hdfs dfs -get -f /index/index_out/part-* index_output.txt

# 6. Parse document stats output: separate avg/total from individual docs
AVG_DL=$(grep -P '^AVG_LENGTH\t' docstats_output.txt | cut -f2 | tr -d '\n')
TOTAL_DOCS=$(grep -P '^TOTAL_DOCS\t' docstats_output.txt | cut -f2 | tr -d '\n')
grep -v -P '^(AVG_LENGTH|TOTAL_DOCS)\t' docstats_output.txt > doc_lengths.txt

# 7. Parse inverted index output: separate TF and DF lines into two files
grep -P '^TF\t' index_output.txt | cut -f2- > postings.txt   # remove the "TF" prefix column
grep -P '^DF\t' index_output.txt | cut -f2- > vocab.txt      # remove the "DF" prefix column

# 8. Load data into Cassandra keyspace/tables using cqlsh
echo "Creating keyspace and tables in Cassandra..."
cqlsh -e "CREATE KEYSPACE IF NOT EXISTS search WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
cqlsh -k search -e "
   -- Create tables (if not exist)
   CREATE TABLE IF NOT EXISTS doc_stats (doc_id text PRIMARY KEY, title text, length int);
   CREATE TABLE IF NOT EXISTS inverted_index (term text, doc_id text, tf int, PRIMARY KEY (term, doc_id));
   CREATE TABLE IF NOT EXISTS vocab (term text PRIMARY KEY, doc_freq int);
   CREATE TABLE IF NOT EXISTS meta (key text PRIMARY KEY, value text);
"

echo "Inserting index data into Cassandra..."
# Bulk load postings (term, doc_id, tf) into inverted_index
cqlsh -k search -e "COPY inverted_index(term, doc_id, tf) FROM 'postings.txt' WITH DELIMITER='\t';"
# Bulk load vocabulary (term, doc_freq) into vocab
cqlsh -k search -e "COPY vocab(term, doc_freq) FROM 'vocab.txt' WITH DELIMITER='\t';"
# Bulk load document stats (doc_id, title, length) into doc_stats
cqlsh -k search -e "COPY doc_stats(doc_id, title, length) FROM 'doc_lengths.txt' WITH DELIMITER='\t';"
# Insert meta information (total_docs and avg_dl)
cqlsh -k search -e "INSERT INTO meta(key, value) VALUES ('total_docs', '${TOTAL_DOCS}');
                    INSERT INTO meta(key, value) VALUES ('avg_dl', '${AVG_DL}');"

echo "Indexing pipeline completed successfully."
