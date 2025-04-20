#!/bin/bash
# index.sh: Run data preparation, indexing MapReduce jobs, and load index into Cassandra.

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

hdfs dfs -rm -r -f /index/docstats_out
hdfs dfs -rm -r -f /index/index_out
hdfs dfs -rm -r -f /index/data

# 1. Start the PySpark document preparation job to create /data and /index/data
echo "Running PySpark document preparation..."
spark-submit --master yarn prepare_data.py 

# 2. Run Hadoop Streaming job for Document Statistics (single reducer for total calc)
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

# 3. Run Hadoop Streaming job for Inverted Index
echo "Running Hadoop MapReduce for inverted index..."
hadoop jar "$STREAMING_JAR" \
  -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
  -archives /app/.venv.tar.gz \
  -D mapreduce.job.name="IndexJob" \
  -D mapreduce.job.reduces=4 \
  -input /index/data \
  -output /index/index_out \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py"

# 4. Retrieve MapReduce outputs to local for Cassandra loading
hdfs dfs -get -f /index/docstats_out/part-* docstats_output.txt
hdfs dfs -getmerge /index/index_out/part-* index_output.txt

# 5. Parse document stats output: separate avg/total from individual docs
AVG_DL=$(grep -P '^AVG_LENGTH\t' docstats_output.txt | cut -f2 | tr -d '\n')
TOTAL_DOCS=$(grep -P '^TOTAL_DOCS\t' docstats_output.txt | cut -f2 | tr -d '\n')
grep -v -P '^(AVG_LENGTH|TOTAL_DOCS)\t' docstats_output.txt > doc_lengths.txt

# 6. Parse inverted index output: separate TF and DF lines into two files
grep -P '^TF\t' index_output.txt | cut -f2- > postings.txt   # remove the "TF" prefix column
grep -P '^DF\t' index_output.txt | cut -f2- > vocab.txt      # remove the "DF" prefix column

# 7. Load data into Cassandra keyspace/tables using cqlsh
echo "Creating keyspace and tables in Cassandra..."
echo "Inserting index data into Cassandra..."
export AVG_DL
export TOTAL_DOCS
python3 /app/load_index.py

echo "Indexing pipeline completed successfully."
