#!/usr/bin/env python3
"""
load_index.py

Connect to Cassandra in the cassandra-server container,
create keyspace/tables, and bulk-load postings.txt,
vocab.txt, and doc_lengths.txt into Cassandra tables.
"""

import os
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel

def main():
    # 1) Connect to the Cassandra cluster via its Docker service name
    cluster = Cluster(['cassandra-server'], port=9042)
    session = cluster.connect()

    # 2) Create keyspace if needed
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    session.set_keyspace('search')

    # 3) Create tables if they don't exist
    session.execute("""
      CREATE TABLE IF NOT EXISTS doc_stats (
        doc_id text PRIMARY KEY,
        title text,
        length int
      );
    """)
    session.execute("""
      CREATE TABLE IF NOT EXISTS inverted_index (
        term text,
        doc_id text,
        tf int,
        PRIMARY KEY (term, doc_id)
      );
    """)
    session.execute("""
      CREATE TABLE IF NOT EXISTS vocab (
        term text PRIMARY KEY,
        doc_freq int
      );
    """)
    session.execute("""
      CREATE TABLE IF NOT EXISTS meta (
        key text PRIMARY KEY,
        value text
      );
    """)

    # 4) Bulk‑load doc_stats from file
    insert_doc = session.prepare(
        "INSERT INTO doc_stats (doc_id, title, length) VALUES (?, ?, ?)"
    )
    with open('doc_lengths.txt') as f:
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        for line in f:
            doc_id, title, length = line.rstrip('\n').split('\t')
            batch.add(insert_doc, (doc_id, title, int(length)))
            # flush every 100 rows to avoid too‐large batches
            if len(batch) >= 100:
                session.execute(batch)
                batch.clear()
        if batch:
            session.execute(batch)

    # 5) Bulk‑load vocab from file
    insert_vocab = session.prepare(
        "INSERT INTO vocab (term, doc_freq) VALUES (?, ?)"
    )
    with open('vocab.txt') as f:
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        for line in f:
            term, df = line.rstrip('\n').split('\t')
            batch.add(insert_vocab, (term, int(df)))
            if len(batch) >= 100:
                session.execute(batch)
                batch.clear()
        if batch:
            session.execute(batch)

    # 6) Bulk‑load inverted_index from file
    insert_post = session.prepare(
        "INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)"
    )
    with open('postings.txt') as f:
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        for line in f:
            term, doc_id, tf = line.rstrip('\n').split('\t')
            batch.add(insert_post, (term, doc_id, int(tf)))
            if len(batch) >= 100:
                session.execute(batch)
                batch.clear()
        if batch:
            session.execute(batch)

    # 7) Insert meta info (TOTAL_DOCS and AVG_DL from env)
    total_docs = os.getenv('TOTAL_DOCS', '')
    avg_dl     = os.getenv('AVG_DL', '')
    session.execute(
        "INSERT INTO meta(key, value) VALUES (%s, %s)",
        ('total_docs', total_docs)
    )
    session.execute(
        "INSERT INTO meta(key, value) VALUES (%s, %s)",
        ('avg_dl',     avg_dl)
    )

    print("✅ Index data loaded into Cassandra keyspace 'search'.")
    cluster.shutdown()

if __name__ == "__main__":
    main()
