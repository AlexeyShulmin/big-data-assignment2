#!/usr/bin/env python3
# index_mapper.py: Mapper for inverted index
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t", 2)  # [doc_id, title, text]
    if len(parts) != 3:
        continue
    doc_id, title, text = parts
    # Tokenize text into terms
    words = text.split()
    for word in words:
        if word:
            # Emit term as key and doc_id as value for each occurrence
            sys.stdout.write(f"{word}\t{doc_id}\n")
