#!/usr/bin/env python3
# docstats_mapper.py: Mapper for document statistics
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t", 2)  # split into [doc_id, title, text]
    if len(parts) != 3:
        continue
    doc_id, title, text = parts
    # Calculate document length in terms of number of words
    words = text.split()
    doc_length = len(words)
    # Emit document length and title (key=doc_id)
    # Format value as "<length>\t<title>" so reducer can separate them
    sys.stdout.write(f"{doc_id}\t{doc_length}\t{title}\n")
    # Emit special keys for total count and length sum
    sys.stdout.write(f"!!DOC_COUNT\t1\n")
    sys.stdout.write(f"!!LENGTH_SUM\t{doc_length}\n")
