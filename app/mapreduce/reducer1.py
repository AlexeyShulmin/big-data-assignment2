#!/usr/bin/env python3
# docstats_reducer.py: Reducer for document statistics
import sys

total_docs = 0
total_length = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, value = line.split("\t", 1)
    if key == "!!DOC_COUNT":
        # accumulate document count
        total_docs += int(value)
    elif key == "!!LENGTH_SUM":
        # accumulate total length
        total_length += int(value)
    else:
        # key is a document ID, value = "<length>\t<title>"
        # Output the doc_id, title, and length
        # (The order is doc_id, title, length as tab-separated)
        if "\t" in value:
            length_str, title = value.split("\t", 1)
        else:
            # Just in case title was empty (unlikely)
            length_str = value
            title = ""
        sys.stdout.write(f"{key}\t{title}\t{length_str}\n")

# After iterating all input, output the computed average length and total docs
if total_docs > 0:
    avg_length = total_length / total_docs
else:
    avg_length = 0.0
sys.stdout.write(f"AVG_LENGTH\t{avg_length}\n")
sys.stdout.write(f"TOTAL_DOCS\t{total_docs}\n")
