#!/usr/bin/env python3
# index_reducer.py: Reducer for inverted index
import sys

current_term = None
current_doc = None
tf_count = 0
doc_count = 0
postings = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    term, doc_id = line.split("\t", 1)
    if current_term is None:
        # Initialize the first term
        current_term, current_doc, tf_count, doc_count = term, doc_id, 1, 0
        postings = []
    elif term != current_term:
        # Term changed, output results for the previous term
        # Finish the last doc for previous term
        postings.append(f"{current_doc}\t{tf_count}")
        doc_count += 1
        # Output all postings for previous term
        for p in postings:
            sys.stdout.write(f"TF\t{current_term}\t{p}\n")
        # Output document frequency for previous term
        sys.stdout.write(f"DF\t{current_term}\t{doc_count}\n")
        # Reset counters for new term
        current_term, current_doc, tf_count, doc_count = term, doc_id, 1, 0
        postings = []
    else:
        # Same term as current_term
        if doc_id == current_doc:
            # Same document continuing – increment term frequency
            tf_count += 1
        else:
            # New document for the same term
            postings.append(f"{current_doc}\t{tf_count}")
            doc_count += 1
            # Reset for the new document
            current_doc = doc_id
            tf_count = 1

# Handle the last term after loop ends
if current_term is not None:
    postings.append(f"{current_doc}\t{tf_count}")
    doc_count += 1
    for p in postings:
        sys.stdout.write(f"TF\t{current_term}\t{p}\n")
    sys.stdout.write(f"DF\t{current_term}\t{doc_count}\n")
