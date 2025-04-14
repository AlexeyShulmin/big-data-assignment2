#!/bin/bash
# search.sh: Launch the query Spark job on YARN and display top 10 results.

# If query is provided as argument(s), use it; otherwise, read from stdin.
if [ $# -gt 0 ]; then
    QUERY="$*"
elif ! [ -t 0 ]; then
    # if input is piped
    QUERY="$(cat)"
else
    # interactive prompt if no args and no pipe
    echo -n "Enter search query: "
    read QUERY
fi

if [ -z "$QUERY" ]; then
    echo "No query provided."
    exit 1
fi

# Run the query Spark job (on YARN) with the query as argument
spark-submit --master yarn /app/query.py "$QUERY"
