#!/bin/bash
# app.sh: Run the full indexing pipeline and then execute sample search queries.
service ssh restart 
# 1. Start all necessary services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# 2. Run the indexing pipeline to build the search index
echo "Running indexing pipeline..."
bash index.sh

# 3. Execute a few test queries and show the top 10 results for each
QUERIES=("big data analytics" "machine learning model" "distributed database")
for query in "${QUERIES[@]}"; do
    echo ""
    echo "Query: $query"
    bash search.sh "$query"
done
