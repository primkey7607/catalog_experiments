# Catalog Schema Experiments
We run the scripts in the following order:
1. genToNSQLCSV.py: generates the synthetic dataset and populates the normalized database.
2. genToDSQLRecords.py: loads the synthetic dataset into the datavault database.
3. nsql_queries.py: executes the experiment queries on the normalized database.
4. dsql_queries.py: executes the experiment queries on the datavault database.
