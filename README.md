# Catalog Schema Experiments

## SQLite Instructions
To generate the SQLite databases and run the queries for both normalized and datavault, we run the scripts in the following order:
1. genToNSQLCSV.py: generates the synthetic dataset and populates the normalized database.
2. genToDSQLRecords.py: loads the synthetic dataset into the datavault database.
3. nsql_queries.py: executes the experiment queries on the normalized database.
4. dsql_queries.py: executes the experiment queries on the datavault database.

## Neo4j Instructions
To generate the neo4j databases, we need two datasets:
1. The normalized dataset.
2. The datavault dataset.

### Normalized Database
Bulk import the normalized dataset using the norm_importCreator.py file:
1. Create a folder called 'baddates/' and move all csv files from the normalized dataset into this folder.
2. In norm_importCreator.py, make sure that all commands except the last two are uncommented.
3. Run norm_importCreator.py from the catalog_experiments/ directory (or whichever directory contains the baddates/ directory)
4. Once norm_importCreator.py is finished running, it should generate:
a. A new set of csv files, containing both header files, and a clean version of the original data.
b. a .sh file called 'full_norm_loader.sh'. 
5. Move this new set of csv files into the neo4j-.../import/ directory, and move the .sh file into the neo4j-.../ directory.
6. Make sure that the --database option in the .sh file matches the name of the database you want to load the files into.
7. Make this .sh file executable (e.g. "chmod +x") and run it.

This bulk loads the normalized data. To run the queries, go to the execute_full function in nneo4j_queries.py and make sure that all methods of the form self.execute_q*() are uncommented. Then, run nneo4j_queries.py. (Of course, you might want to test them one-by-one, in which case simply run the execute_qn() method to run query n).

### Datavault Database
Bulk import the datavault dataset by:
1. Running the dv_importCreator.py in the same directory where the datavault dataset csv files are located. As with the normalized bulk import creator, this will generate:
a. A new set of csv files, containing both header files, and a clean version of the original data.
b. a .sh file called 'full_dv_loader.sh'. 
2. Move this new set of csv files into the neo4j-.../import/ directory, and move the .sh file into the neo4j-.../ directory.
3. Make sure that the --database option in the .sh file matches the name of the database you want to load the files into.
4. Make this .sh file executable (e.g. "chmod +x") and run it.

This bulk loads the normalized data. To run the queries, go to the execute_full function in dneo4j_queries.py and make sure that all methods of the form self.execute_q*() are uncommented. Then, run dneo4j_queries.py. (Of course, you might want to test them one-by-one, in which case simply run the execute_qn() method to run query n).