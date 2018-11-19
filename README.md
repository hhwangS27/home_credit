Project Source: [Kaggle](https://www.kaggle.com/c/home-credit-default-risk)

# Add `csv2schemasAndcqltables.py`

In this script, it uses the csvs in folder `20LineCSVs/` to create the
corresponding `schemas.py` and `cqltables.txt` based on
the description file `HomeCredit_columns_description.csv`. 

# Add `schemas.py`

In this file, I defined a dict `schemas` who is like this:
```
schemas = {tableName:schema, ...}
```

Include this code for using of schemas:
```
from schemas import schemas
```

# Create our cassandra keyspace called 'quartet' and create all the tables:
Firstly, log into cql using `cqlsh --cqlversion=3.4.2 199.60.17.188`
```
CREATE KEYSPACE quartet WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':2};
USE quartet;
```
Then, paste all the command in cqltables.txt into the console.

# Add `load_csvs_to_cassandra.py`

In this file, use spark to read csv files and load them into cassandra.

To be done...

To run this file, use this code (home_credit_data denotes the input dir, quartet denots the cassandra keyspace name):
```
spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 load_csvs_to_cassandra.py home_credit_data quartet
```
