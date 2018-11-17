Project Source: [Kaggle](https://www.kaggle.com/c/home-credit-default-risk)

# Add `schemas.py`

In this field, I defined a dict `schemas` who is like this:
```
schemas = {tableName:schema, ...}
```
There is some stuff to be done, like wether the column is nullable and now
every type is set to be `types.StringType()` which needs to be specified.  

Include this code for using of schemas:
```
from schemas import schemas
```

# Create our cassandra keyspace called 'quartet' using this code:
```
CREATE KEYSPACE quartet WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':2};
```

# Add `load_csvs_to_cassandra.py`

In this file, use spark to read csv files and load them into cassandra.

To be done...

To run this file, use this code (home_credit_data denotes the input dir, quartet denots the cassandra keyspace name):
```
spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 load_csvs_to_cassandra.py home_credit_data quartet
```
