# spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 load_csvs_to_cassandra.py home_credit_data quartet
import os, sys, re, os.path
import uuid

from datetime import datetime

from cassandra.cluster import Cluster, BatchStatement

from pyspark.sql import SparkSession, functions, types, Row

from schemas import schemas

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


df = spark.read.csv(dpath+fn, schema=schemas[tableName])
def main(input_dir, keyspace_name):

    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spk_cass = SparkSession.builder.appName('load_csvs_to_cassandra') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spk_cass.sparkContext.setLogLevel('WARN')

    tableNamec = re.compile(r"^(.+).csv$")
    for (dpath, dnames, fnames) in os.walk(input_dir):
        for fn in fnames:
            tableName = tableNamec.match(fn)
            if tableName:
                tableName = tableName.group(1) # tableName is fiel name excluding of .csv suffix
                df = spk_cass.read.csv(dpath+fn, schema=schemas[tableName],
                                       header = True)
                df.write.format("org.apache.spark.sql.cassandra") \
                   .options(table=tableName, keyspace=keyspace_name).save()

    break #not into subdirectories



if __name__ == "__main__":
    input_dir = sys.argv[1]
    keyspace_name = sys.argv[2]

    main(input_dir, keyspace_name)

