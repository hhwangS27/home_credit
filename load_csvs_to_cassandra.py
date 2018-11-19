# spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 load_csvs_to_cassandra.py home_credit_data quartet
import os, sys, re, os.path

from cassandra.cluster import Cluster, BatchStatement

from pyspark.sql import SparkSession, functions, types, Row

from schemas import schemas
from IOYNtoBoolSets import IOtoBool, YNtoBool

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

#df = spark.read.csv(dpath+fn, schema=schemas[tableName])

def main(input_dir, keyspace_name):

    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spk_cass = SparkSession.builder.appName('load_csvs_to_cassandra') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spk_cass.sparkContext.setLogLevel('WARN')

    tableNames = ("application_test", "application_test", "bureau",
                  "bureau_balance", "credit_card_balance",
                  "installments_payments", "POS_CASH_balance",
                  "previous_application")
    for tableName in tableNames:
        df = spk_cass.read.csv(input_dir+tableName+'.csv', schema=schemas[tableName],
                               header = True, sep =',')
        df.show()
        #if tableName in IOtoBool:
        #    for c in IOtoBool[tableName]:
        #        df = df.withColumn('_'+c, df[c]==1).drop(c)
        #        df = df.withColumnRenamed('_'+c, c)

        #if tableName in YNtoBool:
        #    for c in YNtoBool[tableName]:
        #        df = df.withColumn('_'+c, df[c]=='Y').drop(c)
        #        df = df.withColumnRenamed('_'+c, c)

        #df.write.format("org.apache.spark.sql.cassandra") \
        #   .options(table=tableName, keyspace=keyspace_name).save()


if __name__ == "__main__":
    input_dir = sys.argv[1]
    keyspace_name = sys.argv[2]

    main(input_dir, keyspace_name)

