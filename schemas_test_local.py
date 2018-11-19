#use csvs in 20LineCSVs


# spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 load_csvs_to_cassandra.py home_credit_data quartet
import os, sys, re, os.path

from pyspark.sql import SparkSession, functions, types, Row

from schemas_ import schemas
from IOYNtoBoolSets import IOtoBool, YNtoBool

def main():
    input_dir = "20LineCSVs/"

    spark = SparkSession.builder.appName('schemas test').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    tableNames = ("application_test", "application_test", "bureau",
                  "bureau_balance", "credit_card_balance",
                  "installments_payments", "POS_CASH_balance",
                  "previous_application")
    #tableNames = ("credit_card_balance",)
    for tableName in tableNames:
        df = spark.read.csv(input_dir+tableName+'.csv', schema=schema,
                               header = True, sep =',')
        print(tableName)
        df.show()


if __name__ == "__main__":
    main()

