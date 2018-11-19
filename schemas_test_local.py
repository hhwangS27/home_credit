#use csvs in 20LineCSVs


# spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 load_csvs_to_cassandra.py home_credit_data quartet
import os, sys, re, os.path

from pyspark.sql import SparkSession, functions, types, Row

from schemas import schemas
from IOYNtoBoolSets import IOtoBool, YNtoBool


schema = types.StructType([
                     types.StructField('sk_id_prev', types.LongType(), False),
                     types.StructField('sk_id_curr', types.LongType(), False),
                     types.StructField('months_balance', types.ShortType(), True),
                     types.StructField('amt_balance', types.DoubleType(), True),
                     types.StructField('amt_credit_limit_actual', types.DoubleType(), True),
                     types.StructField('amt_drawings_atm_current', types.DoubleType(), True),
                     types.StructField('amt_drawings_current', types.DoubleType(), True),
                     types.StructField('amt_drawings_other_current', types.DoubleType(), True),
                     types.StructField('amt_drawings_pos_current', types.DoubleType(), True),
                     types.StructField('amt_inst_min_regularity', types.DoubleType(), True),
                     types.StructField('amt_payment_current', types.DoubleType(), True),
                     types.StructField('amt_payment_total_current', types.DoubleType(), True),
                     types.StructField('amt_receivable_principal', types.DoubleType(), True),
                     types.StructField('amt_recivable', types.DoubleType(), True),
                     types.StructField('amt_total_receivable', types.DoubleType(), True),
                     types.StructField('cnt_drawings_atm_current', types.IntegerType(), True),
                     types.StructField('cnt_drawings_current', types.IntegerType(), True),
                     types.StructField('cnt_drawings_other_current', types.IntegerType(), True),
                     types.StructField('cnt_drawings_pos_current', types.IntegerType(), True),
                     types.StructField('cnt_instalment_mature_cum', types.IntegerType(), True),
                     types.StructField('name_contract_status', types.StringType(), True),
                     types.StructField('sk_dpd', types.ShortType(), True),
                     types.StructField('sk_dpd_def', types.ShortType(), True)
                ])

def main():
    input_dir = "20LineCSVs/"

    spark = SparkSession.builder.appName('schemas test').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    #tableNames = ("application_test", "application_test", "bureau",
    #              "bureau_balance", "credit_card_balance",
    #              "installments_payments", "POS_CASH_balance",
    #              "previous_application")
    tableNames = ("credit_card_balance",)
    for tableName in tableNames:
        df = spark.read.csv(input_dir+tableName+'.csv', schema=schema,
                               header = True, sep =',')
        print(tableName)
        df.show()


if __name__ == "__main__":
    main()

