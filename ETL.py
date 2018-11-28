import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('cassandra read tables and etl') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext


def get_dummies_spark(df, group_var):
    # same as get_dummies in pandas. since it's not pre-defined in Spark, we defined one by ourselves
    pivot_cols = [f for f, t in df.dtypes if t == 'string']
    keys = pivot_cols + [group_var]

    before = df.select(keys)

    #function to recursively join a list of dataframes
    def join_all(dfs, keys):
        if len(dfs) > 1:
            return dfs[0].join(join_all(dfs[1:], keys), on=keys, how='inner')
        else:
            return dfs[0]

    dfs = []
    combined = []
    for pivot_col in pivot_cols:
        pivotDF = before.groupBy(keys).pivot(pivot_col).count()
        new_names = pivotDF.columns[:len(keys)] + ["e_{0}_{1}".format(pivot_col, c) for c in
                                                   pivotDF.columns[len(keys):]]
        df = pivotDF.toDF(*new_names).fillna(0)
        combined.append(df)

    encoded = join_all(combined, keys)

    # drop its original columns
    for col in pivot_cols:
        encoded = encoded.drop(col)

    return encoded

def agg_numeric(df, group_var):
    """Aggregates the numeric values in a dataframe. This can
    be used to create features for each instance of the grouping variable.
    """

    # Remove id variables other than grouping variable
    for col in df.columns:
        if col != group_var and 'SK_ID' in col:
            df = df.drop(col)

    numerical_feats = [f for f, t in df.dtypes if t != 'string']
    numeric_df = df.select(numerical_feats)

    # Group by the specified variable and calculate the statistics
    # = numeric_df.groupBy(group_var).agg(['count', 'mean', 'max', 'min', 'sum']).reset_index()
    count = numeric_df.groupBy(group_var).count()
    means = numeric_df.groupBy(group_var).avg().drop('avg(%s)' % (group_var))
    maxs = numeric_df.groupBy(group_var).max().drop('max(%s)' % (group_var))
    mins = numeric_df.groupBy(group_var).min().drop('min(%s)' % (group_var))
    sums = numeric_df.groupBy(group_var).sum().drop('sum(%s)' % (group_var))
    joined = count.join(means, count[group_var] == means[group_var]).drop(means[group_var])
    joined1 = joined.join(maxs, joined[group_var] == maxs[group_var]).drop(maxs[group_var])
    joined2 = joined1.join(mins, joined1[group_var] == mins[group_var]).drop(mins[group_var])
    num_aggregated = joined2.join(sums, joined1[group_var] == sums[group_var]).drop(sums[group_var])

    return num_aggregated

def count_categorical(df, group_var):
    """Computes counts for each observation
    of `group_var` of each unique category in every categorical variable
    """

    # encode the categorical columns
    encoded_df = get_dummies(df, group_var)

    # Groupby the group var and calculate the sum and mean
    categorical_encoded = encoded_df.groupBy(group_var).sum().drop('sum(%s)' % (group_var))

    return categorical_encoded

def etl():

    #########################################################
    # load data from cassandra tables to dataframes

    application_train = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='application_train', keyspace='quartet').option("inferSchema", True).load().drop('uuid')
    application_test = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='application_test', keyspace='quartet').option("inferSchema", True).load().drop('uuid')
    bureau_balance = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='bureau_balance', keyspace='quartet').option("inferSchema", True).load().drop('uuid')
    previous_application = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='previous_application', keyspace='quartet').option("inferSchema", True).load().drop('uuid')
    installments_payments = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='installments_payments', keyspace='quartet').option("inferSchema", True).load().drop('uuid')
    credit_card_balance = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='credit_card_balance', keyspace='quartet').option("inferSchema", True).load().drop('uuid')
    bureau = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='bureau', keyspace='quartet').option("inferSchema", True).load().drop('uuid')
    POS_CASH_balance = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='POS_CASH_balance', keyspace='quartet').option("inferSchema", True).load().drop('uuid')

    #########################################################
    '''deal with bureau datasets, bureau and bureau_balance
    one client may have many other loans at other financial institutions. We need to aggregate them together.
    '''

    # ------ bureau_balance.csv -------
    # get aggregated values on numerical columns
    bureau_balance_num = agg_numeric(bureau_balance, 'SK_ID_BUREAU')
    # aggregate the categorical features, we calculate value counts of each category within each categorical variable.
    bureau_balance_cat = count_categorical(bureau_balance, 'SK_ID_BUREAU')
    # join above 2 tables together, and get one entry for each loan file (i.e. for each bureau id)
    bureau_balance_total = bureau_balance_num.join(bureau_balance_cat, bureau_balance_num.SK_ID_BUREAU == bureau_balance_cat.SK_ID_BUREAU)\
        .drop(bureau_balance_cat.SK_ID_BUREAU)

    # ------ bureau.csv ------
    '''we first join the aggregated bureau_balance data back to the bureau.csv
    bureau balance has 817395 bureau ids, while bureau has 1716428 bureau ids,
    we use left outer join to keep all data of bureau table since it has current id,
    and we'll use it later to join back to application table'''
    # join balance data back to bureau.csv
    bureau_with_balance = bureau.join(bureau_balance_total, bureau.SK_ID_BUREAU == bureau_balance_total.SK_ID_BUREAU, how= 'left_outer')
    # count other loans associated with current id and get aggregated values on numerical columns
    bureau_num = agg_numeric(bureau_with_balance, 'SK_ID_CURR')
    # aggregate the categorical features, we calculate value counts of each category within each categorical variable.
    bureau_cat = count_categorical(bureau_with_balance, 'SK_ID_CURR')
    # join above 2 tables together, and get one entry for each client
    client_bureau_data_encoded = bureau_num.join(bureau_cat, bureau_num.SK_ID_CURR == bureau_cat.SK_ID_CURR).drop(bureau_cat.SK_ID_CURR)

    ###########################################################
    '''deal with previous datasets, which are credit_card_balance, installments_payments, previous application and POS_CASH_BALANCE
    one current client might have many previous application files with HOME-CREDIT. We need to aggregate them together
    '''

    '''for the following three tables, each table has multiple SK_ID_PREV.
    This is because these tables record monthly data for each previous application
    So we aggregate by SK_ID_PREV first'''

    # -------- credit_card_balance.csv ----------
    cc_num = agg_numeric(credit_card_balance, 'SK_ID_PREV')
    cc_cat = count_categorical(credit_card_balance,'SK_ID_PREV')
    cc_total = cc_num.join(cc_cat,cc_num.SK_ID_PREV == cc_cat.SK_ID_PREV).drop(cc_cat.SK_ID_PREV)

    # -------- installments_payments.csv -----------
    installment_num = agg_numeric(installments_payments,'SK_ID_PREV')
    installment_cat = count_categorical(installments_payments,'SK_ID_PREV')
    installment_total = installment_num.join(installment_cat, installment_num.SK_ID_PREV == installment_cat.SK_ID_PREV).drop(installment_cat.SK_ID_PREV)

    # -------- POS_CASH_balance.csv --------------
    pos_cash_num = agg_numeric(POS_CASH_balance, 'SK_ID_PREV')
    pos_cash_cat = count_categorical(POS_CASH_balance, 'SK_ID_PREV')
    pos_cash_total = pos_cash_num.join(pos_cash_cat, pos_cash_num.SK_ID_PREV == pos_cash_cat.SK_ID_PREV).drop(pos_cash_cat.SK_ID_PREV)

    # -------- previous_application.csv ------------
    ''' we first join above 3 dataframes to previous_application, and now it's time to aggregate by current id.
    then it will be ready to be joined back to current application table'''
    # join above 3 dataframes to previous_application.csv
    previous_application = previous_application.join(cc_total, previous_application.SK_ID_PREV == cc_total.SK_ID_PREV,
                                                     how = 'left_outer').drop(cc_total.SK_ID_PREV)
    previous_application = previous_application.join(installment_total,previous_application.SK_ID_PREV == installment_total.SK_ID_PREV,
                                                     how = 'left_outer').drop(installment_total.SK_ID_PREV)
    previous_application = previous_application.join(pos_cash_total, previous_application.SK_ID_PREV == pos_cash_total.SK_ID_PREV,
                                                     how = 'left_outer').drop(pos_cash_total.SK_ID_PREV)
    # aggregate numerical & categorical
    previous_num = agg_numeric(previous_application,'SK_ID_CURR')
    previous_cat = count_categorical(previous_application, 'SK_ID_CURR')
    # get one entry for each client for the previous application data
    client_previous_data_encoded = previous_num.join(previous_cat, previous_num.SK_ID_CURR == previous_cat.SK_ID_CURR).drop(previous_cat.SK_ID_CURR)

    #################################################################
    '''now we have aggregated bureau data and previous application data for each current client,
    we can join these 2 tables back to application train and application test to get the 
    ready-to-use training and testing sets for our ML model
    '''
    # we use left outer join because some application data may not have previous or bureau data, and we still need to keep those
    # training
    application_train = application_train.join(client_previous_data_encoded,
                                               application_train.SK_ID_CURR == client_previous_data_encoded.SK_ID_CURR,
                                               how = 'left_outer').drop(client_previous_data_encoded.SK_ID_CURR)
    ready_train = application_train.join(client_bureau_data_encoded,
                                         application_train.SK_ID_CURR == client_bureau_data_encoded.SK_ID_CURR,
                                         how = 'left_outer').drop(client_bureau_data_encoded.SK_ID_CURR)

    # testing
    application_test = application_test.join(client_previous_data_encoded,
                                               application_test.SK_ID_CURR == client_previous_data_encoded.SK_ID_CURR,
                                               how='left_outer').drop(client_previous_data_encoded.SK_ID_CURR)
    ready_test = application_test.join(client_bureau_data_encoded,
                                         application_test.SK_ID_CURR == client_bureau_data_encoded.SK_ID_CURR,
                                         how='left_outer').drop(client_bureau_data_encoded.SK_ID_CURR)

    return ready_train, ready_test


    # app_train_domain['CREDIT_INCOME_PERCENT'] = app_train_domain['AMT_CREDIT'] / app_train_domain['AMT_INCOME_TOTAL']
    # app_train_domain['ANNUITY_INCOME_PERCENT'] = app_train_domain['AMT_ANNUITY'] / app_train_domain['AMT_INCOME_TOTAL']
    # app_train_domain['CREDIT_TERM'] = app_train_domain['AMT_ANNUITY'] / app_train_domain['AMT_CREDIT']
    # app_train_domain['DAYS_EMPLOYED_PERCENT'] = app_train_domain['DAYS_EMPLOYED'] / app_train_domain['DAYS_BIRTH']