import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import isnull, when, count, col
spark = SparkSession.builder.appName('home credit model training').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.mllib.stat import Statistics

import pandas as pd
import numpy as np

from ETL import ETL, get_dummies_spark

def main():

    ###################################
    # after we read in the ready_train and ready_test, we first on-hot-encode the categorical data
    #ready_train, ready_test = ETL()
    ready_train = spark.read.option("inferSchema", True).csv('/Users/michaelyang/Downloads/all_data/application_train.csv',
                                   header=True)
    train_cat_encoded = get_dummies_spark(ready_train,'SK_ID_CURR','ready_train')
    numerical_feats = [f for f, t in ready_train.dtypes if t != 'string']
    train_num_df = ready_train.select(numerical_feats)
    ready_train = train_cat_encoded.join(train_num_df,on = 'SK_ID_CURR')

    # test_cat_encoded = get_dummies_spark(ready_test, 'SK_ID_CURR', 'ready_test')
    # numerical_feats = [f for f, t in ready_test.dtypes if t != 'string']
    # test_num_df = ready_test.select(numerical_feats)
    # ready_test = test_cat_encoded.join(test_num_df, on='SK_ID_CURR')

    #####################################
    # feature generation
    '''
    CREDIT_INCOME_PERCENT: the percentage of the credit amount relative to a client's income
    ANNUITY_INCOME_PERCENT: the percentage of the loan annuity relative to a client's income
    CREDIT_TERM:  the length of the payment in months (since the annuity is the monthly amount due
    DAYS_EMPLOYED_PERCENT: the percentage of the days employed relative to the client's age
    '''
    ready_train = ready_train.withColumn('CREDIT_INCOME_PERCENT', ready_train.AMT_CREDIT / ready_train.AMT_INCOME_TOTAL)
    ready_train = ready_train.withColumn('ANNUITY_INCOME_PERCENT', ready_train.AMT_ANNUITY/ ready_train.AMT_INCOME_TOTAL)
    ready_train = ready_train.withColumn('CREDIT_TERM', ready_train.AMT_ANNUITY/ ready_train.AMT_CREDIT)

    ######################################
    # remove invalid outliers
    '''through our EDA, we discovered an invalid outlier which is the DAYS_EMPLOYED == 365243 days (approx 100 years).
    we also discovered that over 50,000 data points have that value. So we think this is a strategy that the original
    dataset used to impute the null value. Since we are going to use a tree based model, and it is robust to this kind
    of outliers, we will leave them untreated
    '''

    #######################################
    # remove colinear features
    # tree models are insensitive to redundant features, but feature selection is still necessary.
    '''
    by combining features of all tables together, we end up with 1014 features. Some of the features might be collinear
    to each other. These can decrease the model's availablility to learn, decrease model interpretability, and decrease
     generalization performance on the test set. We want to remove these features.
    '''
    # Threshold for removing correlated variables = 0.9
    threshold = 0.9
    df = ready_train.drop('SK_ID_CURR').drop('TARGET')
    col_names = df.columns
    features = df.rdd.map(lambda row: row[0:])
    corr_mat = Statistics.corr(features, method="pearson")
    corr_df = pd.DataFrame(corr_mat)
    corr_df.index, corr_df.columns = col_names, col_names
    upper = corr_df.where(np.triu(np.ones(corr_df.shape), k=1).astype(np.bool))
    to_drop = [column for column in upper.columns if any(upper[column] > threshold)]

    # drop columns in the to_drop list
    for col in ready_train.columns:
        if col in to_drop:
            ready_train.drop(col)
            #ready_test.drop(col)

    #########################################
    # remove columns with over 75% of missing values
    nullcount = ready_train.select([count(when(isnull(c), c)).alias(c) for c in ready_train.columns]).collect()
    size = ready_train.count()
    threshold = 0.75 * size
    to_drop = []
    for col in ready_train.columns:
        if nullcount[0][col] > threshold:
            to_drop.append(col)

    for col in ready_train.columns:
        if col in to_drop:
            ready_train.drop(col)
            #ready_test.drop(col)

    #########################################
    '''
    We use Wrapper method to reduce the number of features.
    Decision trees often perform well on imbalanced datasets.
    The splitting rules that look at the class variable used in the creation of the trees,
    can force both classes to be addressed.
    '''
    # feature importance, choose from model
    # assembler
    features = list(set(ready_train.columns) - set(['SK_ID_CURR', 'TARGET']))
    feature_assembler = VectorAssembler(inputCols=features, outputCol='features')
    # classifier
    classifier = RandomForestClassifier(labelCol='TARGET', maxBins=60, maxDepth=8)
    # pipeline
    credit_pipeline = Pipeline(stages=[feature_assembler, classifier])
    # fit and see feature importance
    credit_model = credit_pipeline.fit(ready_train.na.fill(-999))
    # get features with zero importance and drop them
    feature_importances = pd.DataFrame({'feature': features, 'importance': credit_model.stages[-1].featureImportances})\
        .sort_values('importance', ascending=False)
    zero_features = list(feature_importances[feature_importances['importance'] == 0.0]['feature'])

    for col in ready_train.columns:
        if col in zero_features:
            ready_train.drop(col)
            #ready_test.drop(col)

    #############################################
    # use cross validation to tune hyperparameter
    # use area under ROC instead of accuracy since we have unbalanced datasets (default metric for binaryclassificationevaluator)
    features = list(set(ready_train.columns) - set(['SK_ID_CURR', 'TARGET']))
    feature_assembler = VectorAssembler(inputCols=features, outputCol='features')
    classifier = GBTClassifier(labelCol='TARGET', maxBins=60)
    pipeline = Pipeline(stages=[feature_assembler, classifier])
    grid = ParamGridBuilder().addGrid(classifier.maxDepth,[5,6,7]).addGrid(classifier.stepSize,[0.05,0.1]).build()
    evaluator = BinaryClassificationEvaluator(labelCol='TARGET')
    cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator, numFolds= 5)
    cvModel = cv.fit(ready_train.fillna(-999))

if __name__ == '__main__':

    pass
