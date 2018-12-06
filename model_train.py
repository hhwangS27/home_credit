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

def model_training():

    ready_train = spark.read.option("inferSchema", True).csv('/user/hwa125/ready_train2/',header=True)

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
    to each other. These can decrease the model's availability to learn, decrease model interpretability, and decrease
     generalization performance on the test set. We want to remove these features.
    '''
    # Threshold for removing correlated variables = 0.8
    threshold = 0.8
    df = ready_train.drop('sk_id_curr').drop('target')
    col_names = df.columns
    features = df.rdd.map(lambda row: row[0:])
    corr_mat = Statistics.corr(features, method="pearson")
    corr_mat = np.abs(corr_mat)
    corr_df = pd.DataFrame(corr_mat)
    corr_df.index, corr_df.columns = col_names, col_names
    upper = corr_df.where(np.triu(np.ones(corr_df.shape), k=1).astype(np.bool))
    to_drop = [column for column in upper.columns if any(abs(upper[column]) > threshold)]
    print('******remove collinear features*********',len(to_drop))

    # drop columns in the to_drop list
    ready_train = ready_train.drop(*to_drop)

    #########################################
    # remove columns with over 75% of missing values
    null_count = ready_train.select([count(when(isnull(c), c)).alias(c) for c in ready_train.columns]).collect()
    size = ready_train.count()
    threshold = 0.75 * size
    to_drop = []
    for col in ready_train.columns:
        if null_count[0][col] > threshold:
            to_drop.append(col)
    print('********remove columns with 75% of missing value************',len(to_drop))

    ready_train = ready_train.drop(*to_drop)

    print('*********number of features left after removing collinear and missing columns*************',len(ready_train.columns))

    #########################################
    '''
    We use Wrapper method to reduce the number of features.
    Decision trees often perform well on imbalanced datasets.
    The splitting rules that look at the class variable used in the creation of the trees,
    can force both classes to be addressed.
    '''
    # feature importance, choose from model
    # assembler
    features = list(set(ready_train.columns) - set(['sk_id_curr', 'target']))
    feature_assembler = VectorAssembler(inputCols=features, outputCol='features')
    # classifier
    classifier = RandomForestClassifier(labelCol='target', maxBins=60, maxDepth=6)
    # pipeline
    credit_pipeline = Pipeline(stages=[feature_assembler, classifier])
    # fit and see feature importance
    credit_model = credit_pipeline.fit(ready_train.na.fill(-999999))
    # get features with zero importance and drop them
    feature_importances = pd.DataFrame({'feature': features, 'importance': credit_model.stages[-1].featureImportances})\
        .sort_values('importance', ascending=False)
    zero_features = list(feature_importances[feature_importances['importance'] == 0.0]['feature'])
    print('*********remove features with zero importance***************',len(zero_features))

    ready_train = ready_train.drop(*zero_features)

    #print('************features left after model selection*****************',ready_train.columns)
    print('************number of features left************', len(ready_train.columns))
    print('************get best model through cross validation*************')

    #############################################
    # use cross validation to tune hyperparameter
    # use area under ROC instead of accuracy since we have unbalanced datasets (default metric for binaryclassificationevaluator)
    features = list(set(ready_train.columns) - set(['sk_id_curr', 'target']))
    feature_assembler = VectorAssembler(inputCols=features, outputCol='features')
    classifier = GBTClassifier(labelCol='target', maxBins=60,stepSize=0.05)
    pipeline = Pipeline(stages=[feature_assembler, classifier])
    #grid = ParamGridBuilder().addGrid(classifier.maxDepth,[4,5]).addGrid(classifier.stepSize,[0.05,0.1]).build()
    grid = ParamGridBuilder().addGrid(classifier.maxDepth, [4, 5]).build()
    evaluator = BinaryClassificationEvaluator(labelCol='target')
    cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator, numFolds= 5)
    cv_model = cv.fit(ready_train.fillna(-999999))
    print('cross validation metrics',cv_model.avgMetrics)
    model = cv_model.bestModel
    # print best model hyperparameters
    print(model.stages[1].extractParamMap())
    #model = pipeline.fit(ready_train.na.fill(-999999))

    return model

if __name__ == '__main__':

    out_dir = sys.argv[1]
    home_credit_model = model_training()
    home_credit_model.save(out_dir)
