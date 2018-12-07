from sklearn.metrics import roc_curve, auc
from matplotlib import pyplot as plt
import pickle

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

def model_tuning():

    data = spark.read.option("inferSchema", True).csv('/user/honghuiw/prepared_for_tuning/',header=True)

    data = data.fillna(-999999999)

    features = list(set(data.columns) - set(['sk_id_curr', 'target']))

    feature_assembler = VectorAssembler(inputCols=features, outputCol='features')

    data = feature_assembler.transform(data)

    data = data.select('target','features')

    data = data.cache()

    Ndata = data.count()

    #############################################
    # use area under ROC instead of accuracy since we have unbalanced datasets (default metric for binaryclassificationevaluator)


    maxDepthes = [3, 4, 5, 6]
    maxIter = [20, 50, 80]
    stepSize = [0.005, 0.02, 0.16, 0.64]


    for maxI in maxIter:
        for maxD in maxDepthes:
            for stepS in stepSize:
                for valStar in range(0, , )

                print("start tuning for maxI:{} maxD:{} stepS:{}".format(maxI, maxD, stepS))

                gbt = GBTClassifier(labelCol='target', predictionCol='prediction',
                                     maxIter=maxI, maxDepth=maxD, stepSize=stepS)
                                     #maxBins=60, maxIter=maxI, maxDepth=maxd, stepSize=stepS)
                model = gbt.fit(trainD)
                #save the model
                #model.write().overwrite().save("models/maxD{}maxI{}stepS{}".format(maxD, maxI, stepS))

                predic = model.transform(testD)

                predic = predic.select('probability', 'target')

                #predic.show()
                #target |features            |rawPrediction|         probability|prediction|
                #+------+--------------------+-------------+--------------------+----------+
                #|     0|(473,[0,1,2,3,4,5...|   [0.9,-0.9]|[0.85814893509951...|       0.0|

                predic = predic.collect()

                target = [1.0-i.target for i in predic]
                prob = [(i.probability)[0] for i in predic]

                fpr, tpr, thresholds = roc_curve(target, prob)
                roc_auc = auc(fpr, tpr)

                fprtpr = {'fpr':fpr, 'tpr':tpr, 'thresholds':thresholds, 'roc_auc':roc_auc,
                         'maxI':maxI, 'maxD':maxD, 'stepS':stepS}

                with open("rocs/maxI{}maxD{}stepS{}.fprtpr".format(maxI, maxD, stepS), 'wb') as fp:
                    pickle.dump(fprtpr, fp)



if __name__ == '__main__':

    model_tuning()
