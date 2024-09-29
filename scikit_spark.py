import sys

import numpy as np
import sklearn
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
import pandas as pd
import pyarrow
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType, ArrayType

# Print the versions of packages, etc.
print(f"python: {sys.version.split()[0]}")
print(f"pyspark: {pyspark.__version__}")
print(f"scikit-learn: {sklearn.__version__}")
# pyspark versions after 2.4.4 should better support pandas and pyarrow versions (https://github.com/apache/spark/pull/24867)
print(f"pandas: {pd.__version__}")
# https://github.com/apache/spark/blob/v2.4.3/python/setup.py#L106
print(f"pyarrow: {pyarrow.__version__}")

# Make some fake data and train a model.
n_samples_test = 100000
n_samples_train = 1000
n_samples_all = n_samples_train + n_samples_test
n_features = 50

X, y = make_classification(n_samples=n_samples_all, n_features=n_features, random_state=123)
X_train, X_test, y_train, y_test = \
    train_test_split(X, y, test_size=n_samples_test, random_state=45)

# Use pandas to put the test data in parquet format to illustrate how to load it up later.
# In real usage, the data might be on S3, Azure Blog Storage, HDFS, etc.
column_names = [f'feature{i}' for i in range(n_features)]
(
    pd.DataFrame(X_test, columns=column_names)
    .reset_index()
    .rename(columns={'index': 'id'})
    .to_parquet('unlabeled_data')
)

# Train a model with scikit-learn
param_grid = {'n_estimators': [100], 'max_depth': [2, 4, None]}
gs_rf = GridSearchCV(
    RandomForestClassifier(random_state=42),
    param_grid=param_grid,
    scoring='roc_auc',
    cv=3
).fit(X_train, y_train)
print('ROC AUC: %.3f' % gs_rf.best_score_)

sc = pyspark.SparkContext(appName="foo")
sqlContext = pyspark.SQLContext(sc)

df_unlabeled = sqlContext.read.parquet('unlabeled_data')
df_unlabeled



import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType, ArrayType

@F.udf(returnType=DoubleType())
def predict_udf(*cols):
    # cols will be a tuple of floats here.
    return float(gs_rf.predict_proba((cols,))[0, 1])

df_pred_a = df_unlabeled.select(
    F.col('id'),
    predict_udf(*column_names).alias('prediction')
)
df_pred_a.take(5)



@F.pandas_udf(returnType=DoubleType())
def predict_pandas_udf(*cols):
    # cols will be a tuple of pandas.Series here.
    X = pd.concat(cols, axis=1)
    return pd.Series(gs_rf.predict_proba(X)[:, 1])

df_pred_b = df_unlabeled.select(
    F.col('id'),
    predict_pandas_udf(*column_names).alias('prediction')
)
df_pred_b.take(5)

@F.pandas_udf(returnType=ArrayType(DoubleType()))
def predict_pandas_udf(*cols):
    X = pd.concat(cols, axis=1)
    return pd.Series(row.tolist() for row in gs_rf.predict_proba(X))

df_pred_multi = (
    df_unlabeled.select(
        F.col('id'),
        predict_pandas_udf(*column_names).alias('predictions')
    )
    # Select each item of the prediction array into its own column.
    .select(
        F.col('id'),
        *[F.col('predictions')[i].alias(f'prediction_{c}')
          for i, c in enumerate(gs_rf.classes_)]
    )
)
df_pred_multi.take(5)


