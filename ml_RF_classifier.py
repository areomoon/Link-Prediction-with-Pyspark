import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler,StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from sklearn.metrics import f1_score

print('Loading data...')
sc = SparkContext(conf=SparkConf())
spark=SparkSession.builder.master("local").appName("tweet").getOrCreate()

graph_cb = spark.read.csv('tweet_data/graph_cb_balanced.csv', header=True)

jaccard  = spark.read.csv('tweet_data/tweet_jaccard_sim.csv', header=True)\
    .select(col('src').alias('id_scr'),col('dst').alias('id_dst'),'jaccard',col('relation').alias('follow'))

pagerank = spark.read.csv('tweet_data/tweet_pagerank.csv', header=True)\
    .select('relation','pr_score_scr','pr_score_dst')

cosine_sim=spark.read.csv('tweet_data/tweet_cosine_sim.csv', header=True)\
    .select(col('src').alias('id_scr'),col('dst').alias('id_dst'),col('users_similarity').alias('user_sim'),col('hashtag_similarity').alias('hash_sim'))

adamic_sim=spark.read.csv('tweet_data/tweet_adamic_sim.csv', header=True)\
    .select(col('src').alias('id_scr'),col('dst').alias('id_dst'),col('adamic').alias('adamic_sim'),col('relationship'))


print('Join dataframe...')
df=graph_cb.join(pagerank,pagerank.relation==graph_cb.relation, how='left')\
    .select(graph_cb.relation,'id_dst','id_scr','pr_score_scr','pr_score_dst')
df=df.join(jaccard,['id_scr','id_dst'])
df=df.join(cosine_sim,['id_scr','id_dst'])
df=df.join(adamic_sim,['id_scr','id_dst'])

print('Extract features...')
df=df.select('follow','pr_score_scr','pr_score_dst','jaccard','adamic_sim','user_sim','hash_sim') #
train_col=['follow','pr_score_scr','pr_score_dst','jaccard','adamic_sim','user_sim','hash_sim'] #
for i in train_col:
    df=df.withColumn(i,df[i].cast('float'))

assembler = VectorAssembler(inputCols=['pr_score_scr','pr_score_dst','jaccard','adamic_sim','user_sim','hash_sim'], outputCol="features") #
df=assembler.transform(df)
df = df.withColumnRenamed('follow','label').select('features','label')
print('Split train and test dataset...')
train_df, test_df = df.randomSplit([0.7, 0.3],seed=0)

print('Train RandomForest model...')
rf = RandomForestClassifier(numTrees=10, maxDepth=5, labelCol="label", seed=0)
model = rf.fit(train_df)

print('Feature Importance')
featureImportances=model.featureImportances.values
print([(train_col[i+1],featureImportances[i]) for i in range(len(featureImportances))])

print('Evaluation...')
prediction=model.transform(test_df).select('label','probability','prediction')
evaluator = BinaryClassificationEvaluator(rawPredictionCol="probability")
print('Area Under ROC:  {:.4f}'.format(evaluator.evaluate(prediction)))
pd_pred=prediction.toPandas()
sc.stop()
f1=f1_score(pd_pred['label'],pd_pred['prediction'])
print('F1_Score:  {:.4f}'.format(f1))

