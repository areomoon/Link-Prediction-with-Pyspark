import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler,StringIndexer
from pyspark.ml.classification import RandomForestClassifier


print('loading data...')
sc = SparkContext(conf=SparkConf())
spark=SparkSession.builder.master("local").appName("tweet").getOrCreate()

graph_cb = spark.read.csv('../tweet_data/graph_cb_balanced.csv', header=True)
jaccard  = spark.read.csv('../tweet_data/graph_cb_balanced_withPRscore_jaccard.csv', header=True)\
    .select(col('src').alias('id_scr'),col('dst').alias('id_dst'),'jaccard',col('relation').alias('follow'))
pagerank = spark.read.csv('../tweet_data/graph_cb_balanced_withPRscore.csv', header=True).select('relation','pr_score_scr','pr_score_dst')

print('join dataframe...')
df=graph_cb.join(pagerank,pagerank.relation==graph_cb.relation, how='left').select(graph_cb.relation,'id_dst','id_scr','pr_score_scr','pr_score_dst')
df=df.join(jaccard,['id_scr','id_dst'])

print('extract features...')
df=df.select('follow','pr_score_scr','pr_score_dst','jaccard')
train_col=['follow','pr_score_scr','pr_score_dst','jaccard']
for i in train_col:
    df=df.withColumn(i,df[i].cast('float'))

assembler = VectorAssembler(inputCols=['pr_score_scr','pr_score_dst','jaccard'], outputCol="features")
df=assembler.transform(df)
df=StringIndexer(inputCol="follow", outputCol="label").fit(df).transform(df).select('features','label')

print('split train and test dataset')
train_df, test_df = df.randomSplit([0.7, 0.3],seed=0)

print('train Randomforest model')
rf = RandomForestClassifier(numTrees=10, maxDepth=5, labelCol="label", seed=0)
model = rf.fit(train_df)
# http://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html
pass