import findspark
findspark.init()
from pyspark import SparkConf
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
import os
import re
import ast
import  pandas as pd

def jaccard_cal(set_1, set_2):
   if (set_1 is not None) and (set_2 is not None):
      set_1 = set(tuple(set_1))
      set_2 = set(tuple(set_2))
      n = len(set_1.intersection(set_2))
      value = n / float(len(set_1) + len(set_2) - n)
   else:
      value = 0
   return value


def get_jaccard(filepath):
   conf = SparkConf()
   sc = SparkContext(conf=conf)
   spark=SparkSession.builder.master("local").appName("tweet_jaccard").getOrCreate()

   graph = sc.textFile(filepath) # import data as rdd
   rdd = graph.map(lambda x: re.sub(r'[\D]', " ", x))
   header=rdd.first()
   rdd = rdd.filter(lambda x: x != header)
   rdd = rdd.map(lambda x: re.split('\s+', x)).map(lambda x: (int(x[1]), int(x[2]), int(x[3])))

   relation_df=rdd.map(lambda x: Row(src=x[0],dst=x[1],relationship=x[2]))
   relation_df=spark.createDataFrame(relation_df)

   rdd2 = rdd.filter(lambda x: x[2]==1).map(lambda x: (int(x[0]), int(x[1])))
   rdd2 = rdd2.groupByKey().mapValues(lambda x: list(x))
   rdd2 = rdd2.map(lambda x :Row(usr=x[0],frd=x[1]))

   friend_df=spark.createDataFrame(rdd2)
   relation_df=relation_df.join(friend_df, relation_df.src==friend_df.usr, how='left').select('src',col('frd').alias('frd_src'),'dst','relationship')
   relation_df=relation_df.join(friend_df, relation_df.dst==friend_df.usr, how='left').select('src','dst','frd_src',col('frd').alias('frd_dst'),'relationship')
   relation_df=relation_df.rdd.map(list)
   # relation_df=relation_df.map(lambda x :[x[0],x[1],float(jaccard_cal(list(x[2]),list(x[3]))),x[4]])

   save_name = os.path.splitext(os.path.basename(filepath))[0] + '_jaccard'
   save_file = os.path.dirname(os.path.realpath(filepath))
   save_name = os.path.join(save_file, save_name)
   relation_df.saveAsTextFile(save_name)
   sc.stop()

   dir_path = save_name
   files = [re.findall('part-[0-9]+', x) for x in os.listdir(dir_path)]
   files = [subfile for file in files for subfile in file]

   df = []
   for file in files:
      for line in open(os.path.join(dir_path, file), 'r'):
         df.append(line.strip())

   df = pd.Series(df).apply(ast.literal_eval)
   df = pd.DataFrame(df.values.tolist(), columns=['src', 'dst', 'frd_src', 'frd_dst', 'relation'])
   df['jaccard'] = df.apply(lambda x: jaccard_cal(x['frd_src'], x['frd_dst']), axis=1)
   save_name = save_name+'.csv'
   df=df[['src', 'dst','jaccard', 'relation']]
   df.to_csv(save_name)
   return save_name

