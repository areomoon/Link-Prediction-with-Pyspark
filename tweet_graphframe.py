# https://docs.databricks.com/spark/latest/graph-analysis/graphframes/user-guide-python.html#graphframes-user-guide-python

import time
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,Row

from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *

print('loading data...')

sc = SparkContext(conf=SparkConf())
graph_cb_data = sc.textFile('tweet_data/graph_cb.txt').map(lambda line: line.split(" "))\
    .map(lambda tokens: Row(src=str(tokens[0]),dst=str(tokens[1]),relationship=str('follow')))

user_list = sc.textFile('tweet_data/user_list2.txt').map(lambda line: line.split("\t"))\
    .map(lambda tokens: Row(origin_id=str(tokens[0]),id=str(tokens[1])))

user_map = sc.textFile('tweet_data/user_map.txt').map(lambda line: line.split(" "))\
    .map(lambda tokens: Row(origin_id=str(tokens[0]),name=str(tokens[1])))


spark=SparkSession.builder.master("local").appName("tweet").getOrCreate()

graph_cb=spark.createDataFrame(graph_cb_data)
user_list=spark.createDataFrame(user_list)
user_map=spark.createDataFrame(user_map)
user_table=user_list.join(user_map,user_map.origin_id==user_list.origin_id).select('id','name')

print('Create graphframe...')

g=GraphFrame(user_table, graph_cb)

pass