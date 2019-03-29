import findspark
findspark.init()
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import udf,col,lit,split,regexp_replace
from pyspark.sql.types import FloatType
from graphframes import *
from collections import Counter
import networkx as nx
import pandas as pd
import os
import re
import ast
import math



def cosine_c(a, b):
    if a == None or b == None or a == [''] or b == ['']:
        return 0.0
    else:
        vec1 = Counter(a)
        vec2 = Counter(b)
        intersection = set(vec1.keys()) & set(vec2.keys())
        numerator = sum([vec1[x] * vec2[x] for x in intersection])
        sum1 = sum([vec1[x]**2 for x in vec1.keys()])
        sum2 = sum([vec2[x]**2 for x in vec2.keys()])
        denominator = math.sqrt(sum1) * math.sqrt(sum2)

        if not denominator:
            return 0.0
        else:
            return float(numerator) / denominator

def get_cosine(filepath):
    sc = SparkContext(conf=SparkConf())
    spark = SparkSession.builder.master("local").appName("tweet").getOrCreate()

    graph_cb_data = sc.textFile('tweet_data/graph_cb.txt').map(lambda line: line.split(" ")) \
        .map(lambda tokens: Row(src=str(tokens[0]), dst=str(tokens[1]), relationship=str('follow')))
    graph_cb = spark.createDataFrame(graph_cb_data)
    user_map = sc.textFile('tweet_data/user_map.txt').map(lambda line: line.split(" ")) \
        .map(lambda tokens: Row(origin_id=str(tokens[0]), name=str(tokens[1])))
    user_map = spark.createDataFrame(user_map)
    user_list = sc.textFile('tweet_data/user_list.txt').map(lambda line: line.split("\t")) \
        .map(lambda tokens: Row(origin_id=str(tokens[0]), id=str(tokens[1])))
    user_list = spark.createDataFrame(user_list)
    user_table = user_list.join(user_map, user_map.origin_id == user_list.origin_id).select('id', 'name')

    data = sc.textFile(filepath)
    header = data.first()
    data = data.filter(lambda x: x != header).map(lambda x: x.split(','))
    username = data.map(lambda x: (x[1], x[2]))
    username = username.mapValues(lambda x: re.split('\s*', x.strip())).mapValues(lambda x: ','.join(x))
    hashtag = data.map(lambda x: (x[1], x[3]))
    hashtag = hashtag.mapValues(lambda x: re.split('\s*', x.strip())).mapValues(lambda x: ','.join(x))
    username = username.toDF(["src", "users"]).withColumn("users", split(regexp_replace("users", " ", ""), ','))
    hashtag = hashtag.toDF(["src", "hashtag"]).withColumn("hashtag",split(regexp_replace("hashtag", " ", ""), ','))
    tweet = hashtag.join(username, username.src == hashtag.src).select(hashtag.src, 'hashtag', 'users')

    tweet_map = user_table.join(tweet, user_table.name == tweet.src, how='left').drop(tweet.src).drop(user_table.name)
    src = graph_cb.join(tweet_map.withColumnRenamed('hashtag', 'src_hashtag') \
                        .withColumnRenamed('users', 'src_users'), tweet_map.id == graph_cb.src, how='left').drop(
        'relationship').drop('id')
    full_data = src.join(tweet_map.withColumnRenamed('hashtag', 'dst_hashtag') \
                         .withColumnRenamed('users', 'dst_users'), tweet_map.id == src.dst, how='left').drop('id').drop(
        'relationship')
    cosine_udfc = udf(lambda a, b: cosine_c(a, b), FloatType())
    users_similarity = full_data.withColumn('users_similarity', cosine_udfc(col('src_users'), col('dst_users')))
    similarity = users_similarity.withColumn('hashtag_similarity', cosine_udfc(col('src_hashtag'), col('dst_hashtag'))) \
        .select(['src', 'dst', 'users_similarity', 'hashtag_similarity'])
    output = similarity.toPandas()
    save_file = os.path.dirname(os.path.realpath(filepath))
    save_name = os.path.join(save_file,'tweet_cosine_sim.csv')
    output.to_csv(save_name)
    sc.stop


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

   df = [line.strip() for file in files for line in open(os.path.join(dir_path, file), 'r')]
   df = pd.Series(df).apply(ast.literal_eval)
   df = pd.DataFrame(df.values.tolist(), columns=['src', 'dst', 'frd_src', 'frd_dst', 'relation'])
   df['jaccard'] = df.apply(lambda x: jaccard_cal(x['frd_src'], x['frd_dst']), axis=1)
   save_name = save_name+'.csv'
   df=df[['src', 'dst','jaccard', 'relation']]
   df.to_csv(save_name)
   return save_name



def get_pagerank(filepath,alpha=0.9,save_file=True):
    D = nx.DiGraph()
    new_graph_df=pd.read_csv(filepath)
    new_graph_df_with_rl=new_graph_df[new_graph_df['follow']==1]
    nodes_uniq = set(pd.concat([new_graph_df['id_scr'], new_graph_df['id_dst']]))

    D.add_nodes_from(nodes_uniq)
    relationship=[tuple(map(int,re.findall('[0-9]+',x))) for x in new_graph_df_with_rl['relation']]
    D.add_edges_from(relationship)

    # Calculate pagerank of the nodes
    pr = nx.pagerank(D, alpha=alpha)

    pr_score = pd.DataFrame.from_dict(pr, orient='index').reset_index()
    pr_score.columns=['id', 'pr_score']

    pr_df=pd.merge(new_graph_df,pr_score, left_on=['id_scr'], right_on=['id'], how='left')
    pr_df.rename(columns = {'pr_score':'pr_score_scr'},inplace=True)
    pr_df=pd.merge(pr_df,pr_score,left_on=['id_dst'], right_on=['id'], how='left')
    pr_df.rename(columns = {'pr_score':'pr_score_dst'},inplace=True)
    pr_df=pr_df[['relation','follow','pr_score_scr','pr_score_dst']]

    if save_file:
        save_name = os.path.splitext(os.path.basename(filepath))[0] + '_withPRscore' + '.csv'
        save_file = os.path.dirname(os.path.realpath(filepath))
        save_name = os.path.join(save_file, save_name)
        pr_df.to_csv(save_name)
        print('Save pr_score file in url: '+ save_name)
    return D,pr,pr_df


def get_adamic(filepath):
    D,pr,pr_df = get_pagerank(filepath,save_file=False)
    H = D.to_undirected()
    adm = nx.adamic_adar_index(H, ebunch=pr_df["relation"])
    # adm_output=[(item[0],item[1],item[2]) for item in adm ]
    pass


def get_IPA_cluster(filepath, max_iter=5,save_file=True):
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    graph_cb_data = sc.textFile('twitter_data/graph_cb.txt').map(lambda line: line.split(" ")) \
        .map(lambda tokens: Row(src=str(tokens[0]), dst=str(tokens[1]), relationship=str('follow')))

    user_list = sc.textFile('twitter_data/user_list.txt').map(lambda line: line.split("\t")) \
        .map(lambda tokens: Row(origin_id=str(tokens[0]), id=str(tokens[1])))

    user_map = sc.textFile('twitter_data/user_map.txt').map(lambda line: line.split(" ")) \
        .map(lambda tokens: Row(origin_id=str(tokens[0]), name=str(tokens[1])))

    spark = SparkSession.builder.master("local").appName("tweet").getOrCreate()

    graph_cb = spark.createDataFrame(graph_cb_data)
    user_list = spark.createDataFrame(user_list)
    user_map = spark.createDataFrame(user_map)
    user_table = user_list.join(user_map, user_map.origin_id == user_list.origin_id).select('id', 'name')

    # Create Dataframe:
    g = GraphFrame(user_table, graph_cb)

    # Run IPA with maximum 5 iterations:（tuned parameter）
    communities = g.labelPropagation(maxIter=max_iter)
    labelled_user = communities.persist()

    # Read in relation scr and dst dataframe to join:
    rdd = sc.textFile(filepath, 4)
    rdd = rdd.map(lambda x: re.sub(r'[\D]', " ", x))
    header = rdd.first()
    rdd = rdd.filter(lambda x: x != header)
    rdd = rdd.map(lambda x: re.split('\s+', x)).map(lambda x: (int(x[3]), int(x[2]), (int(x[4]), int(x[5]))))

    relation_df = rdd.map(lambda x: Row(src=x[0], dst=x[1], relationship=x[2]))
    relation_df = spark.createDataFrame(relation_df)

    relation_df = relation_df.join(labelled_user, relation_df.src == labelled_user.id, how='left').select('src', col('label').alias('cluster_src'), 'dst', 'relationship')
    relation_df = relation_df.join(labelled_user, relation_df.dst == labelled_user.id, how='left').select('src', 'dst','cluster_src',col('label').alias('cluster_dst'),'relationship')
    relation_df = relation_df.drop(relation_df.relationship)

    if save_file:
        save_file = os.path.dirname(os.path.realpath(filepath))
        save_name = os.path.join(save_file, 'clustering_id.csv')
        relation_df.coalesce(1).write.csv(save_name)