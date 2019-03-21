import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS


print('loading data...')
# User_id1, User_id2, TimeStamp
sc = SparkContext(conf=SparkConf())
graph_cb_data = sc.textFile('graph_cb.txt')


print('Create train and test data...')
# Prepare training and testing dataset
graph_cb_data = graph_cb_data.map(lambda line: line.split(" ")).map(lambda tokens: (tokens[0],tokens[1],'1')).cache()

training_RDD, validation_RDD, test_RDD = graph_cb_data.randomSplit([7, 2, 1], seed=0)
validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

print('Build ALS model...')
# Modeling-parameter
seed = 5
iterations = 10
regularization_parameter = 0.1

ranks = [25, 30, 50] # best parameter is 25 !
rates = [0, 0, 0]
count = 0
tolerance = 0.02

min_rate = 0
best_rank = -1
best_iteration = -1

threshold=0.5

for rank in ranks:
    model = ALS.train(training_RDD, rank, seed=seed, iterations=iterations,
                      lambda_=regularization_parameter)
    predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
    rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2])))
    rates_and_preds=rates_and_preds.join(predictions)
    accuracy_rate = rates_and_preds.map(lambda r: r[1][0] == float(r[1][1]>threshold)).mean()
    rates[count] = accuracy_rate
    count += 1
    print ('For rank %s the accuracy rate is %s' % (rank, accuracy_rate))
    if accuracy_rate > min_rate:
        min_error = accuracy_rate
        best_rank = rank
print ('The best model was trained with rank %s' % best_rank)