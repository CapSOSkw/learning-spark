from pyspark import SparkContext

# Initializing
sc = SparkContext(master='local', appName='pair RDD')

data1 = sc.parallelize([1,1,1,2,3,3,4,4,4,4,4,5,5]).map(lambda x: (x, 1))  # create a RDD
data1.persist()  # save it to RAM for multiple uses

data2 = [(i, idx) for idx, i in enumerate([1,1,1,2,3,3,4,4,4,4,4,5,5])]
# data2 = [(1, 0), (1, 1), (1, 2), (2, 3), (3, 4), (3, 5), (4, 6), (4, 7), (4, 8), (4, 9), (4, 10), (5, 11), (5, 12)]
data2 = sc.parallelize(data2)
data2.persist()

'''
Pair RDD Transformations:
1. reduceByKey()
2. groupByKey()
3. combineByKey()
4. mapValues()
5. flatMapValues()
6. keys()
7. values()
8. sortByKey()
'''

reduceByKeyExample = data1.reduceByKey(lambda x,y: x+y).collect()
print('reduceByKey function example: ', reduceByKeyExample)

groupByKeyExample = data2.groupByKey()
print('groupByKey function example: ', groupByKeyExample.collect())
print('groupByKey function example: ', groupByKeyExample.map(lambda x: (x[0], list(x[1]))).collect())

sample = sc.parallelize([('pandas', 12), ('pandas', 4), ('piggy', 10), ('piggy', 20), ('kitty', 8)])
combineByKeyExample = sample.combineByKey(createCombiner=lambda x: (x, 1),
                                          mergeValue=lambda x, y: (x[0]+y, x[1]+1),
                                          mergeCombiners=lambda x,y: (x[0]+y[0], x[1]+y[1]))
# http://abshinn.github.io/python/apache-spark/2014/10/11/using-combinebykey-in-apache-spark/
average_combineByKeyExample1 = combineByKeyExample.mapValues(lambda x: x[0]/x[1])
average_combineByKeyExample2 = combineByKeyExample.map(lambda x: (x[0], x[1][0]/x[1][1]))
print('combineByKey function example: ', combineByKeyExample.collect())
print('Average1: ', average_combineByKeyExample1.collect())
print('Average2: ', average_combineByKeyExample2.collect())


mapValues = data1.mapValues(lambda x: x+10)
print('mapValues function example: ', mapValues.collect())

flatMapValuesExample = groupByKeyExample.map(lambda x: (x[0], list(x[1]))).flatMapValues(lambda x: [i*2 for i in x])
print('flatMapValues function example: ', flatMapValuesExample.collect())

keysExample = data1.keys()
print('keys function example: ', keysExample.collect())

valuesExample = data2.values()
print('values function example: ', valuesExample.collect())

sortByKeyExample = data2.sortByKey(ascending=False)
print('sortByKey function example: ', sortByKeyExample.collect())


'''
Two pairs RDD transformations
1. subtractByKey()
2. join()
3. rightOuterJoin()
4. leftOuterJoin()
5. cogroup()
'''

rdd = sc.parallelize([(1, 2), (3, 4), (3, 6)])
other = sc.parallelize([(3, 9), (5, 10)])
rdd.persist()
other.persist()

subtractByKeyExample = rdd.subtractByKey(other)
print('subtractByKey function example: ', subtractByKeyExample.collect())

joinExample = rdd.join(other)
print('join function example: ', joinExample.collect())

rightOuterJoinExample = rdd.rightOuterJoin(other)   # the keys in the right RDD must exist
print('rightOuterJoin function example: ', rightOuterJoinExample.collect())

leftOuterJoinExample = rdd.leftOuterJoin(other)
print('leftOterJoin function example: ', leftOuterJoinExample.collect())

cogroupExample = rdd.cogroup(other)
print('cogroup function example: ', cogroupExample.map(lambda x: (x[0], [list(i) for i in x[1]])).collect())



'''
Two pairs RDD actions
1. countByKey()
2. collectAsMap()
3. lookup()
'''

countByKeyExample = rdd.countByKey()
print('countByKey function example: ', countByKeyExample)

collectAsMapExample = rdd.collectAsMap()
print('collectAsMap function example: ', collectAsMapExample)

lookupExample = rdd.lookup(key=3)
print('lookup function example: ', lookupExample)













