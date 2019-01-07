from pyspark import SparkContext

# Initializing
sc = SparkContext(master='local', appName='RDD')

# Parallelized Collections

data = [1,2,3,4,5]
distributed_dataset = sc.parallelize(data, numSlices=5)
'''
The parameter "numSlices" is the partition that users pre-define.
Typically, 2-4 partitions for each CPU in the cluster.
Spark tries to set the value of partitions automatically.
'''
print(distributed_dataset, '\n', type(distributed_dataset))
print(distributed_dataset.collect())   # Return a list that contains ALL of the elements in this RDD.
print(f'The number of partitions of RDD is {distributed_dataset.getNumPartitions()}')


# External Datasets

distributed_file = sc.textFile('./files/README.md')
print('First element: ', distributed_file.first())  # Return the first element in the RDD
print('Number of elements: ', distributed_file.count())  # Return the number of elements in the RDD
print(distributed_file.collect())


# RDD Transformations and Actions
'''
Transformation: Create a new dataset from an existing one. Return a new RDD.
Action: Return other types of datasets
'''

'''
Transformation:
1. map()
2. flatMap()
3. filter()
4. distinct()
5. sample(withReplacement, fraction, [seed])
6. union()
7. intersection()
8. subtract()
9. cartesian()
10. repartition()
11. coalesce()
'''

data1 = sc.parallelize([1,2,3,4,5,5], 2)
data2 = sc.parallelize([7,6,5,4,3])

data1.persist()  # save RDD into RAM for multiple uses
data2.persist()

# In Python, persist() is the same to the cache(), which is "MEMORY_ONLY" level.
# Use persist() only if other storage level is required.

mapExample = data1.map(lambda x: x+1)
print('map function example: ', mapExample.collect())

flatMapExample = data1.flatMap(lambda x: range(x, 5))   # iterate each element to 4
print('flatMap function example: ', flatMapExample.collect())

filterExample = data1.filter(lambda x: x <= 3)
print('filter function example: ', filterExample.collect())

distinctExample = data1.distinct()
print('distinct function example: ', distinctExample.collect())

sampleExample = data1.sample(withReplacement=False, fraction=0.2)
print('sample function example: ', sampleExample.collect())

unionExample = data1.union(data2)
print('union function example: ', unionExample.collect())

intersectionExample = data1.intersection(data2)
print('intersection function example: ', intersectionExample.collect())

subtractExample = data1.subtract(data2)
print('subtract function example: ', subtractExample.collect())

cartesianExample = data1.cartesian(data2)
print('cartesian function example: ', cartesianExample.collect())

repartitionExample = data1.repartition(5)
print(f'repartition function example: Original partitions: {data1.getNumPartitions()}, New partitions: {repartitionExample.getNumPartitions()}')

coalesceExample = data1.coalesce(1)
print(f'coalesce function example: Original partitions: {data1.getNumPartitions()}, New partitions: {coalesceExample.getNumPartitions()}')

# https://stackoverflow.com/a/31612810
# This answer well explained the differences between repartition() and coalesce().
# coalesce() avoids a full shuffle.
# coalesce() only moves the data off the extra partitions, onto the partitions that we kept.
# repartition() always shuffles all data over the network.



'''
Actions:
1. collect()
2. count()
3. countByValue()
4. take()
5. top()
6. takeOrdered()
7. takeSample(withReplacement, num, [seed])
8. reduce()
9. fold(zeroValue, op)
10. foreach()
11. aggregate()
'''

collectExample = data1.collect()
print('collect function example: ', collectExample)

countExample = data1.count()   # return the number of elements
print('count function example: ', countExample)

countByValueExample = data1.countByValue()  # return a dictionary
print('countByValue function example: ', countByValueExample)

takeExample = data1.take(3)   # take first 3 elements
print('take function example: ', takeExample)

topExample = data1.top(2)     # take the largest 2 elements
print('top function example: ', topExample)

takeOrderedExample = data1.takeOrdered(num=3, key=lambda x: -x)  # take first 3 elements by descending sorted
print('takeOrdered function example: ', takeOrderedExample)

takeSampleExample = data1.takeSample(withReplacement=False, num=2)
print('takeSample function example: ', takeSampleExample)

reduceExample = data1.reduce(lambda x, y: x+y)  # Equals to sum()
print('reduce function example: ', reduceExample)

foldExample = data1.fold(zeroValue=0, op=lambda x,y: x+y)
# fold() is the almost same to the reduce(),
# while the difference is that fold() needs to pre-define a start value, aka zeroValue.
print('fold function example: ', foldExample)

foreachExample = data1.foreach(print)
# Both foreach() and map() are designed to iterate all elements in RDD.
# The differences are:
# 1. map() is a transformation, foreach() is a action.
# 2. foreach() returns none(void), and map() returns a new RDD.

aggregateExample = data1.aggregate(zeroValue=(0,0),
                                   seqOp=lambda x,y: (x[0] + y, x[1] + 1),
                                   combOp=lambda x,y: (x[0] + y[0], x[1] + y[1]))
# https://stackoverflow.com/a/38949457
# This answer well explained aggregate function and zeroValue.
print('aggregate function example: ', aggregateExample)

