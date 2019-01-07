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


# RDD Transformation and Action
'''
Transformation: Create a new dataset from an existing one. Return a new RDD
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
'''

data1 = sc.parallelize([1,2,3,4,5,5])
data2 = sc.parallelize([7,6,5,4,3])

data1.persist()  # save RDD into RAM for multiple use
data2.persist()

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



