from pyspark import SparkContext

sc1 = SparkContext(master='local', appName='initializing spark')


# OR
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('initializing spark').setMaster('local')
sc2 = SparkContext(conf=conf)


